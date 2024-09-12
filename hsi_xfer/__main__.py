#!/usr/bin/env python3

# pylint: disable=R,C

import argparse
import math
import fcntl
import getpass
import socket
import logging.handlers
import traceback
import json
import logging
import os
import re
import signal
import tarfile
import shutil
import sqlalchemy
import sys
import threading
import time
import tracemalloc

from enum import Enum
from multiprocessing.pool import ThreadPool as Pool
from subprocess import Popen, PIPE, run, STDOUT
from sqlalchemy import ForeignKey
from sqlalchemy import func, select
from sqlalchemy import insert, distinct
from sqlalchemy import update
from sqlalchemy import String
from sqlalchemy import create_engine
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from typing import List
from typing import Optional


VVS_PER_JOB = 1
MIGRATION_THREADS = 2
FILE_RETRY_LIMIT = 3
LOGGER = None
LOCKFILE = None
PROFILER = None
DB_TX_SIZE = None  # This gets set by the -s flag; defaults to 9000
CACHE = None
DATABASE = None
SCHEMA_VERSION = 4
DYING = False
PIDS = []

# NOTE: If you want to batch up EVERYTHING into a single HSI job,
# set --parallel-migration-count=-1

# =============================================================================
# hsi_xfer.py
#
# This is a script that wraps HSI and queries HPSS for files and VV location
# based upon a given HPSS-path. Once queried, it will batch up files based
# upon VV such that users can more smartly recall data from tape, without
# causing A) too many tapes/drives being loaded, and B) reduce tape thrashing.
# By default, only files from a single VV will be transferred to the provded
# destination directory at a time.
# Additionally, this script will:
# * Create the same directory structure that users have on HPSS at the provided
#   source path
# * Checksum files on HPSS (after staging from tape) and checksum files once
#   they have landed in the destination, and ensure that they are the same
# * Generate a JSON report that users can use to see if there were any failures
#   or if any files that were skipped, etc
#
#
# Additional Notes:
#
# Database statuses
#
# File status:
# not_started: Transfer not attempted
# staging: Transfer has been initiated, but data has not arrived in dest yet
# transferring: Transfer started, data is arriving in dest. We can checksum on
# HPSS now
# completed: Data has arrived on HPSS
# failed: transfer did not finish, or was corrupted. This will be the checksum
# failure
#
# checksum status:
# not_attempted: no checksuming yet
# hpss_complete: checksum on hpss complete; please retrieve
# completed: checksum on local fs complete
# failed: checksum failed for some reason: please retry
#
# =============================================================================

def sanitize(path):
    return path.replace(" ", "\ ")

class LockFile:
    def __init__(self, path):
        self.path = path
        self.lockfile = None

    def __del__(self):
        self.cleanup()

    def lock(self):
        f = open(self.path, "w")
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except:
            self.fail()
        self.lockfile = f

    def unlock(self):
        if self.lockfile is not None:
            fcntl.flock(self.lockfile, fcntl.LOCK_UN)
            if os.path.exists(self.path):
                os.close(self.lockfile.fileno())
                self.lockfile = None
                os.remove(self.path)

    def cleanup(self):
        self.unlock()

    def fail(self, delete_lockfile=True):
        LOGGER.error(
            "Error locking process! Please ensure no other hsi_xfer processes are running under your username on any node in this cluster!"
        )
        LOGGER.error(
            "To maximize availability of transfer resources for all users, there is a limit of one active hsi_xfer operation per user"
        )
        cleanup_and_die(997, delete_lockfile=delete_lockfile)


class CustomFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    formatdebug = "\r[%(levelname)s] %(message)s"
    formatinfo = "\r[+] %(message)s"
    formatcritical = "\r[!] %(message)s"
    formaterror = "\r[-] %(message)s"
    formatwarning = "\r[-] %(message)s"

    FORMATS = {
        logging.DEBUG: grey + formatdebug + reset,
        logging.INFO: green + formatinfo + reset,
        logging.WARNING: yellow + formatwarning + reset,
        logging.ERROR: red + formaterror + reset,
        logging.CRITICAL: bold_red + formatcritical + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class ChecksumStatus(Enum):
    not_attempted = 1
    hpss_complete = 3
    completed = 5
    failed = 6


class TransferStatus(Enum):
    not_started = 7
    staging = 8
    completed = 10
    failed = 11
    skipped = 12


class Base(DeclarativeBase):
    pass


class VV(Base):
    __tablename__ = "vvs"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    is_tape: Mapped[bool]


class State(Base):
    __tablename__ = "state"
    id: Mapped[int] = mapped_column(primary_key=True)
    schema_version: Mapped[int]
    indexing_complete: Mapped[bool]
    filelist_count: Mapped[int]
    starttime: Mapped[int]


class DestTree(Base):
    __tablename__ = "desttree"
    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str]
    created: Mapped[bool]


class Files(Base):
    __tablename__ = "files"
    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str] = mapped_column(sqlite_on_conflict_unique="IGNORE", unique=True)
    src_checksum: Mapped[str] = mapped_column(nullable=True)
    dest_checksum: Mapped[str] = mapped_column(nullable=True)
    checksum_status: Mapped[int]
    transfer_status: Mapped[int]
    vv: Mapped[Optional[int]] = mapped_column(ForeignKey("vvs.id"))
    dest: Mapped[str]
    destdir: Mapped[Optional[int]] = mapped_column(ForeignKey("desttree.id"))
    destfileexists: Mapped[bool]
    sizebytes: Mapped[int]


#
# Meta-class to track database and filelists created and to package them into an archive in case of interruption
#
class Cache:
    def __init__(
        self,
        verbose,
        cache_parent_path,
        cleanup_filelists=False,
        cleanup_db=False,
        existing_path=None,
        debug=False,
    ):
        self.verbose = verbose
        # Will either be --preserved-files-path or os.getcwd()
        self.cache_parent_path = cache_parent_path
        self.cleanup_filelists = cleanup_filelists
        self.cleanup_db = cleanup_db
        self.existing_path = (
            None if existing_path is None else os.path.abspath(existing_path)
        )
        self.debug = debug
        self.original_archive = None

        self.filelists = []

        # Cache does not exist
        if self.existing_path is None or (
            self.existing_path is not None and not os.path.exists(self.existing_path)
        ):
            if self.existing_path is not None and not os.path.exists(
                self.existing_path
            ):
                LOGGER.info(
                    f"Cache file does not exist: {self.existing_path}. Creating new cache file"
                )
                self.existing_path = None
            self.archive_path = os.path.join(
                self.cache_parent_path, f"hsi_xfer_{int(time.time())}.cache"
            )
            self.cache_path = os.path.join(self.archive_path, "cache")
            self.dbpath = os.path.join(self.cache_path, "database.db")
            self.filelists_root_path = os.path.join(self.cache_path, "filelists")
            if os.path.exists(self.dbpath):
                # If we get here something weird went wrong! Like a user trying to run 2 within the same second
                LOGGER.error(
                    "Error locking process! Ensure there is only one hsi_xfer process running, and that any hsi_xfer*.cache directories are cleaned up"
                )
                LOGGER.error(
                    "To maximize availability of transfer resources for all users, there is a limit of one active hsi_xfer operation per user"
                )
                cleanup_and_die(201)
        else:
            # Cache exists; resuming
            self.archive_path = self.existing_path
            self.unpack()
            self.cache_path = os.path.join(self.archive_path, "cache")
            self.dbpath = os.path.join(self.cache_path, "database.db")
            self.filelists_root_path = os.path.join(self.cache_path, "filelists")

        # If the directory that the cache will be written to doesn't exist, go ahead and mkdir it
        if not os.path.exists(self.cache_parent_path):
            os.makedirs(self.cache_parent_path)

        # If the cache_path doesn't exist, go ahead and mkdir it
        if not os.path.exists(self.archive_path):
            os.makedirs(self.archive_path)  # hpss_...cache
            os.makedirs(self.cache_path)  # hpss_..._.cache/cache
            os.makedirs(self.filelists_root_path)  # hpss_...cache/cache/filelists

    def cleanup(self):
        if self.cleanup_filelists and self.cleanup_db:
            shutil.rmtree(self.archive_path)
            return
        elif self.cleanup_filelists and not self.cleanup_db:
            shutil.rmtree(self.filelists_root_path)
        elif self.cleanup_db and not self.cleanup_filelists:
            os.remove(self.dbpath)
            shutil.move(self.filelists_root_path, self.cache_parent_path)
        self.pack()

    def caught_exception(self):
        self.cleanup_filelists = False
        self.cleanup_db = False
        # We call cleanup() here just in case we get a weird exception, we catch that exception and leave the cache in a state
        # that the user expects
        self.cleanup()

    def get_db_path(self):
        return self.dbpath

    def pack(self):
        LOGGER.info("Saving cache state...")
        LOGGER.debug("Packing up cache files into tarball")
        with tarfile.open(f"{self.archive_path}.tar.gz", "w:gz") as t:
            t.add(self.archive_path, arcname=os.path.basename(self.archive_path))
            time.sleep(5)

        failed = None
        for i in range(0, 5):
            try:
                shutil.rmtree(self.archive_path)
            except:
                time.sleep(1)
                try:
                    shutil.rmtree(self.archive_path)
                except Exception as e:
                    failed = e
            if failed is None:
                break
            failed = None
        if failed is not None:
            LOGGER.error(
                f"Could not remove the unpacked cache! Cache has been saved to {self.archive_path}.tar.gz; Please remove the existing cachedir before trying again: {self.archive_path}"
            )
            LOGGER.error(f"{e}")

        LOGGER.info(f"Saved cache at {self.archive_path}.tar.gz")

    def unpack(self):
        self.original_archive = self.archive_path  # Save the path to the input archive.
        self.archive_path = os.path.abspath(
            self.archive_path
        )  # Gets the fully qualified path to the tar'd archive
        # Save original path
        cache_basename = ".".join(
            list(filter(None, os.path.basename(self.archive_path).split(".")[:-2]))
        )
        # Interpolate the name of the cache
        self.archive_path = os.path.join(self.cache_parent_path, cache_basename)
        LOGGER.debug(
            f"Unpacking cache archive: archive_path: {self.archive_path}, cache_parent_path: {self.cache_parent_path}, existing_archive: {self.original_archive}"
        )
        # Untar the cache in the --preserved-files-path or cwd
        with tarfile.open(self.original_archive) as t:
            # If theres permissions issues, maybe set filter to either 'tar' or 'fully_trusted'
            t.extractall(self.cache_parent_path, filter="data")

    def get_unpacked_db_path(self):
        return self.dbpath

    def get_unpacked_filelist_root_path(self):
        return self.filelists_root_path

    def get_cache_path(self):
        return self.cache_path

    def get_unpacked_cache_path(self):
        return self.cache_path

    def get_unpacked_report_dir(self):
        return self.cache_parent_path


# Object to wrap communications to and from the DB. This also ensures that db
# communication is locked so that threads don't try
# and access the db at the same time b/c sqlite
class Database:
    def __init__(self, verbose, starttime, disable_checksums, debug=False):
        self.disable_checksums = disable_checksums
        self.verbose = verbose
        self.vvs = {}
        self.desttrees = {}
        self.debug = debug
        self.starttime = starttime

        self.dbpath = CACHE.get_unpacked_db_path()

        # Connect to DB
        try:
            self.engine = create_engine(f"sqlite:///{self.dbpath}", echo=self.debug)
            Base.metadata.create_all(self.engine)
        except Exception as e:  # pylint: disable=bare-except
            LOGGER.critical(
                "Can not open %s. Database may be corrupt: %s", self.dbpath, e
            )
            cleanup_and_die(3)

        metadata = sqlalchemy.MetaData()
        self.vvs_tbl = sqlalchemy.Table("vvs", metadata, autoload_with=self.engine)
        self.files_tbl = sqlalchemy.Table("files", metadata, autoload_with=self.engine)
        self.desttree_tbl = sqlalchemy.Table(
            "desttree", metadata, autoload_with=self.engine
        )
        self.state_tbl = sqlalchemy.Table("state", metadata, autoload_with=self.engine)
        self.init_state_tbl()

    def cleanup(self):
        LOGGER.debug("Killing DB connection")
        self.engine.dispose()

    def __del__(self):
        self.engine.dispose()

    def init_state_tbl(self):
        schema_version = SCHEMA_VERSION
        indexing_complete = False
        filelist_count = 0
        starttime = self.starttime
        LOGGER.debug(
            f"Initializing state table schema_version={schema_version}, indexing_complete={indexing_complete}, filelist_count={filelist_count}, starttime={starttime}"
        )
        row = State(
            schema_version=schema_version,
            indexing_complete=indexing_complete,
            filelist_count=filelist_count,
            starttime=starttime,
        )
        with Session(bind=self.engine) as s:
            cur = s.execute(
                select(State).select_from(State).order_by(State.id.desc()).limit(1)
            ).fetchall()
            if len(cur) > 0:
                cur = cur[0][0]
                LOGGER.debug(
                    f"Found existing state information: schema_version={cur.schema_version}, indexing_complete={cur.indexing_complete}, filelist_count={cur.filelist_count}, starttime={starttime}"
                )
                if cur.schema_version != schema_version:
                    LOGGER.error(
                        f"Attempting to use a cache from an old version of this tool. This will probably fail!"
                    )
            s.add(row)
            s.flush()
            s.commit()

    def getstarttime(self) -> int:
        curstate = self.get_state()
        return curstate.starttime

    def gettotalsize(self) -> int:
        with Session(bind=self.engine) as s:
            stmt = (
                select(func.sum(Files.sizebytes))
                .select_from(Files)
                .where(Files.transfer_status == TransferStatus.completed.value)
            )
            row = s.execute(stmt).fetchall()[0][0]
            if row is not None:
                return row
            else:
                return 0

    def get_state(self):
        cur = None
        with Session(bind=self.engine) as s:
            cur = s.execute(
                select(State).select_from(State).order_by(State.id.desc()).limit(1)
            ).fetchall()[0][0]
            all_entries = s.execute(
                select(State).select_from(State).order_by(State.id.desc())
            ).fetchall()
            for row in all_entries:
                if row[0].indexing_complete:
                    cur.indexing_complete = row[0].indexing_complete
        return cur

    def update_state(
        self, schema_version=SCHEMA_VERSION, indexing_complete=False, filelist_count=0
    ):
        LOGGER.debug(
            f"Updating state table schema_version={schema_version}, indexing_complete={indexing_complete}, filelist_count={filelist_count}"
        )
        with Session(bind=self.engine) as s:
            try:
                cur = s.execute(
                    select(State).select_from(State).order_by(State.id.desc()).limit(1)
                ).fetchall()[0][0]
                if indexing_complete:
                    cur.indexing_complete = indexing_complete
                if filelist_count > 0:
                    cur.filelist_count = filelist_count
                s.execute(
                    update(State)
                    .where(State.id == cur.id)
                    .values(
                        schema_version=cur.schema_version,
                        indexing_complete=cur.indexing_complete,
                        filelist_count=cur.filelist_count,
                    )
                )
                s.flush()
                s.commit()
            except Exception as e:
                if not DYING:
                    LOGGER.error("Could not update state information in the cache!")
                    cleanup_and_die(30)
                else:
                    LOGGER.debug("Encountered the following exception while dying:")
                    LOGGER.debug(f"{e}")

    # Populate the desttree table. Maps files to destination paths
    def insertdesttree(self, tree):
        LOGGER.debug(
            f"Batch inserting destination tree into 'desttree' table (num_dirs={len(tree)})..."
        )
        if len(tree) > 0:
            with Session(bind=self.engine) as s:
                try:
                    s.add_all(tree)
                    s.flush()
                    for t in tree:
                        self.desttrees[t.path] = t.id
                    s.commit()
                except Exception as e:
                    if not DYING:
                        LOGGER.error(
                            f"Could not commit to cache DB or process killed during COMMIT: {self.dbpath} DYING={DYING}"
                        )
                        LOGGER.debug("(insertdesttree)")
                        LOGGER.debug(f"{e}")
                        cleanup_and_die(111)

    # Insert file entries into the files table
    def insertfiles(self, files, srcroot, destroot, qualified_source):
        LOGGER.debug(
            f"Batch inserting files into 'files' table (num_files={len(files)})..."
        )

        files_to_insert = []
        for file in files[:]:
            vv = file[1].split(",")[0]
            fpath = file[0].replace(srcroot, qualified_source, 1)
            if len(vv) != 8:
                LOGGER.debug(
                    f"No VV found for file {fpath}; skipping and will not track!"
                )
                LOGGER.error(
                    f"Could not read HPSS metadata for file {fpath}; skipping..."
                )
                continue
            vvid = self.vvExists(vv)
            dest = fpath.replace(qualified_source, destroot, 1)
            if vvid is None:
                self.insertvv(vv)
                vvid = self.vvExists(vv)
                if vvid is None:
                    LOGGER.error("Insert of VV failed!")
                    cleanup_and_die(100)

            destid = self.getdest(dest)[0]  # This can do a select for each file
            if destid == []:
                LOGGER.critical("No destination found; Cowardly failing...")
                cleanup_and_die(2)

            size = file[2]

            files_to_insert.append(
                Files(
                    path=fpath,
                    vv=vvid,
                    dest=dest,
                    destdir=destid,
                    checksum_status=ChecksumStatus.not_attempted.value,
                    transfer_status=TransferStatus.not_started.value,
                    destfileexists=False,
                    sizebytes=size,
                )
            )

            files.remove(file)

        # Perform the actual inserts
        with Session(bind=self.engine) as s:
            try:
                s.add_all(files_to_insert)
                s.flush()
                s.commit()
            except Exception as e:
                if not DYING:
                    LOGGER.error(
                        f"Could not commit to cache DB or process killed during COMMIT: {self.dbpath}"
                    )
                    LOGGER.debug("(insertfiles)")
                    LOGGER.debug(f"{e}")
                    cleanup_and_die(222)

    # Returns all files where the transfer is complete, and the source checksum did not fail
    def getnonfailedsrcchecksumfiles(self):
        stmt = (
            select(Files.path, Files.dest, Files.id)
            .select_from(Files)
            .where(Files.checksum_status != ChecksumStatus.failed.value)
            .where(Files.transfer_status == TransferStatus.completed.value)
        )

        rows = []
        with Session(self.engine) as s:
            rows = s.execute(stmt).fetchall()

        return rows

    # returns all files that failed transfers
    def getfailedtransfers(self):
        stmt = (
            select(Files.path, Files.dest)
            .select_from(Files)
            .where(Files.transfer_status == TransferStatus.failed.value)
        )
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(stmt).fetchall()
        return rows

    # Gets all files that failed checksumming
    def getfailedchecksums(self):
        stmt = (
            select(Files.path, Files.src_checksum, Files.dest, Files.dest_checksum)
            .select_from(Files)
            .where(Files.checksum_status == ChecksumStatus.failed.value)
        )
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(stmt).fetchall()
        return rows

    # Get all files that successfully finished transferrring and checksumming
    def getsuccessfultransfers(self):
        stmt = (
            select(Files.path, Files.dest, Files.dest_checksum)
            .select_from(Files)
            .where(Files.checksum_status == ChecksumStatus.completed.value)
            .where(Files.transfer_status == TransferStatus.completed.value)
        )
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(stmt).fetchall()
        return rows

    # Mark a file entry as failed to transfer
    def markasfailed(self, srcpath):
        stmt = (
            update(Files)
            .where(Files.path == srcpath)
            .values(transfer_status=TransferStatus.failed.value)
        )
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(333)

    # Mark a destination driectory as created
    def markdestcreated(self, destid):
        stmt = update(DestTree).values(created=True).where(DestTree.id == destid)
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(444)

    # Marks a file as 'staging' this is more or less equiv to 'transferring'
    def markfileasstaging(self, fileid):
        stmt = (
            update(Files)
            .values(transfer_status=TransferStatus.staging.value)
            .where(Files.id == fileid)
        )
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(555)

    # Set the value of the hpss-derived checksum for the file object
    def setsrcchecksum(self, srcpath, checksum):
        stmt = update(Files).values(src_checksum=checksum).where(Files.path == srcpath)
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(666)

    # Sets the value of the md5sum performed at the destination
    def setdestchecksum(self, fileid, checksum):
        stmt = update(Files).values(dest_checksum=checksum).where(Files.id == fileid)
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(777)

    # Marks the checksum status of a file as completed
    def markfilechecksumascomplete(self, fileid):
        stmt = (
            update(Files)
            .values(checksum_status=ChecksumStatus.completed.value)
            .where(Files.id == fileid)
        )
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(888)

    # Marks the checksum status of a file as failed
    def markfilechecksumasfailed(self, fileid):
        stmt = (
            update(Files)
            .values(checksum_status=ChecksumStatus.failed.value)
            .where(Files.id == fileid)
        )
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(112)

    # Marks the file trasfer as completed
    def markfileastransfercompleted(self, srcpath):
        LOGGER.debug(f"Marking complete: {srcpath}={TransferStatus.completed.value}")
        stmt = (
            update(Files)
            .values(transfer_status=TransferStatus.completed.value)
            .where(Files.path == srcpath)
        )
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(113)

    # Marks the file as hpss_checksum_complete
    def markfileashpsschecksumcomplete(self, srcpath):
        stmt = (
            update(Files)
            .values(checksum_status=ChecksumStatus.hpss_complete.value)
            .where(Files.path == srcpath)
        )
        try:
            with Session(self.engine) as s:
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(114)

    # Inserts a VV into the vvs table
    def insertvv(self, vv):
        istape = False
        if "X" in vv or "H" in vv:
            istape = True
        v = VV(name=vv, is_tape=istape)
        try:
            with Session(self.engine) as s:
                s.add(v)
                s.flush()
                self.vvs[vv] = v.id
                s.commit()

        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(115)

    # Retrieve a vv entry by name
    def getvv(self, vv):
        row = None
        with Session(self.engine) as s:
            row = s.execute(
                select(VV.id, VV.name).select_from(VV).where(VV.name == vv)
            ).one_or_none()
        return row

    def desttreeexists(self, path):
        if path in self.desttrees:
            return True
        return False

    # Get a destination driectory entry by path
    def getdest(self, path):
        dirname = os.path.dirname(path)
        if dirname in self.desttrees:
            return [self.desttrees[dirname]]
        else:
            row = None
            with Session(self.engine) as s:
                row = s.execute(
                    select(DestTree.id, DestTree.path)
                    .select_from(DestTree)
                    .where(DestTree.path == dirname)
                ).first()
            return row if row is not None else []

    # Get file entries where the source and dest checksums do not match
    def getnonmatchingchecksums(self):
        stmt = (
            select(Files.path, Files.src_checksum, Files.dest, Files.dest_checksum)
            .select_from(Files)
            .where(Files.dest_checksum != Files.src_checksum)
        )

        rows = []
        if self.disable_checksums:
            return []
        #    stmt = select(Files.path, Files.src_checksum, Files.dest, Files.dest_checksum).select_from(Files).where((Files.dest_checksum != Files.src_checksum) | (Files.dest_checksum == None & Files.src_checksum == None & Files.destfileexists == True))

        with Session(self.engine) as s:
            rows = s.execute(stmt).fetchall()

        return rows

    # vvExists will return None if theres no entries, and the rowid if there is
    def vvExists(self, vv):
        if vv not in self.vvs:
            return None
        else:
            return self.vvs[vv]

    # fileExists will return None if theres no entries, and the rowid if there is
    def fileExists(self, file):
        rows = self.getfile(file)
        for row in rows:
            if row.path == file:
                return row.id
        return None

    # Get the total number of files in the files table in the db
    def gettotalnumberofvvs(self) -> list:
        row = None
        with Session(self.engine) as s:
            try:
                row = s.execute(
                    select(func.count("*")).select_from(VV)
                ).one_or_none()
            except Exception as e:
                LOGGER.debug(
                    "Encountered the following exception in gettotalnumberofvvs while dying"
                )
                LOGGER.debug(f"{e}")
                if not DYING:
                    LOGGER.critical("Could not read from database. Exiting")
                    cleanup_and_die(112)
        return row if row is not None else []

    # Get the total number of files in the files table in the db
    def gettotalnumberoffiles(self):
        row = None
        with Session(self.engine) as s:
            try:
                row = s.execute(
                    select(func.count("*")).select_from(Files)
                ).one_or_none()
            except Exception as e:
                LOGGER.debug(
                    "Encountered the following exception in gettotalnumberoffiles while dying"
                )
                LOGGER.debug(f"{e}")
                if not DYING:
                    LOGGER.critical("Could not read from database. Exiting")
                    cleanup_and_die(116)
        return row if row is not None else []

    # Get files marked as 'destination file exists'
    def getexistingfiles(self):
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(
                select(Files.path, Files.dest, Files.id)
                .select_from(Files)
                .where(Files.destfileexists == True)
            ).fetchall()
        return rows

    # get a file by source path
    def getfile(self, file):
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(
                select(Files.id, Files.path)
                .select_from(Files)
                .where(Files.path == file)
            ).fetchall()
        return rows

    # Get a list of files by VV (DB) id
    def getfilesbyvvlist(self, vvids):
        rows = []
        offset = 0
        with Session(self.engine) as s:
            while True:
                rows = s.execute(
                    select(Files.path, Files.dest, Files.id)
                    .select_from(Files)
                    .where(Files.vv.in_(vvids))
                    #            .where(Files.vv == vvid)
                    .where(Files.transfer_status != TransferStatus.completed.value)
                    .order_by(self.files_tbl.c.id)
                    .offset(offset)
                    .limit(DB_TX_SIZE)
                ).fetchall()
                yield rows
                offset += len(rows)
                if len(rows) < DB_TX_SIZE:
                    break

    # Dump the files table
    def dumpfiles(self):
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(select(Files)).fetchall()
        return rows

    # Get list of vvs that have non-complete file transfers
    def getvvswithnoncompletefiles(self):
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(
                select(distinct(Files.vv))
                .select_from(Files)
                .where(Files.transfer_status != TransferStatus.completed.value)
            ).fetchall()
        return rows

    # dump the desttree table
    def dumpdesttree(self):
        rows = []
        with Session(self.engine) as s:
            rows = s.query(DestTree).filter(func.length(DestTree.path)).all()
        return rows

    # Mark a file as 'destination file exists'
    def markexists(self, fileid):
        try:
            with Session(self.engine) as s:
                s.execute(
                    update(Files)
                    .values(
                        destfileexists=True,
                        transfer_status=TransferStatus.skipped.value,
                    )
                    .where(Files.id == fileid)
                )
                s.commit()
        except Exception as e:
            LOGGER.error(
                f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}"
            )
            LOGGER.debug(f"{e}")
            cleanup_and_die(117)


# Object to manage the HSIJobs required for migration of files in the DB
class MigrateJob:
    def __init__(self, args):
        self.dry_run = args.dry_run
        self.verbose = args.verbose
        self.filelists_path = CACHE.get_unpacked_filelist_root_path()

        self.disable_checksums = args.disable_checksums
        self.preserve_timestamps = args.preserve_timestamps
        self.parallel_migration_count = args.parallel_migration_count
        self.vvs_per_job = args.vvs_per_job
        # self.parallel_migration_count = 1
        self.addl_flags = args.additional_hsi_flags
        self.overwrite = args.overwrite_existing
        self.disable_ta = args.disable_ta
        self.checksum_threads = args.checksum_threads
        self.hsi = "/sw/sources/hpss/bin/hsi"

        self.relative_input_source = (
            f"./{os.path.normpath(args.source)}"
            if args.source[0] not in ["/", "."]
            else os.path.normpath(args.source)
        )
        self.input_source = args.source
        self.source = self._get_qualified_hpss_path(self.relative_input_source)
        self.destination = os.path.abspath(os.path.normpath(args.destination))

        global DATABASE
        DATABASE = Database(
            self.verbose, int(time.time()), self.disable_checksums, debug=args.debug
        )

        # Define the HSIJob that lists all the files recursively in the source directory
        self.ls_job = HSIJob(
            self.hsi,
            self.addl_flags,
            True,
            self.verbose,
            "ls -a -N -R -P '{}'".format(
                self.input_source
            ),  # use input_source here to ensure globbing works
        )

    # Check the destination filesystem for files that match names at the predicted destination directory
    def checkForExisting(self):
        if not self.overwrite:
            files = DATABASE.dumpfiles()

            for file in files:
                destpath = file[0].dest
                fileid = file[0].id
                destsize = file[0].sizebytes
                # Check if file exists in the destination. If it does exist, check if the file sizes are the same. If they're different
                # Assume that the file is incomplete and overwrite it
                if os.path.isfile(destpath):
                    srcsize = os.path.getsize(destpath)
                    if srcsize == destsize:
                        # Check if indexing complete, and if so, check if the file is 'staging', then do not add, else, mark exists
                        LOGGER.debug(
                            f"Found existing file {file[0].path}, destination={destpath}, id={fileid}, size={srcsize}"
                        )
                        DATABASE.markexists(fileid)
        else:
            LOGGER.debug(
                "Skipping check for existing files due to --overwrite-existing"
            )

    # Create the destination directory structure
    def createDestTree(self):
        destpaths = DATABASE.dumpdesttree()
        for path in destpaths:
            try:
                os.makedirs(path.path, exist_ok=True)
            except FileExistsError:
                LOGGER.debug(
                    "Skipping mkdir of %s; directory already exists", path.path
                )
                DATABASE.markdestcreated(path.id)
            except Exception as e:  # pylint: disable=bare-except
                LOGGER.critical(
                    "ERROR: Could not create destination directory (%s).\n(%s)",
                    path.path,
                    e,
                )
                cleanup_and_die(118)
            else:
                DATABASE.markdestcreated(path.id)

    def runHashList(self, files, filelistnum):
        infilename = os.path.join(self.filelists_path, f"hsi_xfer.hashlist.{filelistnum}.list")
        # Write the infile to be passed into HSI
        with open(infilename, "w", encoding="UTF-8") as infile:
            infile.write("\n".join(["hashlist -A {}".format(sanitize(f[0])) for f in files]))
            infile.write("\n")

        hashlist_job = HSIJob(
            self.hsi,
            self.addl_flags,
            True,
            self.verbose,
            "in {}".format(infilename),
        )

        ret = {}
        if not self.dry_run:
            for chunk in hashlist_job.run():
                for line in chunk:
                    if "md5" in line:
                        checksum, srcpath = line.split(" ")[0], line.split(" ")[-2]
                        LOGGER.debug(
                            "Found preexisting hash for file: %s : %s",
                            srcpath,
                            checksum,
                        )
                        ret[srcpath] = checksum

        return ret

    def writeExistingHashes(self, hashlistout):
        for path in hashlistout:
            DATABASE.setsrcchecksum(path, hashlistout[path])
            DATABASE.markfileashpsschecksumcomplete(path)

    
    def chunk(self, lst, chunksize) -> list:
        if chunksize < 0 and chunksize != -1:
            LOGGER.critical(
                "parallel-migration-count needs to be either -1 or a positive integer!"
            )
            cleanup_and_die(4)
        if chunksize == -1:
            yield [ l[0] for l in lst ]
        for i in range(0, len(lst), chunksize):
            yield [ l[0] for l in lst[i : i + chunksize] ]

    # This is the main migration thread! All the fun stuff happens here
    # - Generates the file lists
    # - Calls HSI to migrate and hash files
    # - Populate the db with the resulting information
    # - Profit

    def generateFileList(self) -> (str, list):
        # Get chunked list of vvs
        allvvs = DATABASE.getvvswithnoncompletefiles()

        vvs = list(self.chunk(allvvs, int(self.vvs_per_job)))

        LOGGER.info(f"Will generate {math.ceil(len(vvs)/self.parallel_migration_count)} file lists", extra={"block": "cli"})

        # Get list of exisitng files from the db
        self.existingFiles = DATABASE.getexistingfiles()

        if self.overwrite:
            self.existingFiles = []

        if len(self.existingFiles) > 0:
            LOGGER.debug("Found existing files:")
            # Format existingFiles so it looks better
            existing = []
            for f in self.existingFiles:
                existing.append({"source": f[0], "destination": f[1]})
            LOGGER.debug(json.dumps(existing, indent=2))

        LOGGER.info("Creating file lists from indexed HPSS data...")

        # Set some variables for status output (esp if not using the progress bar)
        filelistnum = 0
        self.filesCompleted += len(self.existingFiles)
        LOGGER.info(
            f"Will transfer {self.filesTotal} files from HPSS", extra={"block": "cli"}
        )

        # If we don't set --additional-hsi-flags, assume we're using keytabs for auth. If so, then we can use the progress bar
        # Otherwise, the progress bar will overwrite the PASSCODE: prompt so it'll just appear to 'hang' for users

        # Launch an HSI job for each chunk of vvs (generally 1 vv/chunk but see below)
        DATABASE.update_state(filelist_count=len(vvs))

        for vvlist in vvs:
            files = []
            allfilesinlist = []
            filelistnum += 1
            tmpgetfilename = os.path.join(
                self.filelists_path, f"hpss_tsf_tmp_get.{filelistnum}.list"
            )
            tmphashfilename = os.path.join(
                self.filelists_path, f"hpss_tsf_tmp_hash.{filelistnum}.list"
            )
            infilename = os.path.join(
                self.filelists_path, f"hsi_xfer.{filelistnum}.list"
            )
            with open(tmpgetfilename, "a", encoding="UTF-8") as getfile, open(
                tmphashfilename, "a", encoding="UTF-8"
            ) as hashfile:
                for chunk in DATABASE.getfilesbyvvlist(vvlist):
                    files = chunk
                    files = [f for f in files if f not in self.existingFiles]
                    allfilesinlist.extend(files)

                    # We're going to be writing multiple gets and hashcreate commands here
                    # Write each to a separate file, and append to infile at the end?
                    if len(files) > 0:
                        LOGGER.debug(
                            "Writing list of %s files to temporary file list...",
                            len(chunk),
                        )

                        # Do a hashlist to get files that have hashes already
                        hashlistout = self.runHashList(files, filelistnum)
                        # if file has hash; save to db
                        self.writeExistingHashes(hashlistout)
                        # to_hash gets written to the infile; add a hashcreate for all files that don't have hashes already
                        to_hash = [f for f in files if f[0] not in hashlistout]

                        getfile.write(
                            "\n".join(["{} : {}".format(sanitize(f[1]), sanitize(f[0])) for f in files])
                        )
                        getfile.write("\n")
                        # Add a hashcreate command to generate and get hashes in the same job.
                        # We're not doing this in parallel here, since it'll create a lot of PASSCODE prompts if a user isn't using keytabs
                        # And since they both (get and hashcreate) need to stage the data, we're not introducing too much extra runtime here
                        if not self.disable_checksums and len(to_hash) > 0:
                            hashfile.write(
                                "\n".join(["{}".format(sanitize(f[0])) for f in to_hash])
                            )
                            hashfile.write("\n")

            if len(allfilesinlist) > 0:
                # Get the length of the hashfiles
                hashfilelen = 0
                with open(tmphashfilename, "r", encoding="UTF-8") as hashfile:
                    hashfilelen = sum(1 for _ in hashfile)

                # Combine both the hashfile and getfile here
                with open(infilename, "w", encoding="UTF-8") as infile, open(
                    tmpgetfilename, "r", encoding="UTF-8"
                ) as getfile, open(
                    tmphashfilename, "r", encoding="UTF-8"
                ) as hashfile:
                    LOGGER.debug("Combining temporary file lists")
                    # Write the infile to be passed into HSI
                    getcmd = f"get {'-P' if self.preserve_timestamps else ''} -T on << EOF\n"
                    if self.disable_ta:
                        getcmd = f"get {'-P' if self.preserve_timestamps else ''} -T off << EOF\n"

                    # hashcreate can not use the TA, so we force -T off
                    hashcreatecmd = "hashcreate -T off << EOF\n"
                    infile.write(getcmd)
                    for line in getfile:
                        infile.write(line)
                    infile.write("EOF\n")
                    if not self.disable_checksums and hashfilelen > 0:
                        infile.write(hashcreatecmd)
                        for hashline in hashfile:
                            infile.write(hashline)
                        infile.write("EOF")

                # Remove our tmp files
                LOGGER.debug("Removing temporary file lists")
                os.remove(tmphashfilename)
                os.remove(tmpgetfilename)
                yield (infilename, allfilesinlist)

    # Does a single hsi migration
    def doFilelistMigration(self, infilename, files) -> int:
        if os.path.exists(infilename):
            # This is our HSI job
            migration_job = HSIJob(
                self.hsi,
                self.addl_flags,
                True,
                self.verbose,
                "in {}".format(infilename),
            )
            if not self.dry_run:
                for f in files:
                    DATABASE.markfileasstaging(f[2])
                # Run the job!
                # Scan through the output looking for errors and md5 sums. Populate the db accordingly
                last_notif_time = int(time.time())
                last_notif_idx = 0
                # TODO: Launch multiple threads for the below block
                for chunk in migration_job.run(continueOnFailure=True):
                    for line in chunk:
                        # Log output to syslog every 10% of total or if its the first iteration

                        if "(md5)" in line:
                            checksum, srcpath = (
                                line.split(" ")[0],
                                line.split(" ")[-1],
                            )
                            DATABASE.setsrcchecksum(srcpath, checksum)
                            DATABASE.markfileashpsschecksumcomplete(srcpath)

                        matches = re.match(
                            r"get\s\s'([^']+/(?:[^/]+/)*[^']+[^/]*)'\s:\s'([^']+/(?:[^/]+/)*[^']+[^/]*)'(?:\s+\([^)]+\))?",
                            line,
                        )
                        if matches:
                            srcpath = matches.group(2)
                            self.filesCompleted += 1
                            DATABASE.markfileastransfercompleted(srcpath)

                        if "get: Error" in line:
                            LOGGER.error(
                                "File %s failed to transfer from HPSS",
                                line.split(" ")[-1],
                            )
                            DATABASE.markasfailed(line.split(" ")[-1])
                # Log the last notifications
                # TODO: This will only notify at the end of a filelist; make this more granular
                LOGGER.info(
                    "%s/%s file transfers have been attempted",
                    self.filesCompleted,
                    self.filesTotal,
                    extra={"block": "syslog"},
                )
                payload = {
                    "filescompleted": self.filesCompleted,
                    "filestotal": self.filesTotal,
                }
                LOGGER.info(json.dumps(payload), extra={"block": "cli"})
            else:
                LOGGER.info("Would have run: %s", migration_job.getcommand())
        else:
            LOGGER.debug(
                "Skipping file list due to existing files. Use --overwrite-existing to bypass this"
            )
        return 0

    # HPSS/HSI is configured such that we can't rely on HSI to parallelize anything here
    def startMigrate(self) -> int:
        # Generator to created a chunked list of vv's
        self.filesTotal = DATABASE.gettotalnumberoffiles()[0]
        self.filesCompleted = 0
        LOGGER.info(
            "Launching a sorted batch migration of %d files from %s to %s (dry-run=%s)",
            self.filesTotal,
            self.source,
            self.destination,
            self.dry_run,
        )
        threads = []
        numfilelists = math.ceil(DATABASE.gettotalnumberofvvs()[0] / self.vvs_per_job)
        flists = self.generateFileList()
        for vvnum in range(0, numfilelists):
            # Generate the next file list and list of files
            infilename, files = next(flists)
            #If active threads is < the number of parallel executions allowed, create a new thread
            if len(threads) < self.parallel_migration_count:
                LOGGER.debug(f"Launching new thread. Active threads: {len(threads)}")
                thread = threading.Thread(target=self.doFilelistMigration, args=(infilename, files))
                thread.start()
                threads.append(thread)
            else:
                # If we're out of free threads, poll the threads every second to see if any have died
                while len(threads) >= self.parallel_migration_count:
                    time.sleep(1)
                    threads = [ t for t in threads if t.is_alive() ]

                if len(threads) >= self.parallel_migration_count:
                    LOGGER.error("Undefined behavior: too many threads")
                    cleanup_and_die(100)

                # Once theres space, add a new thread and continue
                LOGGER.debug(f"Launching new thread. Active threads: {len(threads)}")
                thread = threading.Thread(target=self.doFilelistMigration, args=(infilename, files))
                thread.start()
                threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        return 0


    # This is our main destination checksumming thread. This will be pretty fast b/c GPFS.
    # We're defaulting to a 4 thread pool for checksumming. This can be overwritten by --checksum-threads
    # TODO: Modify this such that it runs during transfers. Use an async function?
    def startDestChecksumming(self):
        LOGGER.info("Starting checksumming of transferred files")
        files = DATABASE.getnonfailedsrcchecksumfiles()
        values = []
        with Pool(self.checksum_threads) as p:
            values = p.map(doDestChecksum, files)

        for value in values:
            if value["returncode"] != 0:
                DATABASE.markfilechecksumasfailed(value["id"])
            else:
                DATABASE.markfilechecksumascomplete(value["id"])
            DATABASE.setdestchecksum(value["id"], value["checksum"])

    # Generates a report of any non-matching checksums
    def checksumMismatchReport(self):
        files = DATABASE.getnonmatchingchecksums()
        ret = {}
        ret["checksum_mismatch"] = []
        if len(files) > 0:
            for file in files:
                LOGGER.debug(file)
                LOGGER.error(
                    "Checksum did not match! Can not guarantee file integrity!"
                )
                LOGGER.debug(" - %s(%s) : %s(%s)", file[0], file[1], file[2], file[3])
                ret["checksum_mismatch"].append(
                    {
                        "source": (file[0], file[1]),
                        "destination": (file[2], file[3]),
                    }
                )
        else:
            LOGGER.debug("All files checksummed")
        return ret

    # Generates our overall report. This will get written out to a JSON file for users
    def genReport(self, mismatchreport):
        report = mismatchreport

        failedtransfers = DATABASE.getfailedtransfers()
        report["failed_transfers"] = []
        for f in failedtransfers:
            report["failed_transfers"].append({"source": f[0], "destination": f[1]})

        failedchecksums = DATABASE.getfailedchecksums()
        report["failed_to_checksum"] = []
        for f in failedchecksums:
            report["failed_to_checksum"].append(
                {"source": (f[0], f[1]), "destination": (f[2], f[3])}
            )

        succeededfiles = DATABASE.getsuccessfultransfers()
        report["successful_transfers"] = []
        for f in succeededfiles:
            report["successful_transfers"].append(
                {"source": f[0], "destination": f[1], "checksum": f[2]}
            )

        existingfiles = DATABASE.getexistingfiles()
        report["existing_files_skipped"] = []
        for f in existingfiles:
            report["existing_files_skipped"].append(
                {"source": f[0], "destination": f[1]}
            )

        if not self.dry_run:
            totalsize = DATABASE.gettotalsize()
            starttime = DATABASE.getstarttime()
            endtime = int(time.time())
            # Default to 1 second elapsed here just to prevent divide by zero
            elapsed = (endtime - starttime) if (endtime - starttime) > 0 else 1
            bps = (totalsize / 1000 / 1000) / elapsed
            report["average_speed"] = bps
            report["elapsed_time"] = elapsed
            report["total_size_in_bytes"] = totalsize

        if self.dry_run:
            files = DATABASE.dumpfiles()
            report["would_have_transferred"] = []
            for f in files:
                report["would_have_transferred"].append(
                    {"source": f[0].path, "destination": f[0].dest}
                )
        return report

    @property
    def _hpss_root_dir(self):
        path = os.path.normpath(self.input_source)
        while any(char in "*?[]" for char in path):
            path = os.path.dirname(path)
        return path

    def _get_qualified_hpss_path(self, path):
        # path = os.path.normpath(path) if path[0] not in [".","/"] else path
        # path = os.path.normpath(path)
        while any(char in "*?[]" for char in path):
            path = os.path.dirname(path)

        ls_job = HSIJob(
            self.hsi,
            self.addl_flags,
            True,
            self.verbose,
            "ls -aNPd '{}'".format(path),
        )

        for chunk in ls_job.run():
            for line in chunk:
                if os.path.normpath(path) in line:
                    if "DIRECTORY" in line:
                        return line.split("\t")[-1]
                    elif "FILE" in line:
                        LOGGER.critical(
                            "Invalid source path provided. Single file transfers are not supported! Please replace the source path with a directory"
                        )
                        cleanup_and_die(118)

        # Last ditch effort, just basically do _hpss_root_dir on path
        return path

    # This gets the list of files from HPSS to be inserted into the DB
    def getSrcFiles(self):
        LOGGER.debug(f"_hpss_root_dir={self._hpss_root_dir}")
        if not DATABASE.get_state().indexing_complete:
            start = int(time.time())
            cur = start
            for chunk in self.ls_job.run():
                LOGGER.debug(
                    f"Got {len(chunk)} entries from HSI. Retrival took {int(time.time())-cur}s"
                )
                LOGGER.debug(f"{chunk}")

                files = []
                dirs = set()

                pstart = int(time.time())
                for line in chunk:
                    s = line.split("\t")
                    objtype = s[0]
                    # If this is a file entry, save it with the VV, and infer the directory
                    if "FILE" in objtype:
                        file, size, vv = s[1], s[2], s[5]
                        files.append((file, vv, size))
                        dirs.add(os.path.dirname(file))
                LOGGER.debug(f"Chunk processing took {int(time.time())-pstart}s")

                dstart = int(time.time())
                # Removing junk from array
                dirs = [
                    DestTree(
                        created=False,
                        path=d.replace(self._hpss_root_dir, self.destination, 1),
                    )
                    for d in dirs
                    if d.replace(self._hpss_root_dir, self.destination, 1)
                    not in DATABASE.desttrees
                ]
                # Adding the root destination since files can be at depth 0, and we want to make sure its there
                if self.destination not in DATABASE.desttrees:
                    dirs.append(DestTree(created=False, path=self.destination))
                if len(dirs) > 0:
                    DATABASE.insertdesttree(dirs)
                if len(files) > 0:
                    DATABASE.insertfiles(
                        files, self._hpss_root_dir, self.destination, self.source
                    )
                LOGGER.debug(f"Database operation took {int(time.time())-dstart}s")
                cur = int(time.time())
            LOGGER.debug(f"Updating database state table")
            DATABASE.update_state(indexing_complete=True)
            if DATABASE.gettotalnumberoffiles()[0] == 0:
                LOGGER.critical(
                    f"No files found on HPSS matching query ({self.source}) or all files matched already exist in {self.destination}"
                )
                LOGGER.critical(
                    f"Use --overwrite-existing to overwrite files in the destination directory"
                )
                cleanup_and_die(119)
        else:
            LOGGER.info("Skipping file indexing due to input cache")
        PROFILER.snapshot()


# Due to the fact that multiprocessing is weird, and pickles things that get passed to the pool (including the function itself),
# and the fact that the MigrationJob object contains a Database object that contains non-serializable data (thread locks), we go
# ahead and play it safe and pull this function outside the object. Yay.
# This function performs a single md5sum on a single file in the destination. Thread pool worker
def doDestChecksum(file):
    checksum = ""

    cmd = ["md5sum","{}".format(file[1])]
    LOGGER.debug(f"Running { ' '.join(cmd) }")

    # Check if file exists or not; When using the TA + NFS sometimes things get weird
    if not os.path.exists(file[1]):
        time.sleep(10)
        if not os.path.exists(file[1]):
            LOGGER.error(
                f"Could not checksum file {file[1]}! Can not guarantee file integrity"
            )
            return {
                "path": file[1],
                "checksum": None,
                "returncode": -1,
                "id": file[2],
            }

    output = []
    p = run(
        cmd,
        stdout=PIPE,
        stderr=STDOUT,
        bufsize=1,
        universal_newlines=True,
        check=False,
    )

    output = p.stdout.strip()
    checksum = output.split(" ")[0]
    if p.returncode != 0:
        LOGGER.debug(f"{output}")
    return {
        "path": file[1],
        "checksum": checksum,
        "returncode": p.returncode,
        "id": file[2],
    }


# Our HSI class. This just wraps subprocess+hsi to make it a little easier to manage
class HSIJob:
    def __init__(self, hsi, flags, quiet, verbose, command):
        self.hsi = hsi
        self.quiet = quiet
        self.cmd = command
        self.flags = flags
        self.verbose = verbose
        if self.quiet:
            if self.flags is None:
                self.flags = "-q"
            else:
                self.flags += " -q"

        self.files = []
        self.dirs = []
        self.ls_out = []

    # This will run HSI and capture any output EXCEPT for the passcode prompt, if the user so wanted to use RSA auth and not keytabs
    # If you're reading this, PLEASE tell the user they should just use keytabs. Their life will be easier. Our life will be eaiser.
    # Puppies will befriend kittens and the world will be at peace.
    def run(self, continueOnFailure=False):
        global DYING
        cmd = self.hsi.split(" ")
        if self.flags is not None:
            cmd.extend(self.flags.split(" "))
        cmd.extend(self.cmd.split(" "))

        LOGGER.debug(f"Running: {' '.join(cmd)}")

        output = []
        p = Popen(
            cmd,
            stdout=PIPE,
            stderr=STDOUT,
            bufsize=1,
            universal_newlines=True,
            preexec_fn=os.setsid,
        )

        global PIDS
        PIDS.append(p.pid)
        errline = ""
        encountered_err = False
        ignore_rc = False
        sinceLastYield = int(time.time())
        try:
            for line in p.stdout:
                # last ditch effort to kill HSI
                if DYING:
                    LOGGER.info(f"Killing HSI process {p.pid}")
                    p.kill()
                if "PASSCODE" in line:
                    print(line)
                if encountered_err is True and ".Trash" in line:
                    ignore_rc = True
                    encountered_err = False
                    errline = ""
                elif encountered_err is True and ".Trash" not in line:
                    LOGGER.error("\n".join([errline, line]))
                    encountered_err = False
                    errline = ""

                if ("***" in line and "HPSS_E" in line) or ("***" in line and "error -" in line):
                    encountered_err = True
                    errline = line

                # Use output as a buffer, and maybe yield the buffer to the parent function
                # Where the output is processed as it comes in.
                output.append(line.strip())
                if len(output) >= DB_TX_SIZE:
                    LOGGER.debug(
                        f"HSI list speed: {DB_TX_SIZE/(int(time.time())-sinceLastYield)} lines/sec"
                    )
                    sinceLastYield = int(time.time())
                    yield output
                    output.clear()

        except UnicodeDecodeError as e:
            LOGGER.error(f"Invalid data received from HSI. Please ensure every file in this directory tree has appropriate permissions")
            LOGGER.error(f"If you continue to encounter this error, please contact OLCF User Assistance.")
            LOGGER.error(f"To help debug this, please re-run the command with '--preserve-cache', and provide the cache file location in your ticket.")
            cleanup_and_die(990)

        p.wait()
        PIDS.remove(p.pid)

        if p.returncode != 0 and not ignore_rc:
            LOGGER.error(f"HSI failed to run! Returned rc={p.returncode}")
            LOGGER.error(
                "Unable to get file data from HPSS or process killed",
            )
            for line in output:
                LOGGER.debug(line)
            if not continueOnFailure or DYING:
                cleanup_and_die(120)

        PROFILER.snapshot()
        if len(output) > 0:
            yield output

    # Returns the full HSI command that will be run as a string.
    def getcommand(self):
        cmd = self.hsi.split(" ")
        if self.flags is not None:
            cmd.extend(self.flags.split(" "))
        cmd.extend(self.cmd.split(" "))
        return " ".join(cmd)


def handler_filter(handler):
    def logger_filter(extra):
        if hasattr(extra, "block"):
            if extra.block in handler:
                return False
        return True

    return logger_filter


def initPrelogger():
    global LOGGER
    loglevel = logging.WARNING
    LOGGER = logging.getLogger("pre_hsi_xfer")
    LOGGER.setLevel(loglevel)

    cliHandler = logging.StreamHandler()
    cliHandler.setLevel(loglevel)
    cliHandler.setFormatter(CustomFormatter())
    cliHandler.name = "preCliHandler"
    cliHandler.addFilter(handler_filter("pre"))
    LOGGER.addHandler(cliHandler)


def initLogger(verbose):
    global LOGGER
    LOGGER = None

    loglevel = logging.INFO
    if verbose:
        loglevel = logging.DEBUG

    username = getpass.getuser()

    LOGGER = logging.getLogger("hsi_xfer")
    LOGGER.setLevel(loglevel)

    cliHandler = logging.StreamHandler()
    cliHandler.setLevel(loglevel)
    cliHandler.setFormatter(CustomFormatter())
    cliHandler.name = "cliHandler"
    cliHandler.addFilter(handler_filter("cli"))

    syslogHandler = logging.handlers.SysLogHandler(address="/dev/log")
    syslogHandler.setFormatter(
        logging.Formatter(f"hsi_xfer: (user={username}) %(message)s")
    )
    syslogHandler.setLevel(logging.INFO)
    syslogHandler.name = "syslogHandler"
    syslogHandler.addFilter(handler_filter("syslog"))

    LOGGER.addHandler(cliHandler)
    LOGGER.addHandler(syslogHandler)

    if verbose:
        LOGGER.debug("Set logger to debug because --verbose")


class Profiler:
    def __init__(self, trace):
        self.trace = trace
        if trace:
            tracemalloc.start(10)

    def snapshot(self):
        if self.trace:
            self.snap = tracemalloc.take_snapshot()

    def print_report(self):
        if self.trace:
            snap = self.snap
            idx = 0
            for stats in snap.statistics("lineno"):
                print(f"========== SNAPSHOT {idx} =============")
                print(stats)
                print(stats.traceback.format())
                idx += 1

            print("\n=========== USEFUL METHODS ===========")
            print("\nTraceback Limit : ", tracemalloc.get_traceback_limit(), " Frames")

            print("\nTraced Memory (Current, Peak): ", tracemalloc.get_traced_memory())

            print(
                "\nMemory Usage by tracemalloc Module : ",
                tracemalloc.get_tracemalloc_memory(),
                " bytes",
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "source",
        type=str,
        help="Top level directory in HPSS to pull files from. This is a recursive action!",
    )
    parser.add_argument(
        "destination",
        type=str,
        help="Top level directory on Kronos to put files. An identical directory structure will be created here as exists in HPSS under `source`",
    )
    parser.add_argument(
        "-D",
        "--dry-run",
        action="store_true",
        default=False,
        help="Generate the file lists but do not actually checksum or transfer files, and output the `hsi` commands that would have been used",
    )
    parser.add_argument(
        "-C",
        "--cache-path",
        default=None,
        type=str,
        help="Path to existing cache generated by --preserve-cache, or from an interrupted job. Picks up where the last job left off",
    )
    parser.add_argument(
        "-l",
        "--preserve-file-lists",
        action="store_true",
        default=False,
        help="Do not delete the file list generated by this script. Will write the list to the cwd or the value of --preserved-files-path",
    )
    parser.add_argument(
        "-P",
        "--preserve-timestamps",
        action="store_true",
        default=False,
        help="Overwrites timestamps on transferred files to match timestamps of file within HPSS",
    )
    parser.add_argument(
        "-p",
        "--preserve-cache",
        action="store_true",
        default=False,
        help="Do not delete the cache. This can be useful for restarting an interrupted migration",
    )
    parser.add_argument(
        "-c",
        "--disable-checksums",
        action="store_true",
        default=False,
        help="Disable the checksumming operation. This can speed things up, but at the cost of not verifying data integrity on the destination",
    )
    parser.add_argument(
        "-a",
        "--additional-hsi-flags",
        default=None,
        type=str,
        help="Additional flags needed to be passed to `hsi`",
    )
    parser.add_argument(
        "-e",
        "--overwrite-existing",
        default=False,
        action="store_true",
        help="Force overwrite of existing files that match the file names in HPSS",
    )
    parser.add_argument(
        "-f",
        "--preserved-files-path",
        default=os.getcwd(),
        type=str,
        help="Sets the path that the transfer report and/or database/file list are written to. This can be useful if the DB and resulting files lists/transfer report are too large for your current working directory",
    )
    parser.add_argument(
        "--checksum-threads", help=argparse.SUPPRESS, default=4, type=int
    )
    parser.add_argument(
        "--debug", default=False, help=argparse.SUPPRESS, action="store_true"
    )
    parser.add_argument(
        "-T", "--disable-ta", default=False, help=argparse.SUPPRESS, action="store_true"
    )
    parser.add_argument(
        "-s", "--db-tx-size", help=argparse.SUPPRESS, default=9000, type=int
    )
    parser.add_argument(
        "-r", "--trace", default=False, help=argparse.SUPPRESS, action="store_true"
    )
    parser.add_argument(
        "-V", "--vvs-per-job", default=VVS_PER_JOB, help=argparse.SUPPRESS, type=int
    )
    parser.add_argument(
        "-t", "--parallel-migration-count", default=MIGRATION_THREADS, help=argparse.SUPPRESS, type=int
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        help="Output additional information about the transfer",
        action="store_true",
    )
    parser._positionals.title = "NOTE: While this script will work with PASSCODE auth, it is HIGHLY recommended to use standard 'keytab' auth (e.g. do not use --additional-hsi-flags unless you absolutely must!)\n\npositional arguments"  # pylint: disable=W0212
    # Get our cli args
    args = parser.parse_args()

    initLogger(args.verbose)

    # if args.parallel_migration_count != 1 or args.debug or args.disable_ta or args.db_tx_size != 9000 or args.trace or args.checksum_threads != 4:
    if (
        args.debug
        or args.disable_ta
        or args.db_tx_size != 9000
        or args.trace
        or args.checksum_threads != 4
        or args.parallel_migration_count != VVS_PER_JOB
    ):
        LOGGER.error("Hidden flag usage detected", extra={"block": "cli"})
        LOGGER.error(f"Flag: --parallel-migration-count={args.parallel_migration_count}", extra={'block':'cli'})
        LOGGER.error(
            f"Flag: --checksum-threads={args.checksum_threads}", extra={"block": "cli"}
        )
        LOGGER.error(f"Flag: --debug={args.debug}", extra={"block": "cli"})
        LOGGER.error(f"Flag: --disable-ta={args.disable_ta}", extra={"block": "cli"})
        LOGGER.error(f"Flag: --db-tx-size={args.db_tx_size}", extra={"block": "cli"})
        LOGGER.error(f"Flag: --trace={args.trace}", extra={"block": "cli"})

    global CACHE
    cleanup_db = True
    cleanup_filelists = (
        not args.preserve_file_lists
    )  # The logic here is inverse so we ! the flags value
    if args.preserve_cache:
        cleanup_db = False
        cleanup_filelists = False

    LOGGER.debug(
        f"Creating cache: preserved_files_path={args.preserved_files_path} cleanup_filelists={cleanup_filelists} cleanup_db={cleanup_db} existing_path={args.cache_path} debug={args.debug}"
    )
    CACHE = Cache(
        args.verbose,
        args.preserved_files_path,
        cleanup_filelists=cleanup_filelists,
        cleanup_db=cleanup_db,
        existing_path=args.cache_path,
        debug=args.debug,
    )

    global DB_TX_SIZE
    DB_TX_SIZE = args.db_tx_size

    global PROFILER
    PROFILER = Profiler(args.trace)

    LOGGER.info(
        "Starting sorted batch transfer from HPSS (%s) to destination (%s)",
        args.source,
        args.destination,
    )
    # Create our migration job
    job = MigrateJob(args)

    if args.cache_path is not None:
        LOGGER.info("Resuming interrupted transfer.")

    # Populate the db with HPSS data
    LOGGER.info("Indexing candidate files for transfer from HPSS...")
    job.getSrcFiles()

    # Check for files that exist in the destination
    LOGGER.info("Checking for existing files...")
    job.checkForExisting()

    # Create the destination directory structure
    if not args.dry_run:
        LOGGER.info("Creating destination directory structure")
        job.createDestTree()

    job.startMigrate()

    # Start checksumming destination files
    # TODO: Can we overlap these threads at some point?
    # We will need the destchecksumthread to poll the filesystem and DB to detect when files are done transferring
    report = {}
    global DYING
    if not args.dry_run and not args.disable_checksums:
        job.startDestChecksumming()
        # Generate our report
        report = job.checksumMismatchReport()

    # generate report
    finalreport = job.genReport(report)

    LOGGER.info("Generating transfer report...")
    LOGGER.debug(json.dumps(finalreport, indent=2))

    # Write the report to a file
    reportfilename = "hsi_xfer_report_{}.json".format(int(time.time()))
    reportfilename = os.path.join(CACHE.get_unpacked_report_dir(), reportfilename)
    with open(reportfilename, "w", encoding="UTF-8") as f:
        json.dump(finalreport, f, indent=2)
    LOGGER.info("Transfer complete. Report has been written to %s", reportfilename)
    if not args.dry_run:
        LOGGER.info(f"Average transfer speed: {finalreport['average_speed']} MB/s")

        syslogpayload = {
            "total_size_in_bytes": finalreport["total_size_in_bytes"],
            "elapsed_time": finalreport["elapsed_time"],
            "num_successful_files": len(finalreport["successful_transfers"]),
            "num_failed_files": len(finalreport["failed_transfers"]),
            "num_files_failed_to_checksum": len(finalreport["failed_to_checksum"]),
            "num_files_skipped": len(finalreport["existing_files_skipped"]),
        }

        LOGGER.info(f"finalreport={json.dumps(syslogpayload)}", extra={"block": "cli"})

    PROFILER.print_report()
    DATABASE.cleanup()
    CACHE.cleanup()
    LOCKFILE.cleanup()


def __handle_sigs(signum, frame):
    global DYING
    if not DYING:
        LOGGER.error(
            f"Caught signal: {signum}, Exiting... Please wait for cleanup to finish."
        )
        LOGGER.debug(traceback.format_exc())
        LOGGER.info(traceback.format_exc(), extra={"block": "cli"})
        DYING = True
        cleanup_and_die(999)


def cleanup_and_die(rc, delete_lockfile=True):
    if rc != 0:
        LOGGER.info(traceback.format_exc(), extra={"block": "cli"})
    if DATABASE is not None:
        DATABASE.cleanup()
    if CACHE is not None:
        CACHE.caught_exception()
    if len(PIDS) > 0:
        for pid in PIDS:
            LOGGER.error(f"Killing pid {pid}")
            try:
                os.killpg(os.getpgid(pid), signal.SIGKILL)
            except Exception as e:
                LOGGER.error(
                    f"Could not kill PID {pid}. Please ensure this process has been killed before restarting!"
                )
                LOGGER.debug(e)
    if LOCKFILE is not None and delete_lockfile is True:
        LOCKFILE.cleanup()
    # We can't use sys.exit here because it does not actually kill the main thread.
    # Thanks Python, real helpful
    # Calling os._exit(rc) is *bad* but not *terrible*. It will prevent all destructors from running
    # and does not flush stdout or anything, so you could get into a weird state in your terminal
    # but we're not using curses or anything here so it should be "fine" <--- famous last words
    os._exit(rc)
    #sys.exit(rc)


def entrypoint():
    signal.signal(signal.SIGTERM, __handle_sigs)
    signal.signal(signal.SIGINT, __handle_sigs)
    try:
        # Create a temporary logger for the LOCKFILE here.
        # This will be overwritten in main()
        initPrelogger()
        global LOCKFILE
        LOCKFILE = LockFile(os.path.expanduser("~/.hsi-xfer.lock"))
        # Can we get a lockfile and if not, error and exit
        LOCKFILE.lock()
        sys.exit(main())

    # Ctrl+c will trigger the signal handler, further Ctrl+c will trigger this exception handler
    except Exception as e:
        DYING = True
        LOGGER.error(
            f"Caught exception: {e}, Exiting... Please wait for cleanup to finish."
        )
        LOGGER.debug(traceback.format_exc())
        LOGGER.info(traceback.format_exc(), extra={"block": "cli"})
        cleanup_and_die(99)
