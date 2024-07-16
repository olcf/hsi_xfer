#!/usr/bin/env python3

# pylint: disable=R,C

import argparse
import json
import logging
import os
import re
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


LOGGER = None
PROFILER = None
DB_TX_SIZE = None # This gets set by the -s flag; defaults to 9000
CACHE = None

# NOTE: If you want to batch up EVERYTHING into a single HSI job,
# set --vvs-per-job=-1

# =============================================================================
# hpss_transfer_sorted_files.py
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
        logging.CRITICAL: bold_red + formatcritical + reset
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
    __tablename__ = 'vvs'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    is_tape: Mapped[bool]

class State(Base):
    __tablename__ = 'state'
    id: Mapped[int] = mapped_column(primary_key=True)
    schema_version: Mapped[int]
    indexing_complete: Mapped[bool]
    filelists_complete: Mapped[bool]
    filelist_name: Mapped[str]
    filelist_count: Mapped[int]


class DestTree(Base):
    __tablename__ = 'desttree'
    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str]
    created: Mapped[bool]

class Files(Base):
    __tablename__ = 'files'
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

#
# Meta-class to track database and filelists created and to package them into an archive in case of interruption
#
# TODO: 
#     *  Change all Database and MigrationJob, etc to grab paths for db or filelist_root_path from the global CACHE
#     *  Implement pack and unpack
#     *  Add a Cache.pack() for when ANYTHING interrupts it. Like a global try, catch or something. Esp ctrl+c
#     *  Implement state table to track if indexing, checksumming, etc has finished. This is to protect against 
#           Interruption of indexing or checksumming operations
#     *  Implement updating the state table
#     *  Users should never care about the cache contents; just where the cache *is* or where the cache *wll be*
#     *  Rename flags, etc to remove references to 'db' and replace with 'cache'
#     *  Cache packing should be the LAST thing to happen ANYWHERE ANYTIME
#     *  Also, do not pack the output report in the cache. Just place this in the output_path (--preserved-files-path?)
#     *  Report output path should also be gotten from the CACHE object. Basically, this object will track all output files
#           that are not part of the actual user data from HPSS
#     *  Maybe use the state table to cache what flags it was run with? That way a user can restart a transfer with something like:
#           ./script.py --cache-path path/to.cache --last-args
#     *  Maybe have a new entry for each attempt of the run in the state table. Last state is always the row with the highest id
#     *  Allows generation of a report to show each attempt and what the last state was
#     *  ^^^^ This may be too much. Not sure we care enough to do this....
#     *  Remove these comments and reformat code
#       
class Cache:
    def __init__(self, verbose, output_path, cleanup_filelists=False, cleanup_db=False, existing_path=None, debug=False):
        self.verbose = verbose 
        self.output_path = output_path
        self.cleanup_filelists = cleanup_filelists
        self.cleanup_db = cleanup_db
        self.existing_path = existing_path
        self.debug = debug

        self.filelists = []
        if self.existing_path is None:
            self.archive_path = os.path.join(
                self.output_path, f"hpss_transfer_sorted_files_{int(time.time())}.cache"
            )
            self.dbpath = os.path.join(self.archive_path, "database.db")
            self.filelists_root_path = os.path.join(self.archive_path, "filelists")
        else:
            self.archive_path = self.existing_path
            self.unpack()
            self.dbpath = os.path.join(
                self.existing_path, "database.db"
            )
            self.filelists_root_path = os.path.join(self.archive_path, "filelists")


    def get_db_path(self):
        return self.dbpath

    def archive(self):
        # TODO: tar self.archive_path and place into output_path
        pass

    def unpack(self):
        # TODO: Untar self.archive_path into output_path
        pass

    def get_unpacked_db_path(self):
        return self.dbpath

    def get_unpacked_filelist_root_path(self):
        return self.filelists_root_path

    def get_unpacked_archive_path(self):
        return self.archive_path

# Object to wrap communications to and from the DB. This also ensures that db
# communication is locked so that threads don't try
# and access the db at the same time b/c sqlite
class Database:
    def __init__(self, cleanup, verbose, output_path, disable_checksums, path=None, debug=False):
        self.disable_checksums = disable_checksums
        self.verbose = verbose
        self.output_path = output_path
        self.preservedb = cleanup
        self.vvs = {}
        self.desttrees = {}
        self.debug = debug

        if path is None:
            self.unmanaged_db = False
            self.dbpath = os.path.join(
                self.output_path, f"hpss_transfer_sorted_files_{int(time.time())}.db"
            )
            CACHE.add_db(self.dbpath)
        else:
            self.unmanaged_db = True
            self.dbpath = path
            CACHE.add_db(self.dbpath, packed=True)

        #Connect to DB
        try:
            self.engine = create_engine(f"sqlite:///{self.dbpath}", echo=self.debug)
            Base.metadata.create_all(self.engine)
        except Exception as e:  # pylint: disable=bare-except
            LOGGER.critical(
                    "Can not open %s. Database may be corrupt: %s", self.dbpath, e
            )
            sys.exit(3)

        metadata = sqlalchemy.MetaData()
        self.vvs_tbl = sqlalchemy.Table('vvs', metadata, autoload_with=self.engine)
        self.files_tbl = sqlalchemy.Table('files', metadata, autoload_with=self.engine)
        self.desttree_tbl = sqlalchemy.Table('desttree', metadata, autoload_with=self.engine)
        #self.destination_tbl = sqlalchemy.Table('destination', metadata, autoload_with=self.engine)

    # Destructor. If we want to preserve the db, don't delete it. Otherwise blow it away
    def __del__(self):
        if self.preservedb:
            LOGGER.info("DB saved at %s", self.dbpath)
        else:
            if not self.unmanaged_db:
                os.remove(self.dbpath)

    # Populate the desttree table. Maps files to destination paths
    def insertdesttree(self, tree):
        LOGGER.debug(f"Batch inserting destination tree into 'desttree' table (num_dirs={len(tree)})...")
        try:
            if len(tree) > 0:
                with Session(bind=self.engine) as s:
                    s.add_all(tree)
                    #s.execute(
                    #        insert(DestTree).values(tree)
                    #)
                    s.flush()
                    for t in tree:
                        self.desttrees[t.path] = t.id
                    s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath} (insertdesttree)")
            LOGGER.debug(f"{e}")
            sys.exit(111)
    

    # Insert file entries into the files table
    def insertfiles(self, files, srcroot, destroot):
        LOGGER.debug(f"Batch inserting files into 'files' table (num_files={len(files)})...")

        #TODO: may need to chunk 'files' up
        files_to_insert = []
        destination_to_insert = []
        for file in files[:]:
            vv = file[1].split(',')[0]
            fpath = file[0]
            if len(vv) != 8:
                LOGGER.debug(f"No VV found for file {fpath}; skipping and will not track!")
                LOGGER.error(f"Could not read HPSS metadata for file {fpath}; skipping...")
                continue
            vvid = self.vvExists(vv) 
            dest = fpath.replace(srcroot, destroot)
            #if not self.fileExists(fpath): # This does a select for each file
            if vvid is None:
                self.insertvv(vv)
                vvid = self.vvExists(vv) 
                if vvid is None:
                    LOGGER.fatal("Insert of VV failed!")
                    sys.exit(100)

            destid = self.getdest(dest)[0] # This can do a select for each file
            if destid == []:
                LOGGER.critical("No destination found; Cowardly failing...")
                sys.exit(2)

            files_to_insert.append(Files(path=fpath, vv=vvid, dest=dest, destdir=destid, checksum_status=ChecksumStatus.not_attempted.value, transfer_status=TransferStatus.not_started.value, destfileexists=False))

            files.remove(file)

        # Perform the actual inserts
        try:
            with Session(bind=self.engine) as s:
                s.add_all(files_to_insert)
                s.flush()
                #s.insert(Destination).values(destination_to_insert)
                #s.add_all([ Destination(path=f.path, completed=False, srcfile=f.id) for f in files_to_insert ])
                s.flush()
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Returns all files where the transfer is complete, and the source checksum did not fail
    def getnonfailedsrcchecksumfiles(self):
        stmt = select(Files.path, Files.dest, Files.id).select_from(Files).where(Files.checksum_status != ChecksumStatus.failed.value).where(Files.transfer_status == TransferStatus.completed.value)

        rows = []
        with Session(self.engine) as s:
            #"SELECT path,dest,id FROM files WHERE transfer_status=? AND checksum_status <> ?",
            rows = s.execute(stmt).fetchall()

        return rows

    # returns all files that failed transfers
    def getfailedtransfers(self):
        stmt = select(Files.path, Files.dest).select_from(Files).where(Files.transfer_status == TransferStatus.failed.value)
        rows = []
        with Session(self.engine) as s:
            #"SELECT path,dest FROM files WHERE transfer_status=?",
            rows = s.execute(stmt).fetchall()
        return rows

    # Gets all files that failed checksumming
    def getfailedchecksums(self):
        stmt = select(Files.path, Files.src_checksum, Files.dest_checksum).select_from(Files).where(Files.checksum_status == ChecksumStatus.failed.value)
        rows = []
        with Session(self.engine) as s:
            #"SELECT path,src_checksum,dest,dest_checksum FROM files WHERE checksum_status=?",
            rows = s.execute(stmt).fetchall()
        return rows

    # Get all files that successfully finished transferrring and checksumming
    def getsuccessfultransfers(self):
        stmt = select(Files.path, Files.dest, Files.dest_checksum).select_from(Files).where(Files.checksum_status == ChecksumStatus.completed.value).where(Files.transfer_status == TransferStatus.completed.value)
        rows = []
        with Session(self.engine) as s:
            #"SELECT path,dest,dest_checksum FROM files WHERE checksum_status=? AND transfer_status=?",
            rows = s.execute(stmt).fetchall()
        return rows

    # Mark a file entry as failed to transfer
    def markasfailed(self, srcpath):
        stmt = update(Files).where(Files.path == srcpath).values(transfer_status=TransferStatus.failed.value)
        try:
            with Session(self.engine) as s:
                #"UPDATE files SET transfer_status=? WHERE path = ?",
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Mark a destination driectory as created
    def markdestcreated(self, destid):
        stmt = update(DestTree).values(created=True).where(DestTree.id == destid)
        try:
            with Session(self.engine) as s:
                #"UPDATE desttree SET created=1 WHERE id = ?"
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)


    # Marks a file as 'staging' this is more or less equiv to 'transferring'
    def markfileasstaging(self, fileid):
        stmt = update(Files).values(transfer_status=TransferStatus.staging.value).where(Files.id == fileid)
        try:
            with Session(self.engine) as s:
                #"UPDATE files SET transfer_status=? WHERE id = ?",
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Set the value of the hpss-derived checksum for the file object
    def setsrcchecksum(self, srcpath, checksum):
        stmt = update(Files).values(src_checksum=checksum).where(Files.path == srcpath)
        try:
            with Session(self.engine) as s:
                #"UPDATE files SET src_checksum=? WHERE path = ?"
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Sets the value of the md5sum performed at the destination
    def setdestchecksum(self, fileid, checksum):
        stmt = update(Files).values(dest_checksum=checksum).where(Files.id == fileid)
        try:
            with Session(self.engine) as s:
                #"UPDATE files SET dest_checksum=? WHERE id = ?", [checksum, fileid]
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Marks the checksum status of a file as completed
    def markfilechecksumascomplete(self, fileid):
        stmt = update(Files).values(checksum_status=ChecksumStatus.completed.value).where(Files.id == fileid)
        try:
            with Session(self.engine) as s:
                #"UPDATE files SET checksum_status=? WHERE id = ?",
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Marks the checksum status of a file as failed
    def markfilechecksumasfailed(self, fileid):
        stmt = update(Files).values(checksum_status=ChecksumStatus.failed.value).where(Files.id == fileid)
        try:
            with Session(self.engine) as s:
                #"UPDATE files SET checksum_status=? WHERE id = ?",
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)


    # Marks the file trasfer as completed
    def markfileastransfercompleted(self, srcpath):
        stmt = update(Files).values(transfer_status=TransferStatus.completed.value).where(Files.path == srcpath)
        try:
            with Session(self.engine) as s:
                #"UPDATE files SET checksum_status=? WHERE id = ?",
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Marks the file as hpss_checksum_complete
    def markfileashpsschecksumcomplete(self, srcpath):
        stmt = update(Files).values(checksum_status=ChecksumStatus.hpss_complete.value).where(Files.path == srcpath)
        try:
            with Session(self.engine) as s:
                #"UPDATE files SET checksum_status=? WHERE path = ?",
                s.execute(stmt)
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Inserts a VV into the vvs table
    def insertvv(self, vv):
        istape = False
        if "X" in vv or "H" in vv:
            istape = True
        v = VV(name=vv, is_tape=istape)
        #stmt = insert(VV).values(name=vv, is_tape=istape)
        try:
            with Session(self.engine) as s:
                #s.execute(stmt)
                s.add(v)
                s.flush()
                self.vvs[vv] = v.id
                s.commit()
                #row = s.execute(select(VV.id).select_from(VV).where(VV.name == vv)).fetchall()
                #self.vvs[vv] = row[0]

        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)

    # Retrieve a vv entry by name
    def getvv(self, vv):
        row = None
        with Session(self.engine) as s:
            row = s.execute(
                    select(VV.id, VV.name).select_from(VV).where(VV.name==vv)
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
            return [ self.desttrees[dirname] ]
        else:
            row = None
            with Session(self.engine) as s:
                row = s.execute(
                        select(DestTree.id, DestTree.path).select_from(DestTree).where(DestTree.path == dirname)
                ).first()
            return row if row is not None else []
#        rows = self._get_rows(
#            "SELECT id, path FROM desttree WHERE path LIKE ?", [dirname]
#        )
#        return rows[0]

    # Get file entries where the source and dest checksums do not match
    def getnonmatchingchecksums(self):
        stmt = select(Files.path, Files.src_checksum, Files.dest_checksum).select_from(Files).where(Files.dest_checksum != Files.src_checksum)

        if self.disable_checksums:
            stmt = select(Files.path, Files.src_checksum, Files.dest, Files.dest_checksum).select_from(Files).where((Files.dest_checksum != Files.src_checksum) | (Files.dest_checksum == None & Files.src_checksum == None & Files.destfileexists == True))

        rows = []
        with Session(self.engine) as s:
            rows = s.execute(stmt).fetchall()
        #suffix = "OR (dest_checksum IS NULL AND src_checksum IS NULL AND destfileexists <> 1)"
        #if self.disable_checksums:
        #    suffix = ""

        #rows = self._get_rows(
        #    "SELECT path,src_checksum,dest,dest_checksum FROM files WHERE dest_checksum <> src_checksum {}".format(suffix) 
        #)
        return rows

    # vvExists will return None if theres no entries, and the rowid if there is
    def vvExists(self, vv):
        if vv not in self.vvs:
            return None
        else:
            return self.vvs[vv]

        #row = self.getvv(vv)
        #if row is not None:
        #    if row[1] == vv:
        #        return row[0]
        #return None

    # fileExists will return None if theres no entries, and the rowid if there is
    def fileExists(self, file):
        rows = self.getfile(file)
        for row in rows:
            if row.path == file:
                return row.id 
        return None

    # Get the total number of files in the files table in the db
    def gettotalnumberoffiles(self):
        row = None
        with Session(self.engine) as s:
            row = s.execute(select(func.count('*')).select_from(Files)).one_or_none()
        return row if row is not None else []

    # Get files marked as 'destination file exists'
    def getexistingfiles(self):
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(
                    select(Files.path, Files.dest, Files.id).select_from(Files).where(Files.destfileexists==True)
            ).fetchall()
        return rows
    #rows = self._get_rows("SELECT path,dest,id FROM files WHERE destfileexists=1")

    # get a file by source path
    def getfile(self, file):
        rows = []
        with Session(self.engine) as s:
            rows = s.execute(
                    select(Files.id, Files.path).select_from(Files).where(Files.path == file)
            ).fetchall()
        return rows
    #rows = self._get_rows("SELECT id, path FROM files WHERE path LIKE ?", [file])

    # Get a list of files by VV (DB) id
    def getfilesbyvv(self, vvid):
        rows = []
        offset = 0
        with Session(self.engine) as s:
            while True:
                rows = s.execute(
                        select(Files.path, Files.dest, Files.id).select_from(Files).where(Files.vv == vvid).where(Files.transfer_status != TransferStatus.completed.value).order_by(self.files_tbl.c.id).offset(offset).limit(DB_TX_SIZE)
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
                    select(distinct(Files.vv)).select_from(Files).where(Files.transfer_status != TransferStatus.completed.value)
            ).fetchall()
        return rows

    # dump the desttree table
    def dumpdesttree(self):
        rows = []
        with Session(self.engine) as s:
            #rows = s.execute(select(DestTree).order_by(func.length(DestTree.path))).fetchall()
            rows = s.query(DestTree).filter(func.length(DestTree.path)).all()
        return rows

    # Mark a file as 'destination file exists'
    def markexists(self, fileid):
        try:
            with Session(self.engine) as s:
                s.execute(
                        update(Files).values(destfileexists=True, transfer_status=TransferStatus.skipped.value).where(Files.id == fileid)
                )
                s.commit()
        except Exception as e:
            LOGGER.error(f"Could not commit to DB! Please ensure you have the correct permissions to write file: {self.dbpath}")
            LOGGER.debug(f"{e}")
            sys.exit(111)


# Object to manage the HSIJobs required for migration of files in the DB
class MigrateJob:
    def __init__(self, args):
        self.source = os.path.normpath(args.source)
        self.destination = os.path.abspath(os.path.normpath(args.destination))
        self.dry_run = args.dry_run
        self.verbose = args.verbose
        self.output_path = os.path.normpath(args.preserved_files_path)

        self.preserve_file_lists = args.preserve_file_lists
        self.preserve_db = args.preserve_db
        self.disable_checksums = args.disable_checksums
        self.vvs_per_job = args.vvs_per_job
        self.addl_flags = args.additional_hsi_flags
        self.overwrite = args.overwrite_existing
        self.disable_ta = args.disable_ta
        self.checksum_threads = args.checksum_threads
        self.hsi = "/sw/sources/hpss/bin/hsi"

        # If --db-path is defined, do not create a new db, use the one at the path provided
        if args.db_path is not None:
            self.db = Database(
                self.preserve_db, self.verbose, self.output_path, self.disable_checksums, path=args.db_path, debug=args.debug
            )
        else:
            self.db = Database(self.preserve_db, self.verbose, self.output_path, self.disable_checksums, debug=args.debug)

        # Define the HSIJob that lists all the files recursively in the source directory
        self.ls_job = HSIJob(
            self.hsi,
            self.addl_flags,
            True,
            self.verbose,
            "ls -a -N -R -P {}".format(self.source),
        )

    # Check the destination filesystem for files that match names at the predicted destination directory
    def checkForExisting(self):
        if not self.overwrite:
            files = self.db.dumpfiles()

            for file in files:
                destpath = file[10]
                fileid = file[0]
                if os.path.isfile(destpath):
                    self.db.markexists(fileid)

    # Create the destination directory structure
    def createDestTree(self):
        destpaths = self.db.dumpdesttree()
        for path in destpaths:
            try:
                os.makedirs(path.path, exist_ok=True)
            except FileExistsError:
                LOGGER.debug("Skipping mkdir of %s; directory already exists", path.path)
                self.db.markdestcreated(path.id)
            except Exception as e:  # pylint: disable=bare-except
                LOGGER.critical(
                    "ERROR: Could not create destination directory (%s).\n(%s)", path.path, e
                )
                sys.exit(3)
            else:
                self.db.markdestcreated(path.id)

    def runHashList(self, files):
        infilename = os.path.join(
            self.output_path, f"hpss_transfer_sorted_files.hashlist.list"
        )
        # Write the infile to be passed into HSI
        with open(infilename, "w", encoding="UTF-8") as infile:
            infile.write("\n".join(["hashlist -A {}".format(f[0]) for f in files]))
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
                        LOGGER.debug("Found preexisting hash for file: %s : %s", srcpath, checksum)
                        ret[srcpath] = checksum

        return ret

    def writeExistingHashes(self, hashlistout):
        for path in hashlistout:
            self.db.setsrcchecksum(path, hashlistout[path])
            self.db.markfileashpsschecksumcomplete(path)

    # This is the main migration thread! All the fun stuff happens here
    # - Generates the file lists
    # - Calls HSI to migrate and hash files
    # - Populate the db with the resulting information
    # - Profit
    def startMigrate(self):
        # Generator to created a chunked list of vv's
        def chunk(lst, chunksize):
            if chunksize < 0 and chunksize != -1:
                LOGGER.critical(
                    "vvs-per-job needs to be either -1 or a positive integer!"
                )
                sys.exit(4)
            if chunksize == -1:
                yield lst
            for i in range(0, len(lst), chunksize):
                yield lst[i : i + chunksize]

        # Get chunked list of vvs
        allvvs = self.db.getvvswithnoncompletefiles()
        vvs = list(chunk(allvvs, int(self.vvs_per_job)))

        # Get list of exisitng files from the db
        existingFiles = self.db.getexistingfiles()

        if self.overwrite:
            existingFiles = []

        if len(existingFiles) > 0:
            LOGGER.debug("Found existing files:")
            # Format existingFiles so it looks better
            existing = []
            for f in existingFiles:
                existing.append({"source": f[0], "destination": f[1]})
            LOGGER.debug(json.dumps(existing, indent=2))

        LOGGER.debug(
            "Creating file lists from indexed HPSS data..."
        )

        # Set some variables for status output (esp if not using the progress bar)
        filelistnum = 0
        filesCompleted = len(existingFiles)
        filesTotal = self.db.gettotalnumberoffiles()[0]

        # If we don't set --additional-hsi-flags, assume we're using keytabs for auth. If so, then we can use the progress bar
        # Otherwise, the progress bar will overwrite the PASSCODE: prompt so it'll just appear to 'hang' for users

        # Launch an HSI job for each chunk of vvs (generally 1 vv/chunk but see below)
        for vvlist in vvs:
            files = []
            # Unpack chunk. Will generally just be a list of 1 vvid, but in case --vvs-per-job is set, this will chunk it up
            for vv in vvlist:
                filelistnum += 1
                tmpgetfilename = os.path.join(
                    self.output_path, f"hpss_tsf_tmp_get.{filelistnum}.list"
                )
                tmphashfilename = os.path.join(
                    self.output_path, f"hpss_tsf_tmp_hash.{filelistnum}.list"
                )
                infilename = os.path.join(
                    self.output_path, f"hpss_transfer_sorted_files.{filelistnum}.list"
                )
                with open(tmpgetfilename, "a", encoding="UTF-8") as getfile, open(tmphashfilename, "a", encoding="UTF-8") as hashfile:
                    for chunk in self.db.getfilesbyvv(vv[0]):
                        LOGGER.debug("Writing chunk of %s to temporary file list...", DB_TX_SIZE)
                        files = chunk
                        files = [f for f in files if f not in existingFiles]

                        # Do a hashlist to get files that have hashes already
                        hashlistout = self.runHashList(files)
                        # if file has hash; save to db
                        self.writeExistingHashes(hashlistout)
                        # to_hash gets written to the infile; add a hashcreate for all files that don't have hashes already
                        to_hash = [ f for f in files if f[0] not in hashlistout ]


                        #TODO: Ensure that this does not kill performance
                        # We're going to be writing multiple gets and hashcreate commands here
                        # Write each to a separate file, and append to infile at the end?
                        if len(files) > 0:
                            getfile.write(
                                "\n".join(["{} : {}".format(f[1], f[0]) for f in files])
                            )
                            getfile.write("\n")
                            # Add a hashcreate command to generate and get hashes in the same job.
                            # We're not doing this in parallel here, since it'll create a lot of PASSCODE prompts if a user isn't using keytabs
                            # And since they both (get and hashcreate) need to stage the data, we're not introducing too much extra runtime here
                            if not self.disable_checksums and len(to_hash) > 0:
                                hashfile.write("\n".join(["{}".format(f[0]) for f in to_hash]))
                                hashfile.write("\n")

                # Get the length of the hashfiles
                hashfilelen = 0
                with open(tmphashfilename, "r", encoding="UTF-8") as hashfile:
                    hashfilelen = sum(1 for _ in hashfile)

                # Combine both the hashfile and getfile here
                with open(infilename, "w", encoding="UTF-8") as infile, open(tmpgetfilename, "r", encoding="UTF-8") as getfile, open(tmphashfilename, "r", encoding="UTF-8") as hashfile:
                    LOGGER.debug("Combining temporary file lists")
                    # Write the infile to be passed into HSI
                    getcmd = "get -T on << EOF\n"
                    if self.disable_ta:
                        getcmd = "get -T off << EOF\n"

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

                # This is our HSI job
                migration_job = HSIJob(
                    self.hsi,
                    self.addl_flags,
                    True,
                    self.verbose,
                    "in {}".format(infilename),
                )
                LOGGER.info(
                    "Starting migration of files from %s to %s (dry-run=%s)", self.source, self.destination, self.dry_run
                )
                if not self.dry_run:
                    for f in files:
                        self.db.markfileasstaging(f[2])
                    # Run the job!
                    # Scan through the output looking for errors and md5 sums. Populate the db accordingly
                    last_notif_time = int(time.time())
                    first_iter = True
                    for chunk in migration_job.run(continueOnFailure=True):
                        for line in chunk:
                            if int(time.time()) - last_notif_time >= 30 or first_iter:
                                first_iter = False
                                LOGGER.info(
                                    "%s/%s file transfers have been attempted",
                                    filesCompleted,
                                    filesTotal,
                                )
                            if "(md5)" in line:
                                checksum, srcpath = line.split(" ")[0], line.split(" ")[-1]
                                self.db.setsrcchecksum(srcpath, checksum)
                                self.db.markfileashpsschecksumcomplete(srcpath)

                            matches = re.match("get\s\s'([^']+/(?:[^/]+/)*[^']+[^/]*)'\s:\s'([^']+/(?:[^/]+/)*[^']+[^/]*)'(?:\s+\([^)]+\))?", line)
                            if matches:
                                srcpath = matches.group(2)
                                filesCompleted += 1
                                LOGGER.debug("Processed file (%s/%s): %s", filesCompleted, filesTotal, srcpath)
                                self.db.markfileastransfercompleted(srcpath)

                            if "get: Error" in line:
                                LOGGER.error(
                                    "File %s failed to transfer from HPSS",
                                    line.split(" ")[-1],
                                )
                                self.db.markasfailed(line.split(" ")[-1])
                    PROFILER.snapshot()
                    # Update our stats
                else:
                    LOGGER.info("Would have run: %s", migration_job.getcommand())

                # Remove infile list if we're not worried about preserving
                if not self.preserve_file_lists:
                    os.remove(infilename)
        return 0

    # This is our main destination checksumming thread. This will be pretty fast b/c GPFS.
    # We're defaulting to a 4 thread pool for checksumming. This can be overwritten by --checksum-threads
    def startDestChecksumming(self):
        files = self.db.getnonfailedsrcchecksumfiles()
        values = []
        with Pool(self.checksum_threads) as p:
            values = p.map(doDestChecksum, files)

        for value in values:
            if value["returncode"] != 0:
                self.db.markfilechecksumasfailed(value["id"])
            else:
                self.db.markfilechecksumascomplete(value["id"])
            self.db.setdestchecksum(value["id"], value["checksum"])

    # Generates a report of any non-matching checksums
    def checksumMismatchReport(self):
        files = self.db.getnonmatchingchecksums()
        ret = {}
        ret["checksum_mismatch"] = []
        if len(files) > 0:
            for file in files:
                LOGGER.debug(
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
        failedtransfers = self.db.getfailedtransfers()
        report["failed_transfers"] = []
        for f in failedtransfers:
            report["failed_transfers"].append({"source": f[0], "destination": f[1]})
        failedchecksums = self.db.getfailedchecksums()
        report["failed_to_checksum"] = []
        for f in failedchecksums:
            report["failed_to_checksum"].append(
                {"source": (f[0], f[1]), "destination": (f[2], f[3])}
            )
        succeededfiles = self.db.getsuccessfultransfers()
        report["successful_transfers"] = []
        for f in succeededfiles:
            report["successful_transfers"].append(
                {"source": f[0], "destination": f[1], "checksum": f[2]}
            )
        existingfiles = self.db.getexistingfiles()
        report["existing_files_skipped"] = []
        for f in existingfiles:
            report["existing_files_skipped"].append(
                {"source": f[0], "destination": f[1]}
            )

        return report

    @property
    def _hpss_root_dir(self):
        path = self.source
        while any(char in "*?[]" for char in path):
            path = os.path.dirname(path)
        return path

    # This gets the list of files from HPSS to be inserted into the DB
    def getSrcFiles(self):
        for chunk in self.ls_job.run(): 
            files = []
            dirs = set()

            for line in chunk:
                s = line.split("\t")
                objtype = s[0]
                # If this is a file entry, save it with the VV, and infer the directory
                if "FILE" in objtype:
                    file, vv = s[1], s[5]
                    files.append((file, vv))
                    dirs.add(os.path.dirname(file))

            if len(files) == 0:
                LOGGER.critical("No in HPSS files matched query: %s", self.source)
                sys.exit(6)
            # Removing junk from array
            dirs = [DestTree(created=False, path=d.replace(self._hpss_root_dir, self.destination)) for d in dirs if d.replace(self._hpss_root_dir, self.destination) not in self.db.desttrees ]
            # Adding the root destination since files can be at depth 0, and we want to make sure its there
            if self.destination not in self.db.desttrees:
                dirs.append(DestTree(created=False, path=self.destination))
            if len(dirs) > 0:
                self.db.insertdesttree(dirs)
            if len(files) > 0:
                self.db.insertfiles(files, self._hpss_root_dir, self.destination)
        PROFILER.snapshot()


# Due to the fact that multiprocessing is weird, and pickles things that get passed to the pool (including the function itself),
# and the fact that the MigrationJob object contains a Database object that contains non-serializable data (thread locks), we go
# ahead and play it safe and pull this function outside the object. Yay.
# This function performs a single md5sum on a single file in the destination. Thread pool worker
def doDestChecksum(file):
    checksum = ""

    cmd = "md5sum {}".format(file[1]).split(" ")

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
        cmd = self.hsi.split(" ")
        if self.flags is not None:
            cmd.extend(self.flags.split(" "))
        cmd.extend(self.cmd.split(" "))

        output = []
        p = Popen(
            cmd,
            stdout=PIPE,
            stderr=STDOUT,
            bufsize=1,
            universal_newlines=True,
        )
        errline = ""
        encountered_err = False
        ignore_rc = False
        for line in p.stdout:
            if "PASSCODE" in line:
                print(line)
            if encountered_err is True and ".Trash" in line:
                ignore_rc = True
                encountered_err = False
                errline = ""
            elif encountered_err is True and ".Trash" not in line:
                LOGGER.error('\n'.join([errline, line]))
                encountered_err = False
                errline = ""

            if "***" in line and "HPSS_E" in line:
                encountered_err = True
                errline = line

            #TODO: Don't append all lines to output.
            # Use output as a buffer, and maybe yield the buffer to the parent function
            # Where the output is processed as it comes in. 
            output.append(line.strip())
            if len(output) >= DB_TX_SIZE:
                yield output
                output.clear()

        p.wait()

        if p.returncode != 0 and not ignore_rc:
            LOGGER.fatal(f"HSI failed to run! Returned rc={p.returncode}")
            LOGGER.fatal(
                "Unable to get file data from HPSS! Please ensure that you can log into HPSS with 'hsi' using keytabs or token authentication, and that you have appropriate read permissions!",
            )
            for line in output:
                LOGGER.debug(line)
            if not continueOnFailure:
                sys.exit(101)

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


def initLogger(verbose):
    global LOGGER
    if verbose:
        # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        LOGGER = logging.getLogger("hpss_transfer_sorted_files")
        LOGGER.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        ch.setFormatter(CustomFormatter())
        LOGGER.addHandler(ch)

        #logging.basicConfig(
        #    level=logging.DEBUG, format="", datefmt="%m/%d/%Y %I:%M:%S %p"
        #)
    else:
        LOGGER = logging.getLogger("hpss_transfer_sorted_files")
        LOGGER.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        ch.setFormatter(CustomFormatter())
        LOGGER.addHandler(ch)
        # logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        #logging.basicConfig(
        #    level=logging.INFO, format="", datefmt="%m/%d/%Y %I:%M:%S %p"
        #)

class Profiler():
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
              
            print("\nMemory Usage by tracemalloc Module : ", tracemalloc.get_tracemalloc_memory(), " bytes")  
                  

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
        "-d",
        "--db-path",
        default=None,
        type=str,
        help="Path to existing db generated by --preserve-db, or from an interrupted job. Picks up where the last job left off",
    )
    parser.add_argument(
        "-l",
        "--preserve-file-lists",
        action="store_true",
        default=False,
        help="Do not delete the file list generated by this script. Will write the list to the cwd",
    )
    parser.add_argument(
        "-p",
        "--preserve-db",
        action="store_true",
        default=False,
        help="Do not delete the db cache. This can be useful for restarting an interrupted migration",
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
    parser.add_argument("--vvs-per-job", help=argparse.SUPPRESS, default=1, type=int)
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
        "-t", "--trace", default=False, help=argparse.SUPPRESS, action="store_true"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        help="Output additional information about the transfer",
        action="store_true",
    )
    parser._positionals.title = "NOTE: While this script will work with PASSCODE auth, it is HIGHLY recommended to use standard 'keytab' auth (e.g. do not use --additional-hsi-flags unless you absolutely must!\n\npositional arguments"  # pylint: disable=W0212
    # Get our cli args
    args = parser.parse_args()

    global CACHE
    CACHE = Cache(args.verbose, os.path.join(args.preserved_files_path, '/cache'), cleanup_filelists=args.preserve_filelists, cleanup_db=args.preserve_db, existing_path=args.db_path, debug=args.debug)

    global DB_TX_SIZE
    DB_TX_SIZE = args.db_tx_size

    initLogger(args.verbose)

    global PROFILER
    PROFILER = Profiler(args.trace)

    LOGGER.info("Starting sorted batch transfer from HPSS (%s) to destination (%s)", args.source, args.destination)
    # Create our migration job
    job = MigrateJob(args)

    if args.db_path is not None:
        LOGGER.info("Resuming interrupted transfer.")

    # Populate the db with HPSS data
    if args.db_path is None:
        LOGGER.info("Indexing candidate files for transfer from HPSS...")
        job.getSrcFiles()

    # Check for files that exist in the destination
    LOGGER.info("Checking for existing files...")
    job.checkForExisting()

    # Create the destination directory structure
    if not args.dry_run:
        LOGGER.info("Creating destination directory structure")
        job.createDestTree()

    # Create our main threads here. One for migration, the other for checksumming
    migrateThread = threading.Thread(target=job.startMigrate)
    destchecksumThread = threading.Thread(target=job.startDestChecksumming)

    # Start migrating files
    migrateThread.start()
    # Wait until migration is done
    migrateThread.join()

    # Start checksumming destination files
    # TODO: Can we overlap these threads at some point?
    # We will need the destchecksumthread to poll the filesystem and DB to detect when files are done transferring
    if not args.dry_run and not args.disable_checksums:
        destchecksumThread.start()

    # Generate our report
    report = {}
    if not args.dry_run and not args.disable_checksums:
        destchecksumThread.join()
        report = job.checksumMismatchReport()

    # generate report
    finalreport = job.genReport(report)

    LOGGER.info("Generating transfer report...")
    LOGGER.debug(json.dumps(finalreport, indent=2))

    # Write the report to a file
    if not args.dry_run:
        reportfilename = "hpss_transfer_sorted_files_report_{}.json".format(
            int(time.time())
        )
        reportfilename = os.path.join(args.preserved_files_path, reportfilename)
        with open(reportfilename, "w", encoding="UTF-8") as f:
            json.dump(finalreport, f, indent=2)
        LOGGER.info("Transfer complete. Report has been written to %s", reportfilename)

    PROFILER.print_report()


if __name__ == "__main__":
    sys.exit(main())
