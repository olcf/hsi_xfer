#!/usr/bin/env python3

# pylint: disable=R,C

import sys
import logging
import os
import subprocess
from multiprocessing.pool import ThreadPool as Pool
from subprocess import Popen, PIPE
import argparse
import sqlite3
import time
import threading
import json

from enum import Enum
from alive_progress import alive_it


# NOTE: If you want to batch up EVERYTHING into a single HSI job,
# set --vvs-per-job=-1

# =============================================================================
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


# Object to wrap communications to and from the DB. This also ensures that db
# communication is locked so that threads don't try
# and access the db at the same time b/c sqlite
class DBCache:
    def __init__(self, cleanup, verbose, output_path, path=None):
        self.verbose = verbose
        self.output_path = output_path
        self.preservedb = cleanup
        self.lock = threading.Lock()
        self.stmts = []
        if path is None:
            self.unmanaged_db = False
            self.dbpath = os.path.join(
                self.output_path, f"hpss_transfer_sorted_files_{int(time.time())}.db"
            )
            self.db = sqlite3.connect(self.dbpath, check_same_thread=False)
            self.cur = self.db.cursor()

            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS vvs(id INTEGER PRIMARY KEY,
                name TEXT,
                is_tape BOOLEAN)"""
            )
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS desttree(id INTEGER PRIMARY KEY,
                path TEXT,
                created BOOLEAN)"""
            )
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY,
                path TEXT,
                perms TEXT,
                owner_id INTEGER,
                group_id INTEGER,
                src_checksum TEXT,
                dest_checksum TEXT,
                checksum_status INTEGER,
                transfer_status INTEGER,
                vv INTEGER,
                dest TEXT,
                destdir INTEGER,
                destfileexists BOOLEAN,
                FOREIGN KEY (vv) REFERENCES vvs(id),
                FOREIGN KEY (destdir) REFERENCES desttree(id))"""
            )
            self.cur.execute(
                """
                CREATE TABLE IF NOT EXISTS destination(id INTEGER PRIMARY KEY,
                path TEXT,
                completed BOOLEAN,
                srcfile INTEGER,
                FOREIGN KEY (srcfile) REFERENCES files(id))"""
            )
        else:
            self.unmanaged_db = True
            self.dbpath = path
            try:
                self.db = sqlite3.connect(self.dbpath, check_same_thread=False)
                self.cur = self.db.cursor()
            except:  # pylint: disable=bare-except
                logging.critical(
                    "Can not open %s. Database may be corrupt", self.dbpath
                )
                sys.exit(3)

    # Destructor. If we want to preserve the db, don't delete it. Otherwise blow it away
    def __del__(self):
        if self.preservedb:
            logging.info("DB saved at %s", self.dbpath)
        else:
            if not self.unmanaged_db:
                os.remove(self.dbpath)

    def _execute(self, stmt, args=None):
        try:
            self.lock.acquire(True)
            if args is not None:
                self.cur.execute(stmt, args)
            else:
                self.cur.execute(stmt)
            self.db.commit()
        finally:
            self.lock.release()

    def _add_to_tx(self, stmt, args=None):
        tmp = (stmt, args)
        self.stmts.append(tmp)

    def _execute_tx(self):
        try:
            self.lock.acquire(True)
            for stmt in self.stmts:
                if stmt[1] is not None:
                    self.cur.execute(stmt[0], stmt[1])
                else:
                    self.cur.execute(stmt[0])
            self.db.commit()
        finally:
            self.stmts = []
            self.lock.release()

    def _get_rows(self, stmt, args=None, maxrows=None):
        rows = []
        try:
            self.lock.acquire(True)
            if args is not None:
                res = self.cur.execute(stmt, args)
            else:
                res = self.cur.execute(stmt)
            if maxrows == 1:
                rows = res.fetchone()
            else:
                rows = res.fetchall()
        finally:
            self.lock.release()
        return rows

    # Populate the desttree table. Maps files to destination paths
    def insertdesttree(self, tree):
        logging.debug("Building destination tree cache table...")
        if self.verbose:
            tree = alive_it(tree)
        for dest in tree:
            self._add_to_tx(
                "INSERT INTO desttree (path, created) VALUES (?, 0)", [dest]
            )
        self._execute_tx()

    # Insert file entries into the files table
    def insertfiles(self, files, srcroot, destroot):
        logging.debug("Creating files table from HPSS source...")
        if self.verbose:
            files = alive_it(files)
        for file in files:
            vv = file[5]
            fpath = file[1]
            vvid = self.vvExists(vv)
            dest = fpath.replace(srcroot, destroot)
            if not self.fileExists(fpath):
                if vvid is None:
                    self.insertvv(vv)
                    vvid = self.vvExists(vv)

                destid = self.getdest(dest)[0]
                if destid == []:
                    logging.critical("No destination found; Cowardly failing...")
                    sys.exit(2)

                self._execute(
                    """INSERT INTO files (
                        path, 
                        vv, 
                        dest, 
                        destdir, 
                        checksum_status, 
                        transfer_status,
                        destfileexists) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [
                        fpath,
                        vvid,
                        dest,
                        destid,
                        ChecksumStatus.not_attempted.value,
                        TransferStatus.not_started.value,
                        0,
                    ],
                )

                fileid = self.fileExists(fpath)
                if fileid is None:
                    logging.critical(
                        "Commit to DB did not work as intended. Internal Error!"
                    )
                    sys.exit(1)

                self._add_to_tx(
                    "INSERT INTO destination (path, completed, srcfile) VALUES (?, ?, ?)",
                    [fpath, 0, fileid],
                )
            else:
                logging.debug("File already in DB")
        self._execute_tx()

    # Returns all files where the transfer is complete, and the source checksum did not fail
    def getnonfailedsrcchecksumfiles(self):
        rows = self._get_rows(
            "SELECT path,dest,id FROM files WHERE transfer_status=? AND checksum_status <> ?",
            [TransferStatus.completed.value, ChecksumStatus.failed.value],
        )
        return rows

    # returns all files that failed transfers
    def getfailedtransfers(self):
        rows = self._get_rows(
            "SELECT path,dest FROM files WHERE transfer_status=?",
            [TransferStatus.failed.value],
        )
        return rows

    # Gets all files that failed checksumming
    def getfailedchecksums(self):
        rows = self._get_rows(
            "SELECT path,src_checksum,dest,dest_checksum FROM files WHERE checksum_status=?",
            [ChecksumStatus.failed.value],
        )
        return rows

    # Get all files that successfully finished transferrring and checksumming
    def getsuccessfultransfers(self):
        rows = self._get_rows(
            "SELECT path,dest,dest_checksum FROM files WHERE checksum_status=? AND transfer_status=?",
            [ChecksumStatus.completed.value, TransferStatus.completed.value],
        )
        return rows

    # Mark a file entry as failed to transfer
    def markasfailed(self, srcpath):
        self._execute(
            "UPDATE files SET transfer_status=? WHERE path = ?",
            [TransferStatus.failed.value, srcpath],
        )

    # Mark a destination driectory as created
    def markdestcreated(self, destid):
        self._execute("UPDATE desttree SET created=1 WHERE id = ?", [destid])

    # Marks a file as 'staging' this is more or less equiv to 'transferring'
    def markfileasstaging(self, fileid):
        self._execute(
            "UPDATE files SET transfer_status=? WHERE id = ?",
            [TransferStatus.staging.value, fileid],
        )

    # Set the value of the hpss-derived checksum for the file object
    def setsrcchecksum(self, srcpath, checksum):
        self._execute(
            "UPDATE files SET src_checksum=? WHERE path = ?", [checksum, srcpath]
        )

    # Sets the value of the md5sum performed at the destination
    def setdestchecksum(self, fileid, checksum):
        self._execute(
            "UPDATE files SET dest_checksum=? WHERE id = ?", [checksum, fileid]
        )

    # Marks the checksum status of a file as completed
    def markfilechecksumascomplete(self, fileid):
        self._execute(
            "UPDATE files SET checksum_status=? WHERE id = ?",
            [ChecksumStatus.completed.value, fileid],
        )

    # Marks the checksum status of a file as failed
    def markfilechecksumasfailed(self, fileid):
        self._execute(
            "UPDATE files SET checksum_status=? WHERE id = ?",
            [ChecksumStatus.failed.value, fileid],
        )

    # Marks the file trasfer as completed
    def markfileastransfercompleted(self, srcpath):
        self._execute(
            "UPDATE files SET transfer_status=? WHERE path = ?",
            [TransferStatus.completed.value, srcpath],
        )

    # Marks the file as hpss_checksum_complete
    def markfileashpsschecksumcomplete(self, srcpath):
        self._execute(
            "UPDATE files SET checksum_status=? WHERE path = ?",
            [ChecksumStatus.hpss_complete.value, srcpath],
        )

    # Inserts a VV into the vvs table
    def insertvv(self, vv):
        istape = False
        if "X" in vv or "H" in vv:
            istape = True
        self._execute(
            "INSERT INTO vvs (name, is_tape) VALUES (?, ?)", [vv, int(istape)]
        )

    # Retrieve a vv entry by name
    def getvv(self, vv):
        rows = self._get_rows("SELECT id, name FROM vvs WHERE name LIKE ?", [vv])
        return rows

    # Get a destination driectory entry by path
    def getdest(self, path):
        dirname = os.path.dirname(path)
        rows = self._get_rows(
            "SELECT id, path FROM desttree WHERE path LIKE ?", [dirname]
        )
        return rows[0]

    # Get the total number of files in the files table in the db
    def gettotalnumberoffiles(self):
        row = self._get_rows("SELECT count(*) FROM files", maxrows=1)
        return row[0]

    # Get file entries where the source and dest checksums do not match
    def getnonmatchingchecksums(self):
        rows = self._get_rows(
            "SELECT path,src_checksum,dest,dest_checksum FROM files WHERE dest_checksum <> src_checksum"
        )
        return rows

    # Get files marked as 'destination file exists'
    def getexistingfiles(self):
        rows = self._get_rows("SELECT path,dest,id FROM files WHERE destfileexists=1")
        return rows

    # get a file by source path
    def getfile(self, file):
        rows = self._get_rows("SELECT id, path FROM files WHERE path LIKE ?", [file])
        return rows

    # Get a list of files by VV (DB) id
    def getfilesbyvv(self, vvid):
        rows = self._get_rows(
            "SELECT path,dest,id FROM files WHERE vv = ? AND transfer_status <> ?",
            [vvid, TransferStatus.completed.value],
        )
        return rows

    # Dump the files table
    def dumpfiles(self):
        rows = self._get_rows("SELECT * FROM files")
        return rows

    # Get list of vvs that have non-complete file transfers
    def getvvswithnoncompletefiles(self):
        rows = self._get_rows(
            "SELECT DISTINCT vv FROM files where transfer_status <> ?",
            [TransferStatus.completed.value],
        )
        return rows

    # dump the desttree table
    def dumpdesttree(self):
        rows = self._get_rows("SELECT * FROM desttree")
        return rows

    # vvExists will return None if theres no entries, and the rowid if there is
    def vvExists(self, vv):
        rows = self.getvv(vv)
        for row in rows:
            if row[1] == vv:
                return row[0]
        return None

    # fileExists will return None if theres no entries, and the rowid if there is
    def fileExists(self, file):
        rows = self.getfile(file)
        for row in rows:
            if row[1] == file:
                return row[0]
        return None

    # Mark a file as 'destination file exists'
    def markexists(self, fileid):
        self._execute(
            "UPDATE files SET destfileexists=1,transfer_status=? WHERE id=?",
            [TransferStatus.skipped.value, fileid],
        )


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
            self.db = DBCache(
                self.preserve_db, self.verbose, self.output_path, path=args.db_path
            )
        else:
            self.db = DBCache(self.preserve_db, self.verbose, self.output_path)

        # Define the HSIJob that lists all the files recursively in the source directory
        self.ls_job = HSIJob(
            self.hsi,
            self.addl_flags,
            True,
            self.verbose,
            "ls -R -P {}".format(self.source),
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
                os.mkdir(path[1])
            except FileExistsError:
                logging.debug("Skipping mkdir of %s; directory already exists", path[1])
                self.db.markdestcreated(path[0])
            except:  # pylint: disable=bare-except
                logging.critical(
                    "ERROR: Could not create destination directory. Do you have the correct permissions?"
                )
                sys.exit(3)
            else:
                self.db.markdestcreated(path[0])

    # This is the main migration thread! All the fun stuff happens here
    # - Generates the file lists
    # - Calls HSI to migrate and hash files
    # - Populate the db with the resulting information
    # - Profit
    def startMigrate(self):
        # Generator to created a chunked list of vv's
        def chunk(lst, chunksize):
            if chunksize < 0 and chunksize != -1:
                logging.critical(
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
            logging.debug("Found existing files:")
            logging.debug(json.dumps(existingFiles, indent=2))

        logging.info(
            "Starting migration of files from %s to %s", self.source, self.destination
        )

        # Set some variables for status output (esp if not using the progress bar)
        filelistnum = 0
        filesCompleted = len(existingFiles)
        filesTotal = self.db.gettotalnumberoffiles()

        # If we don't set --additional-hsi-flags, assume we're using keytabs for auth. If so, then we can use the progress bar
        # Otherwise, the progress bar will overwrite the PASSCODE: prompt so it'll just appear to 'hang' for users

        if self.addl_flags is None:
            vvs = alive_it(vvs, monitor=False, stats=False)

        # Launch an HSI job for each chunk of vvs (generally 1 vv/chunk but see below)
        for vvlist in vvs:
            files = []
            # Unpack chunk. Will generally just be a list of 1 vvid, but in case --vvs-per-job is set, this will chunk it up
            for vv in vvlist:
                files.extend(self.db.getfilesbyvv(vv[0]))

            files = [f for f in files if f not in existingFiles]

            if len(files) > 0:

                filelistnum += 1
                infilename = os.path.join(
                    self.output_path, f"hpss_transfer_sorted_files.{filelistnum}.list"
                )
                # Write the infile to be passed into HSI
                with open(infilename, "w", encoding="UTF-8") as infile:
                    getcmd = "get -T on << EOF\n"
                    # hashcreate can not use the TA, so we force -T off
                    hashcreatecmd = "hashcreate -T off << EOF\n"

                    if self.disable_ta:
                        getcmd = "get -T off << EOF\n"

                    infile.write(getcmd)
                    infile.write(
                        "\n".join(["{} : {}".format(f[1], f[0]) for f in files])
                    )
                    infile.write("\nEOF\n")
                    # Add a hashcreate command to generate and get hashes in the same job.
                    # We're not doing this in parallel here, since it'll create a lot of PASSCODE prompts if a user isn't using keytabs
                    # And since they both (get and hashcreate) need to stage the data, we're not introducing too much extra runtime here
                    if not self.disable_checksums:
                        infile.write(hashcreatecmd)
                        infile.write("\n".join(["{}".format(f[0]) for f in files]))
                        infile.write("\nEOF\n")

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
                        self.db.markfileasstaging(f[2])
                    # Run the job!
                    output, rc = migration_job.run()
                    if rc != 0:
                        logging.info("HSI job failed: returned %d", rc)
                    # Scan through the output looking for errors and md5 sums. Populate the db accordingly
                    for line in output:
                        if "(md5)" in line:
                            checksum, srcpath = line.split(" ")[0], line.split(" ")[-1]
                            self.db.setsrcchecksum(srcpath, checksum)
                            self.db.markfileashpsschecksumcomplete(srcpath)
                            logging.debug("Completed transfer for file %s", srcpath)
                            self.db.markfileastransfercompleted(srcpath)
                        if "get: Error" in line:
                            logging.info(
                                "File %s failed to transfer from HPSS",
                                line.split(" ")[-1],
                            )
                            self.db.markasfailed(line.split(" ")[-1])
                    # Update our stats
                    filesCompleted += len(files)
                    if self.addl_flags is not None:
                        logging.debug(
                            "%s/%s file transfers have been attempted",
                            filesCompleted,
                            filesTotal,
                        )
                else:
                    logging.info("Would have run: %s", migration_job.getcommand())

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
                logging.debug(
                    "Checksum did not match! Can not guarantee file integrity!"
                )
                logging.debug(" - %s(%s) : %s(%s)", file[1], file[2], file[3], file[4])
                ret["checksum_mismatch"].append(
                    {
                        "source": (file[1], file[2]),
                        "destination": (file[3], file[4]),
                    }
                )
        else:
            logging.debug("All files checksummed :)")
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

    # This gets the list of files from HPSS to be inserted into the DB
    def getSrcFiles(self):
        output, rc = self.ls_job.run()
        if rc != 0:
            logging.critical(
                "Unable to get file data from HPSS! Please ensure that you can log into HPSS with 'hsi' using keytabs or token authentication, and that you have appropriate read permissions in %s!",
                self.source,
            )
            sys.exit(5)

        files = []
        dirs = []
        for i in output:
            i = i.split("\t")
            if i[0] == "FILE":
                files.append(i)
            elif i[0] == "DIRECTORY":
                dirs.append(i)

        # Removing junk from array
        dirs = [d[1].replace(self.source, self.destination) for d in dirs]
        # Adding the root destination since files can be at depth 0
        dirs.append(self.destination)
        self.db.insertdesttree(dirs)
        self.db.insertfiles(files, self.source, self.destination)


# Due to the fact that multiprocessing is weird, and pickles things that get passed to the pool (including the function itself),
# and the fact that the MigrationJob object contains a DBCache object that contains non-serializable data (thread locks), we go
# ahead and play it safe and pull this function outside the object. Yay.
# This function performs a single md5sum on a single file in the destination. Thread pool worker
def doDestChecksum(file):
    checksum = ""

    cmd = "md5sum {}".format(file[1]).split(" ")

    output = []
    p = subprocess.run(
        cmd,
        stdout=PIPE,
        stderr=subprocess.STDOUT,
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
    def run(self):
        cmd = self.hsi.split(" ")
        if self.flags is not None:
            cmd.extend(self.flags.split(" "))
        cmd.extend(self.cmd.split(" "))

        output = []
        p = Popen(
            cmd,
            stdout=PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            universal_newlines=True,
        )
        for line in p.stdout:
            if "PASSCODE" in line:
                print(line)

            output.append(line.strip())
        p.wait()

        if p.returncode != 0:
            logging.debug("\n")
            for i in output:
                logging.debug(i)
            logging.debug("\n")

        return output, p.returncode

    # Returns the full HSI command that will be run as a string.
    def getcommand(self):
        cmd = self.hsi.split(" ")
        if self.flags is not None:
            cmd.extend(self.flags.split(" "))
        cmd.extend(self.cmd.split(" "))
        return " ".join(cmd)


def initLogger(verbose):
    if verbose:
        # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        logging.basicConfig(
            level=logging.DEBUG, format="", datefmt="%m/%d/%Y %I:%M:%S %p"
        )
    else:
        # logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        logging.basicConfig(
            level=logging.INFO, format="", datefmt="%m/%d/%Y %I:%M:%S %p"
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
        "-T", "--disable-ta", default=False, help=argparse.SUPPRESS, action="store_true"
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        help="Output additional information about the transfer",
        action="store_true",
    )
    parser._positionals.title = "NOTE: While this script will work with PASSCODE auth, it is HIGHLY recommended to use standard 'keytab' auth (e.g. do not use --additional-hsi-flags unless you absolutely must!\n\npositional arguments" # pylint: disable=W0212
    # Get our cli args
    args = parser.parse_args()
    initLogger(args.verbose)
    # Create our migration job
    job = MigrateJob(args)

    if args.db_path is not None:
        logging.info("Resuming interrupted transfer.")

    # Populate the db with HPSS data
    if args.db_path is None:
        logging.debug("Getting file data from HPSS...")
        job.getSrcFiles()

    # Check for files that exist in the destination
    logging.debug("Checking for existing files...")
    job.checkForExisting()

    # Create the destination directory structure
    if not args.dry_run:
        logging.debug("Creating destination directory structure")
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

    logging.debug(json.dumps(finalreport, indent=2))

    # Write the report to a file
    if not args.dry_run:
        reportfilename = "hpss_transfer_sorted_files_report_{}.json".format(
            int(time.time())
        )
        with open(reportfilename, "w", encoding="UTF-8") as f:
            json.dump(finalreport, f, indent=2)
        logging.info("Transfer complete. Report has been written to %s", reportfilename)


if __name__ == "__main__":
    sys.exit(main())
