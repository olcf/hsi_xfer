# hsi_xfer

## Overview:
This tool (`hsi_xfer`) was developed to provide a smooth and user-friendly experience to users for moving bulk data out of our aging HPSS system. Without having a Globus endpoint available for use, the only tool available to users was the generic `hsi` tool, which does not provide the same level of experience, or feature parity with Globus. `hsi_xfer` attempts to bridge this gap by providing the following additional features:

* Checksumming of files after transfer to ensure data integrity at the destination
* Checkpointing to allow interrupted transfers to continue roughly where they left off
* HSI threads are launched in parallel to allow concurrent transfer of file lists
* JSON formatted reporting of file transfers
* Caching of large lists of files to prevent reindexing upon reinvoking the tool

## How it works:

* A user provides a source path on HPSS, and a destination path elsewhere
* `hsi_xfer` will recursively index every file underneath the `source` path and cache this list.
    * This part is skipped if the user provides a path to an existing cache file from a previous run, if the previous run completed checksumming
* The list of indexed files are broken up into multiple file lists to be retrieved in parallel. The tool, by default, will put all files that exist on a single HPSS VV in the same file list. This helps to prevent damage to the tape, as well as optimizes the retrieval time for all these files by minimizing seeks and tape library thrashing
    * If a file exists (both file name and size match on the destination and HPSS), or the file is already marked as "successfully transferred" in the cache, the file will be skipped, saving time and bandwith
* A thread pool of `hsi` processes is created, and the file lists are passed into the pool for concurrent checksumming and transfer.
    * If a checksum for a file does not exist in HPSS, one is created at this time.
    * The checksum is retreived and saved into the cache
    * The file is then transferred to the destination.
    * As `hsi` processes complete, new threads are spawned and provided with the next VV's file list
* Once the transfer is complete, another thread pool is spawned to do concurrent checksumming on the destination filesystem. This checksum is also saved in the cache.
* Finally, `hsi_xfer` will compare the HPSS checksum with the destination checksum and generate a JSON formatted report that is output in a file in the user's current working directory. If the file was successfully transferred, but checksums did not match, the file is marked as a "failed transfer" in the report

## Caveats:

* While technically, this script will work with HPSS accounts that use 2 factor authentication (like RSA tokens), it is NOT recommended. Please use keytab authentication!
* Either `$HSI_PATH` or `--hsi-path` must be set to a valid path to an `hsi` executable.
* If you encounter issues with database transactions, it is recommended to either change your current working directory to a non-networked filesystem (like `/tmp`), or use the `-f` flag to point to a non-networked filesystem.
    * This is due to the fact that the caching mechanism is built upon a SQLite database, and SQLite does not like networked filesystems, like NFS.
* This script is designed to be a bulk transfer tool, so requesting a `source` path of a single file will fail. Please set your `source` path to a valid directory within HPSS
* At this time, moving files *into* HPSS isn't supported
* If you are wanting to transfer a directory tree that is not all under the same path (e.g. you want to pull `/hpss/path/a/b` and `/hpss/path/a/c` but not `/hpss/path/a/b`), you can create a set of HSI `in files` to directly pass to `hsi_xfer` with the `--skip-filelist-build --list-input-override=/path/to/list/directory` flags
    * NOTE: it is absolutely *NOT* recommended to use this feature since this will disable all caching, checksumming, and checkpointing features! This is only provided as an override mechanism for indexing files and creating file lists
    * The file names of the `in files` must be of the format `*.list`.
    * Once complete, a filelist will be moved to `*.list.done`, if any file in the `in file` list fails to transfer, the `in file` list file will be moved to `*.list.error`
* By default, a thread pool of only 2 `hsi` parallel processes are created. This value can be changed by using the `--parallel-migration-count` flag (e.g. `--parallel-migration-count=10` to launch 10 parallel `hsi` processes)
* To prevent abuse of the HPSS system, a lock file gets created in the running user's home directory. Only 1 `hsi_xfer` process can be run at any given time by any single user.

## Prerequisites:

* An HPSS filesystem
* `hsi`
* python 3.12
* conda

## To build:

To build a copy of the python environment, run the following:
```
$ ./build.sh /path_to_dev_env
```

## To run:

To run the script, simply call the `hsi_xfer` tool:
```
$ /path/to/conda/env/bin/hsi_xfer/hsi_xfer --hsi-path=/path/to/hsi/binary
```

## Usage:
```
usage: hsi_xfer [-h] [-H HSI_PATH] [-D] [-C CACHE_PATH] [-l] [-P] [-p] [-c] [-a ADDITIONAL_HSI_FLAGS] [-e] [-f PRESERVED_FILES_PATH] [-E] [-i UPDATE_INTERVAL] [-S] [--checksum-threads CHECKSUM_THREADS] [--debug] [-T] [-s DB_TX_SIZE] [-r] [-V VVS_PER_JOB] [-L LIST_INPUT_OVERRIDE] [-m] [-t PARALLEL_MIGRATION_COUNT] [-v] source destination

NOTE: While this script will work with PASSCODE auth, it is HIGHLY recommended to use standard 'keytab' auth (e.g. do not use --additional-hsi-flags unless you absolutely must!)

positional arguments:
  source                Top level directory in HPSS to pull files from. This is a recursive action!
  destination           Top level directory on Kronos to put files. An identical directory structure will be created here as exists in HPSS under `source`

options:
  -h, --help            show this help message and exit
  -H HSI_PATH, --hsi-path HSI_PATH
                        Path the the HSI executable. This argument can be ignored if $HSI_PATH is set appropriately in your shell
  -D, --dry-run         Generate the file lists but do not actually checksum or transfer files, and output the `hsi` commands that would have been used
  -C CACHE_PATH, --cache-path CACHE_PATH
                        Path to existing cache generated by --preserve-cache, or from an interrupted job. Picks up where the last job left off
  -l, --preserve-file-lists
                        Do not delete the file list generated by this script. Will write the list to the cwd or the value of --preserved-files-path
  -P, --preserve-timestamps
                        Overwrites timestamps on transferred files to match timestamps of file within HPSS
  -p, --preserve-cache  Do not delete the cache. This can be useful for restarting an interrupted migration
  -c, --disable-checksums
                        Disable the checksumming operation. This can speed things up, but at the cost of not verifying data integrity on the destination
  -a ADDITIONAL_HSI_FLAGS, --additional-hsi-flags ADDITIONAL_HSI_FLAGS
                        Additional flags needed to be passed to `hsi`
  -e, --overwrite-existing
                        Force overwrite of existing files that match the file names in HPSS
  -f PRESERVED_FILES_PATH, --preserved-files-path PRESERVED_FILES_PATH
                        Sets the path that the transfer report and/or database/file list are written to. This can be useful if the DB and resulting files lists/transfer report are too large for your current working directory
  -E, --enable-log-timestamps
                        Turn on log timestamps for status output
  -i UPDATE_INTERVAL, --update-interval UPDATE_INTERVAL
                        Sets the interval between status updates during file transfers
  -S, --skip-filelist-build
                        When --dry-run is set, skip building of the filelists
  --checksum-threads CHECKSUM_THREADS
                        Number of threads to use for destination checksumming
  --debug               Enable debug output
  -T, --disable-ta      Disables the HPSS transfer agent
  -s DB_TX_SIZE, --db-tx-size DB_TX_SIZE
                        Sets the maximum number of rows for any individual database transaction
  -r, --trace           Enable trace output
  -V VVS_PER_JOB, --vvs-per-job VVS_PER_JOB
                        Sets the number of HPSS VVs that each HSI thread processes at any one time
  -L LIST_INPUT_OVERRIDE, --list-input-override LIST_INPUT_OVERRIDE
                        Processes an exisitng directory of HSI in-files. NOTE: This disables all caching, checksumming, and checkpointing features!
  -m, --move-filelists  When -L is used, this flag is used to determine whether to move processed in-file lists to either *.error or *.done
  -t PARALLEL_MIGRATION_COUNT, --parallel-migration-count PARALLEL_MIGRATION_COUNT
                        Sets the number of parallel HSI threads in the pool to dispatch filelists to
  -v, --verbose         Output additional information about the transfer

```
