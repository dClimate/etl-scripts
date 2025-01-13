#!/bin/bash
# This script uses restic to backup ~/.ipfs to an S3 compatible cloud storage service

pgrep ipfs
if (( $? == 0 )); then
    echo Error: ipfs daemon looks to be running! >&2
    echo The daemon may be in the process of changing files, restic may backup an invalid state >&2
    echo Exiting now, please shutdown the ipfs daemon before trying to run the backup again >&2
    exit 1
fi

set -e # Exit if any commands return an errored exit status

# The -a flag tells bash to export all variables defined. +a then unsets this
set -a
source .env
set +a

echo Backing up kubo directory
restic backup /srv/ipfs/.ipfs

# A snapshot is compared against all these possiblities, ORed per se, to see if it should be kept
# keep-last is just the 4 most recent snapshots
# keep-weekly means if a week has multiple snapshots, keep only the most recent ones, for the 4 most recent weeks
# keep-monthly looks at the last 6 months and only keeps the most recent snapshots
echo Forgetting stale snapshots
restic forget --keep-last 4 --keep-weekly 4 --keep-monthly 6

echo Removing unused data
restic prune
