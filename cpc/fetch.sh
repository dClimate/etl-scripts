#!/bin/bash
set -e

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

cd $dataset
if [[ ! -e download-links.txt ]]; then
    echo "File $dataset/download-links.txt does not exist!"
    echo "Please run prefetch.sh for $dataset"
    exit 1
fi
# Using xargs to run as many concurrent downloads as possible
# --max-procs=0 flag tells it to run as many processes as possible
# wget's flag --timestamping makes it check to see if files from the incoming server are marked as newer or not than the ones we've already downloaded
# This makes fetching idempotent, as long as the download files all remain inside the same folder and as the same names
echo "Fetching all .nc files for: $dataset"
cat download-links.txt | xargs --max-procs=0 -n 1 wget --timestamping
