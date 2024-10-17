#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared-functions.sh

check_there_is_one_argument $#

dataset=$1
check_valid_dataset $dataset

create_cpc_log $dataset

cd $dataset
if [[ ! -e download-links.txt ]]; then
    cpc_log $dataset "Error: download-links.txt does not exist! Please run prefetch step"
    exit 1
fi
cpc_log "Starting fetch of all .nc files"
cat download-links.txt | xargs --max-procs=0 -n 1 wget --timestamping
cd ..
cpc_log "Finished fetch"
