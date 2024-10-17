#!/bin/bash
set -e

# Change to this script's directory
cd $(dirname "$0")

source shared-functions.sh
check_there_are_no_arguments $#


cd vh
if [[ ! -e download-links.txt ]]; then
    echo download-links.txt does not exist for VHI!
    echo Please run prefetch.sh for VHI first
    echo Exiting with error
    exit 1
fi
cat download-links.txt | xargs --max-procs=0 -n 1 wget --timestamping
