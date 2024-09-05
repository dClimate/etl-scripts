#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

cd $dataset
echo "Downloading all datasets from: $dir_name"
cat download-links.txt | xargs --max-procs=0 -n 1 wget --timestamping
