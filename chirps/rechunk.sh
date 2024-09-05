#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

# Iterate through all .nc files in the current directory
for nc_file in $dataset/*.nc; do
    # Check that the nc file is not already rechunked
    if [[ "$nc_file" != *-rechunked.nc ]]; then
        python ../shared_python_scripts/rechunk_nc.py "$PWD/$nc_file"
    fi
done
