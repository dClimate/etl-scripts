#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

echo Processing all .nc files for $dataset
# This sends, one by one, absolute paths to the netCDF files to the extractor script
echo "$PWD/$dataset"/*.nc | xargs -n 1 python ../shared_python_scripts/extract_netcdf.py
