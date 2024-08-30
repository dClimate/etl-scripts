#!/bin/bash
set -e

source shared_functions.sh

check_there_is_one_argument $#
check_argument_is_valid $1

dataset=$1
echo Processing all .nc files for $dataset
# This sends, one by one, absolute paths to the netCDF files to the extractor script
echo "$PWD/$dataset"/*.nc | xargs -n 1 python ../shared_python/extract_netcdf.py
