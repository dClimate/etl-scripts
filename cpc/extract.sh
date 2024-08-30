#!/bin/bash
set -e

source shared_functions.sh

check_there_is_one_argument $#
check_argument_is_valid $1

dataset=$1
echo Processing all .nc files for $dataset
# The weird ls is so that we get absolute paths to send to the netcdf extractor
ls -d "$PWD/$dataset"/*.nc | xargs -n 1 python ../shared_python/extract_netcdf.py
