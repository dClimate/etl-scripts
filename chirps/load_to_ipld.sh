#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

zarr_path="$PWD/${dataset}/${dataset}.zarr"
echo Loading "$zarr_path" to IPLD
python ../shared_python_scripts/zarr_to_ipld.py "$zarr_path"
