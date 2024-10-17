#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared-functions.sh

check_there_is_one_argument $#

dataset=$1
check_valid_dataset $dataset

create_cpc_log $dataset

source ../.venv/bin/activate

cpc_log "Starting load to IPLD"
zarr_path="$PWD/${dataset}/${dataset}.zarr"
python_output=$(python ../shared_python_scripts/zarr_to_ipld.py "$zarr_path" 2>&1)
cpc_log "zarr_to_ipld.py output: $python_output"
cpc_log "Ending load to IPLD"
