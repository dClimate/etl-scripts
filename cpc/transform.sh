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

cpc_log "Starting transform"
python_output=$(python ../shared_python_scripts/transform_nc.py "$PWD/$dataset/" 2>&1)
cpc_log "transform_nc.py output: $python_output"
cpc_log "Ending transform"
