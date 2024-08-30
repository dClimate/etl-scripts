#!/bin/bash
set -e

source shared_functions.sh

check_there_is_one_argument $#
check_argument_is_valid $1

dataset=$1
dataset_abs_path="$PWD/$dataset"
# The echo subshell just lists the absolute paths to the kerchunk JSONs
python ../shared_python/combine.py $(echo "$PWD/$dataset"/*.nc.json) $dataset_abs_path
