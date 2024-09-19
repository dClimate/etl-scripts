#!/bin/bash
set -e

source shared_functions.sh

cd_to_script_dir "$0"

check_python_virtualenv_activated

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

python ./combine.py $dataset
