#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_python_virtualenv_activated

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

python ../shared_python_scripts/transform_prism.py $dataset
