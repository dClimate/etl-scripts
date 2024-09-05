#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

source ../.venv/bin/activate

bash prefetch.sh $dataset
bash fetch.sh $dataset
bash transform.sh $dataset
bash load_to_ipld.sh $dataset
bash update_ipns.sh $dataset
