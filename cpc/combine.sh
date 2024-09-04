#!/bin/bash
set -e

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

python ../shared_python/combine.py cpc $dataset
