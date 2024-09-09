#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

for year in {1981..2024}; do
    echo Downloading files for year $year
    pin_name="bootstrap-prism-${dataset}-${year}.tar.gz"
    cid=$(ipfs-cluster-ctl pin ls | grep $pin_name | awk '{print $1}')

    ipfs get $cid
    mv $cid $pin_name
    tar -zxf $pin_name
    rm $pin_name
done
