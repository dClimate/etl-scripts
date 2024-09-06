#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

cd $dataset
echo Creating bootstrap for $dataset
echo Creating new .tar.gz files for each year
for year in {1981..2024}; do
    pin_name="bootstrap-prism-${dataset}-${year}.tar.gz"
    echo $year:tarring and gzipping
    tar -zcf "$year".tar.gz "$year"-??-??_?.nc
    echo $year: unpinning previous $year.tar.gz if it exists
    ipfs-cluster-ctl pin ls | grep $pin_name | awk '{print $1}' | xargs -n 1 ipfs-cluster-ctl pin rm
    echo $year: adding new file
    ipfs-cluster-ctl add --name pin_name $year.tar.gz
    echo $year: removing $year.tar.gz
    rm $year.tar.gz
done
