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
    echo $year:tarring and gzipping
    tar -zcf "$year".tar.gz "$year"-??-??_?.nc
done

echo Unpinning previous bootstrap .tar.gz files
ipfs-cluster-ctl pin ls | grep -E "bootstrap-prism-${dataset}-[1-2][0-9]{3}.tar.gz" | awk '{print $1}' | xargs -n 1 ipfs-cluster-ctl pin rm

echo Adding the new .tar.gz files
for year in {1981..2024}; do
    echo Adding $year.tar.gz

    pin_name="bootstrap-prism-${dataset}-${year}.tar.gz"
    ipfs-cluster-ctl add --name pin_name $year.tar.gz
    echo Deleting $year.tar.gz
    rm $year.tar.gz
done

echo Creating list of all hashes and filenames
txt_filename="bootstrap-prism-${dataset}.txt"
ipfs-cluster-ctl pin ls | grep -E "bootstrap-prism-${dataset}-[1-2][0-9]{3}.tar.gz" | awk '{print $3 " " $1}' | sort > $txt_filename

echo Unpinning previous list
previous_cid=$(ipfs-cluster-ctl pin ls | grep $txt_filename | awk '{print $1}')
ipfs-cluster-ctl pin rm $previous_cid

echo Pinning the list to the cluster
cid=$(ipfs-cluster-ctl add --name $txt_filename $txt_filename | awk '{print $2}')
echo Pinned CID $cid

echo Publishing the list to IPNS
ipns_key_name="bootstrap-prism-${dataset}"
ipfs name publish --key="$ipns_key_name" "/ipfs/$cid"

echo Removing list of all hashes and filenames
rm $txt_filename
