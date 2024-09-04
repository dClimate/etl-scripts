#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

cid_path="$PWD/${dataset}/${dataset}.cid"
# Conditional checks if file does not exist
if [[ ! -f "$cid_path" ]]; then
    echo CID not found at "$cid_path", quitting publishing CID to IPNS
fi
cid=$(cat $cid_path)

ipns_key_name_path="$dataset/ipns-key-name.txt"
ipns_key_name=$(cat "$ipns_key_name_path")

echo Publishing CID "$cid" with ipns key "$ipns_key_name"
ipfs name publish --key="$ipns_key_name" "/ipfs/$cid"
