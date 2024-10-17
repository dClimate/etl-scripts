#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared-functions.sh

check_there_is_one_argument $#

dataset=$1
check_valid_dataset $dataset

create_cpc_log $dataset

cpc_log "Starting IPNS Update"

cid_path="$PWD/${dataset}/${dataset}.cid"
if [[ ! -f "$cid_path" ]]; then
    cpc_log "CID not found at $cid_path, quitting publishing CID to IPNS"
fi
cid=$(cat $cid_path)

ipns_key_name_path="$dataset/ipns-key-name.txt"
ipns_key_name=$(cat "$ipns_key_name_path")

cpc_log "Publishing CID $cid with ipns key $ipns_key_name"

publish_output=$(ipfs name publish --key="$ipns_key_name" "/ipfs/$cid" 2>&1)
cpc_log "ipfs name publish output: $publish_output"

cpc_log "Finished IPNS Update"
