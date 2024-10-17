#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared-functions.sh

check_there_is_one_argument $#

dataset=$1
check_valid_dataset $dataset
create_cpc_log $dataset

cleanup() {
    cpc_log "Encountered error on the step $STEP"
    cpc_log "Stopping pipeline and exiting immediately"
}
trap cleanup ERR

cpc_log "🚰 Starting pipeline"

STEP=prefetch
bash prefetch.sh $dataset
STEP=fetch
bash fetch.sh $dataset
STEP=transform
bash transform.sh $dataset
STEP="load to ipld"
bash load-to-ipld.sh $dataset
STEP="update ipns"
bash update-ipns.sh $dataset

cpc_log "🚰 Finished pipeline"
