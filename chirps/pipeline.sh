#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

cd ..
source shared-functions.sh
cd chirps

error_usage_exit() {
    er Error: "$*"
    er Usage: bash "$0" "dataset:final-p05|final-p25|prelim-p05" "step:all|prefetch|fetch|transform|load_to_ipfs|send_to_cluster|update_ipns"
    er Example: bash "$0" final-p25 all
    exit 1
}

chirps_log() {
    message="$*"
    echo_and_webhook "[CHIRPS $dataset]" $message
}

notify_of_error() {
    exit_code=$?
    if (( $exit_code != 0 )); then
        chirps_log Pipeline failed on step $current_step
    fi
}
trap notify_of_error ERR

# Check that there are only two arguments
num_arguments=$#
if (( num_arguments != 2 )); then
    error_usage_exit Invalid number of arguments, expected 2 and received $num_arguments
fi

# Check valid dataset argument
dataset=$1
case $dataset in
    final-p05|final-p25|prelim-p05)
        ;;
    *)
        error_usage_exit Unknown dataset $dataset
        ;;
esac

# Check valid step argument
step=$2
case $step in
    all|prefetch|fetch|transform|load_to_ipfs|send_to_cluster|update_ipns)
        ;;
    *)
        error_usage_exit Unknown step $step
    ;;
esac

# Note that all etl steps just access the $dataset variable without it needing to be passed in

current_step="Pipeline not started yet"

prefetch() {
    current_step="Prefetch: create download URLs"
    chirps_log $current_step

    local dataset_type
    local precision
    case $dataset in
        final-p25)
            dataset_type="final"
            precision="p25"
            ;;
        final-p05)
            dataset_type="final"
            precision="p05"
            ;;
        prelim-p05)
            dataset_type="prelim"
            precision="p05"
            ;;
    esac
    local base_url='https://data.chc.ucsb.edu/products/CHIRPS-2.0'
    local url
    if [ "$dataset_type" = "prelim" ]; then
        url="${base_url}/prelim/global_daily/netcdf/${precision}"
    else
        url="${base_url}/global_daily/netcdf/${precision}"
    fi

    cd "$dataset"
    curl "${url}/" |
        grep 'href="' |
        sed -n 's/.*href="\([^"]*\.nc\)".*/\1/p' |
        sed "s|^|${url}/|" > ./download-links.txt
    cd ..
}

fetch() {
    current_step="Fetch: download .nc files"
    cd $dataset
    if [[ ! -e download-links.txt ]]; then
        chirps_log Fetch step has no download links for $dataset, please run prefetch step first
        exit 1
    fi
    chirps_log $current_step
    cat download-links.txt | xargs --max-procs=0 -n 1 wget --timestamping
    cd ..
}

transform() {
    current_step="Transform: create Zarr from .nc files"
    chirps_log $current_step
    python ../shared_python_scripts/transform_nc.py "$PWD/$dataset/"
}

load_to_ipfs() {
    current_step="Load to IPFS: Convert Zarr to HAMT on IPFS"
    chirps_log $current_step
    time python ../shared_python_scripts/zarr_to_ipfs.py "$PWD/${dataset}/${dataset}.zarr"
}

send_to_cluster() {
    cluster=halogen
    current_step="Sending and pinning latest HAMT root CID to cluster $cluster"
    chirps_log $current_step

    cid_path="$PWD/${dataset}/${dataset}.cid"
    # Conditional checks if file does not exist
    if [[ ! -f "$cid_path" ]]; then
        chirps_log CID not found at path "$cid_path"
        exit 1
    fi
    cid=$(cat $cid_path)
    # First, pin it directly with ipfs
    # For some reason, ipfs-cluster-ctl pin adds encounter unknown errors with larger datasets, but kubo handles it fine
    # Then doing the ipfs-cluster-ctl pin add allows us to associate it with a name and keep better track
    time ssh $cluster "/srv/ipfs/bin/ipfs pin add $cid"
    # ssh $cluster "/srv/ipfs/bin/ipfs-cluster-ctl pin add --name $dataset.zarr --wait $cid"
}

update_ipns() {
    current_step="Updating IPNS record to point to latest root CID"
    cid_path="$PWD/${dataset}/${dataset}.cid"
    # Conditional checks if file does not exist
    if [[ ! -f "$cid_path" ]]; then
        chirps_log CID not found at path "$cid_path", quitting publishing CID to IPNS
    fi
    cid=$(cat $cid_path)

    ipns_key_name=$(cat "$dataset/ipns-key-name.txt")

    echo Publishing CID "$cid" with ipns key "$ipns_key_name"
    ipfs name publish --key="$ipns_key_name" "/ipfs/$cid"
}

case $step in
    all)
        prefetch
        fetch
        source ../.venv/bin/activate
        transform
        load_to_ipfs
        send_to_cluster
        # update_ipns
        ;;
    prefetch) prefetch;;
    fetch) fetch;;
    transform)
        source ../.venv/bin/activate
        transform
        ;;
    load_to_ipfs)
        source ../.venv/bin/activate
        load_to_ipfs
        ;;
    send_to_cluster) send_to_cluster;;
    update_ipns) update_ipns;;
esac
