#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

cd ..
source shared-functions.sh
cd cpc

error_usage_exit() {
    er Error: "$*"
    er Usage: bash "$0" "dataset:precip-conus|precip-global|tmin|tmax" "step:all|prefetch|fetch|transform|load_to_ipfs|send_to_cluster|update_ipns"
    er Example: bash "$0" precip-conus all
    exit 1
}

cpc_log() {
    message="$*"
    echo_and_webhook "[CPC $dataset]" $message
}

notify_of_error() {
    exit_code=$?
    if (( $exit_code != 0 )); then
        cpc_log Pipeline failed on step $current_step
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
    precip-conus|precip-global|tmax|tmin)
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
    base_url=""
    start_year=""
    end_year=$(date +"%Y") # Set to the current year
    case $dataset in
        precip-conus)
            base_url='https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_us_precip/RT/precip.V1.0.'
            start_year=2007
            ;;
        precip-global)
            base_url='https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_precip/precip.'
            start_year=1979
            ;;
        tmax)
            base_url='https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmax.'
            start_year=1979
            ;;
        tmin)
            base_url='https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmin.'
            start_year=1979
            ;;
    esac

    cpc_log $current_step
    cd $dataset
    year=$start_year
    > ./download-links.txt # Clear the file first
    while (( year <= end_year )); do
        url="${base_url}${year}.nc"
        echo $url >> ./download-links.txt
        year=$((year + 1))
    done
    cd ..
}

fetch() {
    current_step="Fetch: download .nc files"
    cd $dataset
    if [[ ! -e download-links.txt ]]; then
        cpc_log Fetch step has no download links for $dataset, please run prefetch step first
        exit 1
    fi
    # Using xargs to run as many concurrent downloads as possible
    # --max-procs=0 flag tells it to run as many processes as possible
    # wget's flag --timestamping makes it check to see if files from the incoming server are marked as newer or not than the ones we've already downloaded
    # This makes fetching idempotent, as long as the download files all remain inside the same folder and as the same names
    cpc_log $current_step
    cat download-links.txt | xargs --max-procs=0 -n 1 wget --timestamping
    cd ..
}

transform() {
    current_step="Transform: create Zarr from .nc files"
    cpc_log $current_step
    python ./transform.py "$PWD/$dataset/"
}

load_to_ipfs() {
    current_step="Load to IPFS: Convert Zarr to HAMT on IPFS"
    cpc_log $current_step
    time python ../shared_python_scripts/zarr_to_ipfs.py "$PWD/${dataset}/${dataset}.zarr"
}

send_to_cluster() {
    cluster=halogen
    current_step="Sending and pinning latest HAMT root CID to cluster $cluster"
    cpc_log $current_step

    cid_path="$PWD/${dataset}/${dataset}.cid"
    # Conditional checks if file does not exist
    if [[ ! -f "$cid_path" ]]; then
        cpc_log CID not found at path "$cid_path"
        exit 1
    fi
    cid=$(cat $cid_path)
    # First, pin it directly with ipfs
    # For some reason, ipfs-cluster-ctl pin adds encounter unknown errors with larger datasets, but kubo handles it fine
    # Then doing the ipfs-cluster-ctl pin add allows us to associate it with a name and keep better track
    time ssh $cluster "/srv/ipfs/bin/ipfs pin add $cid"
    ssh $cluster "/srv/ipfs/bin/ipfs-cluster-ctl pin add --name $dataset.zarr --wait $cid"
}

update_ipns() {
    current_step="Updating IPNS record to point to latest root CID"
    cid_path="$PWD/${dataset}/${dataset}.cid"
    # Conditional checks if file does not exist
    if [[ ! -f "$cid_path" ]]; then
        cpc_log CID not found at path "$cid_path", quitting publishing CID to IPNS
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
