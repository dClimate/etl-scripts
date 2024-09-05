#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

retrieve_and_write_urls() {
    local dataset_type="$1"
    local precision="$2"
    local urls

    local base_url='https://data.chc.ucsb.edu/products/CHIRPS-2.0'
    local url
    if [ "$dataset_type" = "prelim" ]; then
        url="${base_url}/prelim/global_daily/netcdf/${precision}"
    else
        url="${base_url}/global_daily/netcdf/${precision}"
    fi
    echo $url

    local dataset_dir="${dataset_type}-${precision}"
    cd "$dataset_dir"

    curl "${url}/" |
        grep 'href="' |
        sed -n 's/.*href="\([^"]*\.nc\)".*/\1/p' |
        sed "s|^|${url}/|" > ./download-links.txt
}

case "$dataset" in
    final-p05)
        retrieve_and_write_urls final p05
        ;;
    final-p25)
        retrieve_and_write_urls final p25
        ;;
    prelim-p05)
        retrieve_and_write_urls prelim p05
        ;;
    *)
        echo "Unknown argument: $arg" >&2
        ;;
esac
