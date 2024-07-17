#!/bin/sh
# Downloading is idempotent. Because wget only continues downloads if needed, if something was already downloaded and has the same filename, it won't try again.

retrieve_urls() {
    local base_url='https://data.chc.ucsb.edu/products/CHIRPS-2.0'
    local precision="$1"
    local dataset_type="$2"

    local url
    if [ "$dataset_type" = "prelim" ]; then
        url="${base_url}/prelim/global_daily/netcdf/${precision}"
    else
        url="${base_url}/global_daily/netcdf/${precision}"
    fi

    # Fetch the directory listing in html, extract .nc file links, and append to download file
    curl "${url}/" |
        grep 'href="' |
        sed -n 's/.*href="\([^"]*\.nc\)".*/\1/p' |
        sed "s|^|${url}/|"
}

retrieve_and_write_urls() {
    local dir_name="$1"
    local precision="$2"
    local dataset_type="$3"
    local urls

    mkdir -p "$dir_name" # create if it doesn't exist yet
    cd "$dir_name"
    urls=$(retrieve_urls "$precision" "$dataset_type")
    echo "$urls" > ./download-links.txt
    cd ..
}

download() {
    local dir_name="$1"

    cd "$dir_name"
    echo "Downloading all datasets from: $dir_name"
    # wget's flag --timestamping makes it check to see if files from the incoming server are marked as newer or not than the ones we've already downloaded
    # This makes it idempotent, as long as the download files all remain inside the same folder and as the same names
    # Using `parallel` runs as many concurrent downloads as possible
    # +0 of the --jobs flag in GNU parallel tells it to consume as many concurrent processes as possible
    cat download-links.txt | parallel --jobs +0 wget --timestamping {}
    cd ..
}

retrieve_and_write_urls p05 p05 "final"
retrieve_and_write_urls p25 p25 "final"
retrieve_and_write_urls prelim p05 "prelim"

download p05
download p25
download prelim
