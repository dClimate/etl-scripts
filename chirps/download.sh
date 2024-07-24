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
    local dataset_type="$1"
    local precision="$2"
    local dir_name="${dataset_type}-${precision}"
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
    # Using xargs to run as many concurrent downloads as possible
    # This is idempotent, as long as the download files all remain inside the same folder as this script, since the -c flag of wget will either resume or quit downloads if it sees the same filename
    # --max-procs=0 flag tells it to run as many processes as possible
    cat download-links.txt | xargs --max-procs=0 -n 1 wget --timestamping
    cd ..
}

for arg in "$@"; do
    case "$arg" in
        final-p05)
            retrieve_and_write_urls final p05
            download final-p05
            ;;
        final-p25)
            retrieve_and_write_urls final p25
            download final-p25
            ;;
        prelim-p05)
            retrieve_and_write_urls prelim p05
            download prelim-p05
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            ;;
    esac
done
