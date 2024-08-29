#!/bin/bash
set -e

print_usage() {
    echo Usage: bash "$0" "precip-conus|precip-global|tmin|tmax"
    echo Example: bash "$0" precip-conus
}

# Checks if number of arguments is exactly 1
if (( $# != 1 )); then
    echo "Error: Too many arguments"
    print_usage
    exit 1
fi

# Validate arguments are within the possibilities
case $1 in
    precip-conus|precip-global|tmax|tmin)
        ;;
    *)
        echo "Error: Unknown argument $1" >&2
        print_usage
        exit 1
        ;;
esac

dataset=$1

cd $dataset
echo "Fetching all .nc files for: $dataset"
# Using xargs to run as many concurrent downloads as possible
# --max-procs=0 flag tells it to run as many processes as possible
# wget's flag --timestamping makes it check to see if files from the incoming server are marked as newer or not than the ones we've already downloaded
# This makes fetching idempotent, as long as the download files all remain inside the same folder and as the same names
cat download-links.txt | xargs --max-procs=0 -n 1 wget --timestamping
