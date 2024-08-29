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
echo Processing all .nc files for $dataset
# The weird ls is so that we get absolute paths to send to the netcdf extractor
ls -d "$PWD/$dataset"/*.nc | xargs -n 1 python ../shared_python/extract_netcdf.py
