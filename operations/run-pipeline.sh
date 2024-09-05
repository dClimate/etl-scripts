#!/bin/sh
set -e

print_usage() {
    echo Usage: bash "$0" "cpc|chirps"
    echo Example: bash "$0" cpc
}

# Checks if number of arguments is exactly 1
num_arguments=$#
if (( num_arguments != 1 )); then
    echo "Error: Too many arguments"
    print_usage
    exit 1
fi

script_dir=$(dirname "$0")
cd "$script_dir"

provider=$1
case $provider in
    cpc)
        cd ../cpc
        bash pipeline.sh precip-conus
        bash pipeline.sh precip-global
        bash pipeline.sh tmax
        bash pipeline.sh tmin
        ;;
    chirps)
        cd ../chirps
        bash pipeline.sh final-p05
        bash pipeline.sh final-p25
        bash pipeline.sh prelim-p05
        ;;
    *)
        echo "Error: Unknown argument $provider" >&2
        print_usage
        exit 1
        ;;
esac
