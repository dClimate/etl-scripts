#!/bin/bash
set -e

# Use separate print usage and argument validation functions in pipeline.sh compared to the other shell scripts to allow for the "all" option
print_usage() {
    echo Usage: bash "$0" "precip-conus|precip-global|tmin|tmax|all"
    echo Example: bash "$0" all
}

num_arguments=$#

# Checks if number of arguments is exactly 1
if (( num_arguments != 1 )); then
    echo "Error: Too many arguments"
    print_usage
    exit 1
fi

script_dir=$(dirname "$0")
cd "$script_dir"

pipeline() {
    dataset=$1

    source ../.venv/bin/activate

    bash prefetch.sh $dataset
    bash fetch.sh $dataset
    bash transform.sh $dataset
    bash load_to_ipld.sh $dataset
    bash update_ipns.sh $dataset
}

case $1 in
    precip-conus|precip-global|tmax|tmin)
        pipeline $1
        ;;
    all)
        pipeline precip-conus
        pipeline precip-global
        pipeline tmax
        pipeline tmin
        ;;
    *)
        echo "Error: Unknown argument $arg" >&2
        print_usage
        exit 1
        ;;
esac
