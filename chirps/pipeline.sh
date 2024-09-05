#!/bin/bash
set -e

# Use separate print usage and argument validation functions in pipeline.sh compared to the other shell scripts to allow for the "all" option
print_usage() {
    echo Usage: bash "$0" "final-p05|final-p25|prelim-p05|all"
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
    final-p05|final-p25|prelim-p05)
        pipeline $1
        ;;
    all)
        pipeline final-p05
        pipeline final-p25
        pipeline prelim-p05
        ;;
    *)
        echo "Error: Unknown argument $arg" >&2
        print_usage
        exit 1
        ;;
esac
