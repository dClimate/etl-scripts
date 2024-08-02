#!/bin/sh
# Exit if any command fails
set -e

process_dataset() {
    local dataset_name="$1"
    sh download.sh "$dataset_name"
    source ../.venv/bin/activate
    python combine_to_zarr.py "$dataset_name"
    python zarr_to_ipld.py "$dataset_name"
}

for arg in "$@"; do
    case "$arg" in
        all)
            process_dataset final-p05
            process_dataset final-p25
            process_dataset prelim-p05
            ;;
        final-p05)
            process_dataset final-p05
            ;;
        final-p25)
            process_dataset final-p25
            ;;
        prelim-p05)
            process_dataset prelim-p05
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            ;;
    esac
done
