#!/bin/sh
# Exit if any command fails
set -e

process_dataset() {
    local dataset_name="$1"
    sh download.sh "$dataset_name"
    . ../.venv/bin/activate
    python combine_to_zarr.py "$dataset_name"
    python zarr_to_ipld.py "$dataset_name"
}

for arg in "$@"; do
    case "$arg" in
        all)
            process_dataset precip-conus
            process_dataset precip-global
            process_dataset tmax
            process_dataset tmin
            ;;
        precip-conus)
            process_dataset precip-conus
            ;;
        precip-global)
            process_dataset precip-global
            ;;
        tmax)
            process_dataset tmax
            ;;
        tmin)
            process_dataset tmin
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            ;;
    esac
done
