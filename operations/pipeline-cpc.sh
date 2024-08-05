#!/bin/sh
# Exit if any command fails
set -e

process_dataset() {
    local dataset_name="$1"

    # Go into CPC
    cd cpc
    # Convert the dataset over
    sh download.sh "$dataset_name"
    python combine_to_zarr.py "$dataset_name"
    python zarr_to_ipld.py "$dataset_name"

    # Publish to IPNS
    cd ../operations
    sh publish-to-ipns.sh cpc

    # Go back to the repo root directory
    cd ..
}

original_dir=$(pwd)

# Find the root of the git repository
repo_root=$(git rev-parse --show-toplevel 2>/dev/null)

if [ -z "$repo_root" ]; then
    echo "Error: This script must be run from within the etl-scripts git repository." >&2
    exit 1
fi

# Change to the root directory of the repository
cd "$repo_root"

# Activate the python virtual environment
. .venv/bin/activate

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

# Return to the original directory
cd "$original_dir"
