#!/bin/sh
# Immediately exit on error
set -e

original_dir=$(pwd)

# Change to the directory of the script
script_dir=$(dirname "$0")
cd "$script_dir"

# Switch to the repo directory since this script is inside of the operations directory
cd ..

for arg in "$@"; do
    case "$arg" in
        nc)
            find . -type f -name ".nc" -delete
            ;;
        download-links)
            find . -type f -name "download-links.txt" -delete
            ;;
        zarr)
            find . -type d -name "*.zarr" -exec rm -rf {} +
            ;;
        misc)
            find . -type d -name ".ruff_cache" -exec rm -rf {} +
            find . -type f -name "*.cid" -delete
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "See script switch case statements for all options"
            ;;
    esac
done

# Return to the original directory
cd "$original_dir"
