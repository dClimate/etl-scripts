#!/bin/sh
# Recurses down directories and, if there are any files ending in .zarr.hamt.cid AND there is a file named ipns-key-name.txt in the directory, this publishes that CID using that key name

process_directory() {
    dir="$1"
    echo Publishing CIDs inside of directory "$dir"

    # Create a temporary file to store find results
    temp_file=$(mktemp)
    if [ $? -ne 0 ]; then
        echo "Failed to create temporary file"
        return 1
    fi

    # Use find to locate all .zarr.hamt.cid files recursively and store in temp file
    find "$dir" -type f -name "*.zarr.hamt.cid" -print > "$temp_file"

    # Read and process each line from the temp file
    while IFS= read -r cid_file
    do
        cid_abs_path=$(realpath "$cid_file")
        dir_abs_path=$(dirname "$cid_abs_path")
        ipns_key_file="$dir_abs_path/ipns-key-name.txt"

        # if there's an ipns-key-name.txt file in the same directory as the .zarr.hamt.cid file then publish
        if [ -f "$ipns_key_file" ]; then
	    cid=$(cat "$cid_file")
	    ipns_key=$(cat "$ipns_key_file")
            echo "Publishing $cid_file with key $ipns_key"
            if ipfs name publish --key="$ipns_key" "$cid"; then
                echo "Successfully published $cid_file with key $ipns_key"
            else
                echo "Failed to publish $cid_file with key $ipns_key"
            fi
        fi
    done < "$temp_file"

    rm "$temp_file"
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

for arg in "$@"; do
    case "$arg" in
        all)
            process_directory .
            ;;
        cpc)
            process_directory cpc
            ;;
        chirps)
            process_directory chirps
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "See script switch case statements for all options"
            ;;
    esac
done

# Return to the original directory
cd "$original_dir"
