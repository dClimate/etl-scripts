#!/bin/sh
# Ecurses down directories and, if there are any files ending in .zarr.hamt.cid AND there is a file named ipns-key-name.txt in the directory, this publishes that CID using that key name

# Function to process a directory
process_directory() {
    dir="$1"

    # Use find to locate .zarr.hamt.cid files (POSIX-compliant)
    find "$dir" -maxdepth 1 -name "*.zarr.hamt.cid" -print |
    while read -r cid_file; do
        # Check if there's an ipns-key-name.txt file in this directory
        ipns_key_file="$dir/ipns-key-name.txt"

        if [ -f "$ipns_key_file" ]; then
            # Read the CID and IPNS key name
            cid=$(cat "$cid_file")
            ipns_key=$(cat "$ipns_key_file")

            echo "Publishing $cid_file with key $ipns_key"

            if ipfs name publish --key="$ipns_key" "$cid"; then
                echo "Successfully published $cid_file with key $ipns_key"
            else
                echo "Failed to publish $cid_file with key $ipns_key"
            fi
        fi
    done

    # Recursively process subdirectories
    for subdir in "$dir"/*; do
        if [ -d "$subdir" ]; then
            process_directory "$subdir"
        fi
    done
}

# Start processing from the current directory
process_directory "."
