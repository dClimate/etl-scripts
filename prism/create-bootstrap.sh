#!/bin/bash
set -e

pin_and_publish() {
    dataset=$1

    cd $dataset

    local output_filename="bootstrap-prism-${dataset}.txt"
    local max_retries=5
    local retry_count=0
    while (( retry_count < $max_retries )); do
        echo Processing .nc.zip files for for $dataset
        # Avoid using --max-procs=0 here on xargs, this will prevent overloading the server and stopping normal ipfs serving operations slowing down, but also alleviates errors with replication due to too many replication requests being tried at once
        ls *.nc.zip | xargs -I {} ipfs-cluster-ctl add --name "bootstrap-prism-${dataset}-{}" "{}"
        ipfs-cluster-ctl pin ls | grep "bootstrap-prism-${dataset}" | awk '{print $3 " " $1}' | sort > $output_filename

        local zip_count=$(ls *.nc.zip | wc -l)
        local pin_count=$(ipfs-cluster-ctl pin ls | grep "bootstrap-prism-${dataset}" | wc -l)
        echo Number of .nc.zip files: $zip_count
        echo Number of files in bootstrap: $pin_count

        ((retry_count+=1))
        if (( zip_count != pin_count )); then
            echo The number of .nc.zip files does not match the number of files successfully pinned by the cluster!

            if (( retry_count > max_retries )); then
                echo Max retries reached. Bootstrap creation failed for $dataset
            else
                echo Retrying to create bootstrap for $dataset
            fi
        else
            # Publishing the results to IPNS
            cid=$(ipfs-cluster-ctl add --name $output_filename $output_filename | awk '{print $2}')
            ipns_key_name="bootstrap-prism-${dataset}"
            ipfs name publish --key="$ipns_key_name" "/ipfs/$cid"

            # Exit early
            retry_count=$((max_retries))
        fi
    done

    cd ..
}

original_dir=$(pwd)

# Find the root of the git repository and cd there
repo_root=$(git rev-parse --show-toplevel 2>/dev/null)

if [ -z "$repo_root" ]; then
    echo "Error: This script must be run from within the etl-scripts git repository." >&2
    exit 1
fi

# Change to the root directory of the repository
cd "$repo_root"

# Change to where PRISM is located
cd prism
for arg in "$@"; do
    dataset="$arg"

    case "$dataset" in
        precip-4km)
            pin_and_publish precip-4km
            ;;
        tmax-4km)
            pin_and_publish tmax-4km
            ;;
        tmin-4km)
            pin_and_publish tmin-4km
            ;;
        *)
            echo "Error: Invalid dataset '$dataset'. Must be 'precip-4km', 'tmax-4km', or 'tmin-4km'." >&2
            ;;
    esac
done

# Return to the original directory
cd "$original_dir"
