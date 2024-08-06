#!/bin/sh
# Exit immediately on any errors
set -e

# Usage Examples
# sh publish-to-ipns.sh cpc precip-conus
# sh publish-to-ipns.sh chirps final-p05
# This script can also be run from anywhere as long as it is inside the etl-scripts directory
# sh etl-scripts/dir1/dir2/publish-to-ipns.sh cpc tmax

original_dir=$(pwd)

# Find the root of the git repository and cd there
repo_root=$(git rev-parse --show-toplevel 2>/dev/null)

if [ -z "$repo_root" ]; then
    echo "Error: This script must be run from within the etl-scripts git repository." >&2
    exit 1
fi

# Change to the root directory of the repository
cd "$repo_root"

provider="$1"
dataset_name="$2"

dir="$provider/$dataset_name"
ipns_key_name_path="$dir/ipns-key-name.txt"
ipns_key_name=$(cat "$ipns_key_name_path")

ipns_address=$(ipfs key list -l | grep "$ipns_key_name" | awk '{print $1}')
current_published_cid=$(ipfs name resolve "$ipns_address")
echo "$current_published_cid"

dir="$provider/$dataset_name"
echo Publishing CIDs for "$dir"

cid=$(cat "$dir"/*.zarr.hamt.cid)

echo Executing: ipfs name publish --key="$ipns_key_name" "/ipfs/$cid"
ipfs name publish --key="$ipns_key_name" "/ipfs/$cid"

# Return to the original directory
cd "$original_dir"
