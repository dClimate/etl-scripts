#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

# Function to handle script interruption
cleanup() {
    echo
    echo "PRISM fetch script interrupted. Cleaning up and exiting..."
    exit 1
}
# Set up trap to handle SIGINT (Ctrl+C)
trap cleanup SIGINT

# Check, for every possible date, if we have the most up to date netCDF file
# If not, download and extract it from the zip
cd $dataset
while read data_date upstream_grid_count raw_dl_url; do
    nc_name="${data_date}_${upstream_grid_count}.nc"

    # Temporary stop until PRISM resolves issue with their backend
    # PRISM's netCDF converter for 2024-02-01 returns errors
    if [[ $data_date == 2024-02-01 ]]; then
        echo "Date 2024-02-01 found in data_date. Exiting fetch script early."
        exit
    fi

    # ! -e = Condition is true if file does not exist
    if [[ ! -e $nc_name ]]; then
        # Remove any older files with an older grid count if needed
        # We first check if the file exists, otherwise rm will return an error which will exit due to set -e
        for grid_count in {1..7}
        do
            older_nc_name=${data_date}_${grid_count}.nc
            if [[ -e $rm_name ]]; then
                rm $older_nc_name
            fi
        done

        # Download and overwrite
        zip_name="${nc_name}.zip"
        # wget -O "$zip_name" "$raw_dl_url"

        # Unzip and extract just the nc file
        mkdir -p $data_date
        unzip $zip_name -d $data_date > /dev/null
        cd $data_date
        mv *.nc "${data_date}_${upstream_grid_count}.nc"
        mv *.nc ../
        cd ..
        rm -r $data_date
        rm $zip_name

        # Wait 2 seconds between each download, to avoid overwhelming the PRISM servers
        # 2 seconds is the recommended amount found in PRISM's guide to their web service
        # https://prism.oregonstate.edu/documents/PRISM_downloads_web_service.pdf
        sleep 2
    fi
done < available-for-download.txt
