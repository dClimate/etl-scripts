#!/bin/bash
set -e

# Function to handle script interruption
cleanup() {
    echo -e "\nPRISM download script interrupted. Cleaning up and exiting..."
    exit 1
}

# Set up trap to handle SIGINT (Ctrl+C)
trap cleanup SIGINT

generate_available() {
    local dataset_dir="$1"
    local data_type="$2"
    local available_for_download_txt="available-for-download.txt"

    cd "$dataset_dir" # cd to the dataset's directory

    echo Finding the years of available data for "$dataset_dir"
    local years_list_url="https://ftp.prism.oregonstate.edu/daily/$data_type/"
    local years_list=$(curl --silent "$years_list_url" | grep --extended-regexp '[1-2][0-9]{3}\/' | sed -E 's/.*>([1-2][0-9]{3})\/<.*/\1/' | sort)
    local earliest_year=$(echo "$years_list" | head -n 1)
    local latest_year=$(echo "$years_list" | tail -n 1)

    echo Retrieving dates available for data from $earliest_year to $latest_year
    local earliest_year_dates_url="https://ftp.prism.oregonstate.edu/daily/$data_type/$earliest_year/"
    local latest_year_dates_url="https://ftp.prism.oregonstate.edu/daily/$data_type/$latest_year/"
    # Matches all bil files in the ftp directory listing
    local prism_file_grep_regexp='>PRISM_(ppt|tmax|tmin)_.*[0-9]{8}_bil\.zip<'
    # find all YYYYMMDD strings
    local yyyymmdd_sed_regexp='s/.*([1-2][0-9]{3}[0-1][0-9][0-3][0-9]).*/\1/'

    # grep -o prints each result match in a newline
    local earliest_date=$(curl --silent "$earliest_year_dates_url" | grep -oE "$prism_file_grep_regexp" | sed -E "$yyyymmdd_sed_regexp" | sort | head -n 1)
    local latest_date=$(curl --silent "$latest_year_dates_url" | grep -oE "$prism_file_grep_regexp" | sed -E "$yyyymmdd_sed_regexp" | sort | tail -n 1)

    echo Writing list of all dates from $earliest_date to $latest_date to "$dataset_dir/$available_for_download_txt"
    local range_query_url="https://services.nacse.org/prism/data/public/releaseDate/$data_type/$earliest_date/$latest_date"
    curl --silent $range_query_url | awk '{print $1 " " $4 " " $5 "?format=nc"}' > "$available_for_download_txt"

    cd ..
}

download() {
    local dataset_dir="$1"
    local available_for_download_txt="available-for-download.txt"

    cd $dataset_dir

    # Check, for every possible date, if we have the most up to date netCDF file
    # If not, download and extract it from the zip
    while read data_date upstream_grid_count raw_dl_url; do
        nc_name="${data_date}_${upstream_grid_count}.nc"

        # Temporary stop until PRISM resolves issue with their backend
        # Currently dates for 2024-02-01 do have errors
        if [[ $data_date == 2024-02-01 ]]; then
            echo "Date 2024-02-01 found in data_date. Exiting early."
            cd ..
            return
        fi

        # ! -e = Condition is true if file does not exist
        if [ ! -e $nc_name ]; then
            # Remove any older files with an older grid count if needed
            # We first check if the file exists, otherwise rm will return an error which will exit due to set -e
            for grid_count in {1..8}
            do
                older_nc_name=${data_date}_${grid_count}.nc
                if [[ -e $rm_name ]]; then
                    rm $older_nc_name
                fi
            done

            # Download and overwrite
            zip_name="${nc_name}.zip"
            wget -O "$zip_name" "$raw_dl_url"

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
    done < "$available_for_download_txt"

    cd ..
}

# Process the dataset
for arg in "$@"; do
    dataset="$arg"

    case "$dataset" in
        precip-4km)
            generate_available precip-4km ppt
            download precip-4km
            ;;
        tmax-4km)
            generate_available tmax-4km tmax
            download tmax-4km
            ;;
        tmin-4km)
            generate_available tmin-4km tmin
            download tmin-4km
            ;;
        *)
            echo "Error: Invalid dataset '$dataset'. Must be 'precip-4km', 'tmax-4km', or 'tmin-4km'." >&2
            ;;
    esac
done
