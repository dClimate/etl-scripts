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

    # Clear file
    truncate -s 0 "$available_for_download_txt"

    echo Finding the years of available data for "$dataset_dir"
    local years_list_url="https://ftp.prism.oregonstate.edu/daily/$data_type/"
    local years_list=$(curl --silent "$years_list_url" | grep --extended-regexp '[1-2][0-9]{3}\/' | sed -E 's/.*>([1-2][0-9]{3})\/<.*/\1/' | sort)
    local earliest_year=$(echo "$years_list" | head -n 1)
    local latest_year=$(echo "$years_list" | tail -n 1)

    echo Retrieving dates available for data from $earliest_year to $latest_year
    local date_list_per_year_urls=$(echo "$years_list" | awk "{print \"$years_list_url\" \$1 \"/\"}")
    local date_list_results=$(echo "$date_list_per_year_urls" | xargs --max-procs=0 -n 1 curl --silent)

    local range_query_urls=""

    local year=$earliest_year
    while [ $year -le $latest_year ]; do
        # Get the earliest and latest date available for this year
        local dates_list_html=$(echo "$date_list_results" | grep -E "$year[0-1][0-9][0-3][0-9]_bil\.zip")
        local dates_list=$(echo "$dates_list_html" | grep -E 'PRISM_(ppt|tmax|tmin)_.*[0-9]{8}_bil\.zip' | sed -E 's/.*([1-2][0-9]{3}[0-1][0-9][0-3][0-9])_bil\.zip<.*/\1/' | sort)
        local earliest_date=$(echo "$dates_list" | head -n 1)
        local latest_date=$(echo "$dates_list" | tail -n 1)

        # Setup the range request with these dates
        local new_url="https://services.nacse.org/prism/data/public/releaseDate/$data_type/$earliest_date/$latest_date"
        # Concatenate with a newline
        range_query_urls="$range_query_urls
$new_url"

        # Move to the next year
        year=$((year + 1))
    done

    echo Constructing list of all dates with grid counts and download links...
    echo "$range_query_urls"
    local all_ranges=$(echo "$range_query_urls" | xargs --max-procs=0 -n 1 curl --silent)
    echo Writing list to "$dataset_dir/$available_for_download_txt"
    echo "$all_ranges" | sort | awk '{print $1, $4, $5 "?format=nc"}' > "$available_for_download_txt"

    cd ..
}

download() {
    local dataset_dir="$1"
    local available_for_download_txt="available-for-download.txt"

    # For every possible date, check if we already have it, and if not, download it
    # to_be_downloaded=$(cat "$dataset_dir/$available_for_download_txt" | xargs --max-procs=0 -n 1 bash update_date.sh "$dataset_dir")
    # echo $to_be_downloaded

    cd "$dataset_dir"

    while read date_info; do
        data_date=$(echo "$date_info" | awk '{print $1}')
        upstream_grid_count=$(echo "$date_info" | awk '{print $2}')
        download_url=$(echo "$date_info" | awk '{print $3}')

        # The first portion,  ls -p | grep -v / , gets only the files
        # grep -E 'regexp' just finds the files in the format YYYY-MM-DD_N.nc.zip
        # sed then trasnforms that into a list YYYY-MM-DD N
        current_download_info=$(ls -p | grep -v / | grep -E "${data_date}_[0-8]\.nc" | sed -E 's/([1-2][0-9]{3}-[0-1][0-9]-[0-3][0-9])_([1-8])\.nc/\1 \2/')
        current_grid_count=$(echo "$current_download_info" | awk '{print $2}')

        nc_name="${data_date}_${upstream_grid_count}.nc"
        zip_name="$nc_name.zip"
        # If the file does not exist OR the upstream grid count is greater, then download and extract the nc file to this directory
        if [ -z "$current_download_info" ] || [ $upstream_grid_count -gt $current_grid_count ]; then
            # Download and overwrite
            wget -O "$zip_name" "$download_url"

            # Unzip and extract just the nc file
            mkdir -p "$data_date"
            unzip "$zip_name" -d "$data_date"
            nc_inside_zip_name=$(ls "$data_date" | grep -E '.*.nc$')
            mv "$data_date/$nc_inside_zip_name" "./$nc_name"
            rm -r "$data_date"

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
