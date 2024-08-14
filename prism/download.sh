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
    local dir="$1"
    local data_type="$2"
    local available_for_download_txt="available-for-download.txt"

    cd "$dir" # cd to the dataset's directory

    # Clear file
    truncate -s 0 "$available_for_download_txt"

    # Find the latest date the data is available for
    echo Finding the years of available data for "$dir"
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
        local dates_list=$(echo "$dates_list_html" | grep --extended-regexp 'PRISM_(?:ppt|tmax|tmin)_.*[0-9]{8}_bil\.zip' | sed -E 's/.*([1-2][0-9]{3}[0-1][0-9][0-3][0-9])_bil\.zip<.*/\1/' | sort)
        local earliest_date=$(echo "$dates_list" | head -n 1)
        local latest_date=$(echo "$dates_list" | tail -n 1)

        # Setup the range request with these dates
        local new_url="https://services.nacse.org/prism/data/public/releaseDate/$data_type/$earliest_date/$latest_date"
        range_query_urls="$range_query_urls\n$new_url"

        # Move to the next year
        year=$((year + 1))
    done

    echo Constructing list of all dates with grid counts and download links...
    local all_ranges=$(echo "$range_query_urls" | xargs --max-procs=0 -n 1 curl --silent)
    echo Writing list to "$dir/$available_for_download_txt"
    echo "$all_ranges" | sort | awk '{print $1, $4, $5 "?format=nc"}' > "$available_for_download_txt"

    cd ..
}

# Process the dataset
for arg in "$@"; do
    dataset="$arg"

    case "$dataset" in
        precip-4km)
            generate_available precip-4km ppt
            ;;
        tmax-4km)
            generate_available tmax-4km tmax
            ;;
        tmin-4km)
            generate_available tmin-4km tmin
            ;;
        *)
            echo "Error: Invalid dataset '$dataset'. Must be 'precip-4km', 'tmax-4km', or 'tmin-4km'." >&2
            ;;
    esac
done
