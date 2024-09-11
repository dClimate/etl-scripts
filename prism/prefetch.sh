#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

generate_available() {
    local dataset_dir="$1"
    local data_type="$2"

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

    available_for_download_txt="available-for-download.txt"
    echo Writing list of all dates from $earliest_date to $latest_date to "$dataset_dir/$available_for_download_txt"
    local range_query_url="https://services.nacse.org/prism/data/public/releaseDate/$data_type/$earliest_date/$latest_date"

    cd "$dataset_dir"
    curl --silent $range_query_url | awk '{print $1 " " $4 " " $5 "?format=nc"}' > "$available_for_download_txt"
}

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
esac
