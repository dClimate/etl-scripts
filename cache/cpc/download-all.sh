#!/bin/sh
# Downloading is idempotent. Because wget only continues downloads if needed, if something was already downloaded and has the same filename, it won't try again.

generate_urls() {
    local base_url="$1"
    local start_year="$2"
    local end_year="$3"
    local url

    local year=$start_year
    while [ $year -le $end_year ]; do
        url="${base_url}${year}.nc"
        echo "$url"
        year=$((year + 1))
    done
}

generate_and_write() {
    local dir_name="$1"
    local base_url="$2"
    local start_year="$3"
    local current_year=$(date +"%Y")
    local urls

    mkdir -p "$dir_name" # create if it doesn't exist yet
    cd "$dir_name"
    urls=$(generate_urls "$base_url" "$start_year" "$current_year")
    echo "$urls" > ./download-links.txt
    cd ..
}

download() {
    local dir_name="$1"

    cd "$dir_name"
    echo "Downloading all datasets from: $dir_name"
    # Using `parallel` runs as many concurrent downloads as possible
    # This is idempotent, as long as the download files all remain inside the same folder as this script, since the -c flag of wget will either resume or quit downloads if it sees the same filename
    # +0 of the --jobs flag in GNU parallel tells it to consume as many concurrent processes as possible
    cat download-links.txt | parallel --jobs +0 wget --timestamping {}
    cd ..
}

generate_and_write precip-global 'https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_precip/precip.' 1979

generate_and_write precip-conus 'https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_us_precip/RT/precip.V1.0.' 2016

generate_and_write tmax 'https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmax.' 1979

generate_and_write tmin 'https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmin.' 1979

download precip-global
download precip-conus
download tmax
download tmin
