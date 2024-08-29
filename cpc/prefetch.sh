#!/bin/bash
set -e

print_usage() {
    echo Usage: bash "$0" "precip-conus|precip-global|tmin|tmax"
    echo Example: bash "$0" precip-conus
}

# Checks if number of arguments is exactly 1
if (( $# != 1 )); then
    echo "Error: Too many arguments"
    print_usage
    exit 1
fi

# Validate arguments are within the possibilities
case $1 in
    precip-conus|precip-global|tmax|tmin)
        ;;
    *)
        echo "Error: Unknown argument $1" >&2
        print_usage
        exit 1
        ;;
esac

dataset=$1

base_url=""
start_year=""
end_year=$(date +"%Y") # Set to the current year
case $dataset in
    precip-conus)
        base_url='https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_us_precip/RT/precip.V1.0.'
        start_year=2007
        ;;
    precip-global)
        base_url='https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_precip/precip.'
        start_year=1979
        ;;
    tmax)
        base_url='https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmax.'
        start_year=1979
        ;;
    tmin)
        base_url='https://psl.noaa.gov/thredds/fileServer/Datasets/cpc_global_temp/tmin.'
        start_year=1979
        ;;
esac

cd $dataset
year=$start_year
> ./download-links.txt # Clear the file first
while (( year < end_year )); do
    url="${base_url}${year}.nc"
    echo $url >> ./download-links.txt
    year=$((year + 1))
done
echo Finished writing download URLs for $dataset
