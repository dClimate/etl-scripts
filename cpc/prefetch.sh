#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

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
while (( year <= end_year )); do
    url="${base_url}${year}.nc"
    echo $url >> ./download-links.txt
    year=$((year + 1))
done
echo Finished writing download URLs for $dataset
