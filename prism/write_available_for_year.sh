#!/bin/sh
set -e

validate_year() {
    local year=$1
    if ! [[ "$year" =~ ^[0-9]+$ ]]; then
        echo "Error: '$year' is not a valid nonnegative number." >&2
        exit 1
    fi

    local current_year=$(date +"%Y")
    if [ "$year" -lt 1981 ] || [ "$year" -gt "$current_year" ]; then
        echo "Error: '$year' is out of the valid range (1981-$current_year)." >&2
        exit 1
    fi
}

validate_data_type() {
    local data_type=$1
    case "$data_type" in
        ppt|tmax|tmin)
            return 0
            ;;
        *)
            echo "Error: Invalid data type. Must be 'ppt', 'tmax', or 'tmin'." >&2
            exit 1
            ;;
    esac
}

validate_dir_name() {
    local dir_name=$1
    case "$dir_name" in
        precip-4km|tmax-4km|tmin-4km)
            return 0
            ;;
        *)
            echo "Error: Invalid dir name. Must be 'precip-4km', 'tmax-4km', or 'tmin-4km'." >&2
            exit 1
            ;;
    esac
}

# Check if we have the correct number of arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <dir_name> <data_type> <year>" >&2
    echo "  dir_name: precip-4km, tmax-4km, tmin-4km" >&2
    echo "  data_type: ppt, tmax, or tmin" >&2
    echo "  year: between 1981 and $current_year" >&2
    exit 1
fi

dir_name=$1

data_type=$2
validate_data_type "$data_type"

year=$3
validate_year "$year"

echo "Processing data type: $data_type, year: $year"
url="https://ftp.prism.oregonstate.edu/daily/$data_type/$year/"
out_filename="available-for-download-$year.txt"

cd "$dir_name"
# Output is in the format YYYYMMDD YYYY-MM-DD HH:MM format, with YYYYMMDD representing the date of the data itself, and YYYY-MM-DD HH:MM representing the timestamp of the last modified header
curl -s "$url" | \
grep -E 'PRISM_ppt_.*[0-9]{8}_bil\.zip' | \
sed -E 's/.*PRISM_ppt_.*_([0-9]{8})_bil\.zip.*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}).*/\1 \2/' | \
awk '{print $1, $2, $3}' | sort > "$out_filename"
cd ..
