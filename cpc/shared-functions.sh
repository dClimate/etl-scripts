# pwd should be ..../etl-scripts/cpc/
cd ..
source shared-functions.sh
cd cpc

error_usage_exit() {
    echo "Error: $1" >&2
    echo Usage: bash "$0" "precip-conus|precip-global|tmin|tmax" >&2
    echo Example: bash "$0" precip-conus >&2
    exit 1
}

check_valid_dataset() {
    dataset=$1

    case $dataset in
        precip-conus|precip-global|tmax|tmin)
            ;;
        *)
            error_usage_exit "Unknown dataset $dataset"
            ;;
    esac
}

create_cpc_log() {
    dataset="$1"
    cpc_log() {
        message="$1"
        echo_and_webhook "[CPC $dataset] $message"
    }
    export -f cpc_log
}
