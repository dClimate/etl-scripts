print_usage() {
    echo Usage: bash "$0" "precip-conus|precip-global|tmin|tmax"
    echo Example: bash "$0" precip-conus
}

check_there_is_one_argument() {
    num_arguments=$1

    # Checks if number of arguments is exactly 1
    if (( num_arguments != 1 )); then
        echo "Error: Too many arguments"
        print_usage
        exit 1
    fi
}

check_argument_is_valid() {
    arg=$1

    # Validate arguments are within the possibilities
    case $arg in
        precip-conus|precip-global|tmax|tmin)
            ;;
        *)
            echo "Error: Unknown argument $arg" >&2
            print_usage
            exit 1
            ;;
    esac
}
