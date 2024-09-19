cd_to_script_dir() {
    cd $(dirname "$1")
}

check_python_virtualenv_activated() {
    if [[ -z "$VIRTUAL_ENV" ]]; then
        echo "No python virtual environment detected, exiting immediately"
        exit 1
    fi
}

print_usage() {
    echo Usage: bash "$0" "precip-4km|tmax-4km|tmin-4km"
    echo Example: bash "$0" precip-4km
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
        precip-4km|tmax-4km|tmin-4km)
            ;;
        *)
            echo "Error: Unknown argument $arg" >&2
            print_usage
            exit 1
            ;;
    esac
}
