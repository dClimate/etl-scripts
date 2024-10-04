check_python_virtualenv_activated() {
    if [[ -z "$VIRTUAL_ENV" ]]; then
        echo Error: No python virtual environment detected, exiting immediately
        exit 1
    fi
}

print_usage() {
    echo Usage: bash "$0"
}

check_there_are_no_arguments() {
    num_arguments=$1

    if (( num_arguments != 0 )); then
        echo "Error: Too many arguments, received ${num_arguments}, expected 0"
        print_usage
        exit 1
    fi
}
