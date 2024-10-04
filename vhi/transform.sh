#!/bin/bash
set -e

# Change to this script's directory
cd $(dirname "$0")

source shared-functions.sh

check_there_are_no_arguments $#

check_python_virtualenv_activated

python ../shared_python_scripts/zarr_to_ipld.py "$PWD/vhi/"
