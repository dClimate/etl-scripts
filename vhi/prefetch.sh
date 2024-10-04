#!/bin/bash
set -e

# Change to this script's directory
cd $(dirname "$0")

source shared-functions.sh

check_there_are_no_arguments $#

page_with_links='https://www.star.nesdis.noaa.gov/data/pub0018/VHPdata4users/data/Blended_VH_4km/VH/'
download_link_stem=$page_with_links

curl --silent $page_with_links | grep 'VH.nc' | sed -E 's/.*(VHP\.G04\.C07\.j01\.P20[0-9][0-9]0[0-5][0-9]\.VH\.nc).*/\1/' | sed "s|^|${download_link_stem}|" > vhi/download-links.txt
