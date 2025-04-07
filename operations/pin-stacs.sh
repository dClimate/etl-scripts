#!/bin/bash
# This script takes a STAC catalog CID, and then pins all the JSONs in that STAC tree, including the catalog

# Call this from the etl-scripts folder, instead of inside of operations, since this assumes stac.py is within the current working directory

cid=$1

uv run stac.py collect all --plain $cid | xargs -n 1 ipfs pin add --recursive=false
