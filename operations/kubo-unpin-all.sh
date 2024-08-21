#!/bin/bash
set -e

ipfs pin ls --type=recursive --quiet | xargs ipfs pin rm
ipfs pin ls --type=direct --quiet | xargs ipfs pin rm
