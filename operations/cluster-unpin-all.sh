#!/bin/bash
set -e

ipfs-cluster-ctl pin ls | awk '{print $1}' | xargs -n 1 ipfs-cluster-ctl pin rm
