#!/bin/bash
set -e

script_dir=$(dirname "$0")
cd "$script_dir"

source shared_functions.sh

check_there_is_one_argument $#

dataset=$1
check_argument_is_valid $dataset

cd $dataset

# Based on the dataset, get the ipns hash the list of files are published to
case $dataset in
    precip-4km)
        ipns_hash="/ipns/k51qzi5uqu5dir4p5b2i0pensejrdo9mvr66mxvaq87o0p9t61lyrdul5ws4c6"
        ;;
    tmax-4km)
        ipns_hash="/ipns/k51qzi5uqu5dk5ozhkbrmauboyvckczhopqhaeqsn052wencwlzlrrna38sehg"
        ;;
    tmin-4km)
        ipns_hash="/ipns/k51qzi5uqu5dl2iu0od81lnt2gd838wvbrthjgt129831i00ox98ctg96nsipk"
        ;;
esac

echo Getting all .tar.gz files
cid=$(ipfs name resolve $ipns_hash)
txt_filename="bootstrap-prism-${dataset}.txt"
ipfs get $cid -o $txt_filename
cat $txt_filename | awk '{print $2}' | xargs -n 1 ipfs get
cat $txt_filename | awk '{print $2 " " $1}' | xargs -n 2 mv

echo Untarring all .tar.gz files to retrieve .nc files
cat $txt_filename | awk '{print $1}' | xargs -n 1 tar -zxvf

echo Cleaning up by removing .tar.gz files and txt file with list of hashes
cat $txt_filename | awk '{print $1}' | xargs -n 1 rm
rm $txt_filename
