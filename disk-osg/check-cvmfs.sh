#!/bin/bash

dirname="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

cache=$HOME/cvmfs-errors.txt

touch $cache

$dirname/probe.py -held -cvmfs >> $cache

if ! [ -z $1 ]; then
    tmp=$(mktemp /tmp/gemc/cvmfs.XXXXXX)
    sed 's/@/ /' $cache | awk '{print$1,$3}' | sort | uniq -c | sort -n -r > $tmp
    if [ $(cat $tmp | wc -l) -ne 0 ]; then
        echo "Nodes with CVMFS issues in the past 24 hours:"
        cat $tmp
    fi
    rm -f $cache $tmp
fi

