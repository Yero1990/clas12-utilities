#!/bin/bash

dirname="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

cache=$HOME/vacate-stalls.txt

touch $cache

$dirname/condor-probe.py -vacate 16 >> $cache

if ! [ -z $1 ]; then
    if [ $(cat $cache | wc -l) -ne 0 ]; then
        echo "Vacated jobs in the past 24 hours:"
        cat $cache
    fi
    rm -f $cache
fi

