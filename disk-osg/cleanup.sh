#!/bin/bash

dirname="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

$dirname/disk-cleanup.py \
  -path /osgpool/hallb/clas12/gemc
  -days 14
  -delete

$dirname/disk-cleanup.py \
  -path /osgpool/hallb/clas12/gemc
  -days 7
  -gzip

