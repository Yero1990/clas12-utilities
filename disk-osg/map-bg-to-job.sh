#!/bin/bash

usage='map-bg-to-job.sh USER GEMCID'

local_dir=/osgpool/hallb/clas12/gemc/$1/job_$2/log

if [ -z $2 ]
then
    echo $usage
    exit 1
fi

for log in $local_dir/*.out
do
    xrootd=`grep ^xroot: $log`
    gemc=`echo $log | awk -F/ '{print$7}' | awk -F_ '{print$2}'`
    job=`echo $log | awk -F/ '{print$9}' | awk -F. '{print$3}'`
    echo $gemc.$job $xrootd
done

