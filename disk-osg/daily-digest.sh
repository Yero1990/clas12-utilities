#!/bin/bash

recipients='baltzell@jlab.org ungaro@jlab.org devita@jlab.org'

dirname="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

mkdir -p /osgpool/hallb/clas12/gemc/daily
timestamp=$(date +%Y%m%d_%H%M%S)
plotfile=$(mktemp /osgpool/hallb/clas12/gemc/daily/$timestamp.XXXXXX.pdf)
emailbody=$(mktemp /osgpool/hallb/clas12/gemc/daily/$timestamp.XXXXXX.txt)
touch $emailbody

function munge {
    # 1. remove username, keeping only site, host, and job ids
    # 2. sort and remove uniques, i.e. same host and job id
    # 3. count per host and sort by counts
    sed 's/@/ /g' $1 | awk '{print$1,$(NF-1),$NF}' | sort | uniq | awk '{print$1,$2}' | sort | uniq -c | sort -n -r
}

cvmfs_cache=$HOME/cvmfs-errors.txt
vacate_cache=$HOME/vacate-stalls.txt
xrootd_cache=$HOME/xrootd-stalls.txt

echo Nodes with CVMFS issues in the past 24 hours: >> $emailbody
munge $cvmfs_cache >> $emailbody
echo -e '\n' >> $emailbody

#echo Nodes with XRootD issues in the past 24 hours: >> $emailbody
#munge $xrootd_cache >> $emailbody
#echo -e '\n' >> $emailbody

munge $vacate_cache >> $emailbody
echo Vacated jobs in the past 24 hours: >> $emailbody
echo -e '\n'>> $emailbody

rm -f $cvmfs_cache $xrootd_cache $vacate_cache

export DISPLAY=:0.0
source /group/clas12/packages/setup.sh
module load clas12/dev
$dirname/condor-probe.py -completed -hours 24 -plot $plotfile >& /dev/null

cat $emailbody | mail -a $plotfile -s OSG-CLAS12-Daily-Digest $recipients

