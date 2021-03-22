#!/bin/bash
#
# N. Baltzell, March 2021
#
# For rsyncing sufficiently old OSG data files from local scosg##
# filesystem to final Lustre destination for users, deleting their
# local copies upon successful transfer, and cleaning up other,
# older local files, e.g. logs and submission files.
#
# Does *not* delete anything at the destination, but there should be
# something in place to at least clean up old empty directories there.
#
# Intended to be run as a cronjob with MAILTO set, with no arguments,
# and without redirecting stdout/stderr.  Does *not* print anything
# to stdout/stderr unless there's a critical error or filesystem
# almost full, with all logging to $logfile.
#
# Notes:
#
# 1) The rsync version on scosg16 appears to be too old to support
#    full wildcards in ignore/exclude arguments.
#
# 2) There exist things in these CLAS12 OSG job that can be cleaned
#    up before registering in the payload, e.g. LUND and EVIO files,
#    whose information is already in HIPO, and background files.
#
# 3) Identically named nodeScript.sh exists at both the submit and
#    every job directory level, and we need to keep at least one of
#    them for provenance until, until that info is in HIPO. 
#
# 4) Job numbers appear in directory names, but not those jobs'
#    output files.  More fully-qualifed output file names (at least
#    job number, but could consider other specs too) could
#    facilitate automatic downsizing unnecessary directories later. 
#

########################################################################
# Static setup:
########################################################################

# user@host that should be running this script:
user=gemc
localhost=scosg16

# path on $localhost to be rsync'd to $dest:
src=/osgpool/hallb/clas12/gemc

# remote destination for contents of $src:
remotehost=dtn1902-ib
remotepath=/lustre19/expphy/volatile/clas12/osg2
dest=$user@$remotehost:$remotepath

# data files older than this will be rsync'd to $dest:
rsync_minutes=60

# files older than this will be deleted from $src:
delete_days=14

# script name and absolute path containing this script:
scriptname=$(basename $0)
dirname="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"

# output files for this instance of this script:
mkdir -p $src/transfers /tmp/gemc
timestamp=$(date +%Y%m%d_%H%M%S)
tmpfile=$(mktemp /tmp/gemc/$timestamp.XXXXXX)
logfile=$(mktemp $src/transfers/$timestamp.XXXXXX.log)

# conveneniences for logging:
errmsg="ERROR:  $scriptname: "
warnmsg="WARNING:  $scriptname: "
infomsg="INFO:  $scriptname: "
tee="tee -a $logfile"

########################################################################
# Interpret command line:
########################################################################

dryrun=0
verbose=0
usage="Usage:  $scriptname [-d (dry run)] [-v (verbose)]"
while getopts "dvh" opt; do
  case "${opt}" in
    d)
      dryrun=1
      ;;
    v)
      verbose=1
      ;;
    h)
      echo $usage
      exit 0
      ;;
    *)
      echo $usage
      echo "Incorrect arguments:  $@"
      exit 1
      ;;
  esac
done

# setup verbose/dryrun rsync options:
if [ $verbose -ne 0 ]; then
  rsync_opts=-vv
fi
if [ $dryrun -ne 0 ]; then
  rsync_opts="$rsync_opts --dry-run"
fi

########################################################################
# Convenience functions:
########################################################################

function cleanup {
  rm -f $tmpfile
}
trap cleanup EXIT

function checkssh {
  host=$1
  path=$2
  maxtries=10
  tries=0
  while [ 1 ]; do
    let tries=$tries+1
    echo "$infomsg Attempting ssh $host $path, #$tries ... "
    ssh -q $1 ls -d $path > /dev/null
    if [ $? -eq 0 ]; then
      echo "$infomsg ssh success."
      break
    else
      if [ $tries -gt $maxtries ]; then
        echo "$errmsg  ssh failed $maxtries times.  Aborting."
        return 1
      fi
      echo "$warnmsg ssh failed.  Retrying ..."
    fi
    sleep 10
  done
  return 0
}

########################################################################
# Perform some sanity checks and aborts before doing anything:
########################################################################

# abort if incorrect user:
[ $(whoami) != "$user" ] && echo "$errmsg User must be $user." | $tee && exit 2

# abort if incorrect host:
[ $(hostname -s) != "$localhost" ] && echo "$errmsg Must be on $localhost." | $tee && exit 3

# print warning if local disk is getting full:
used_frac=`df $src | tail -1 | awk '{print$5}' | sed 's/%//'`
[ "$used_frac" -gt 80 ] && echo "$warnmsg $src more than 80% full:  $used_frac%" | $tee 

# check we can ssh to remote host and access the destination path:
checkssh $user@$remotehost $remotepath 2&>1 >> $logfile
[ $? -ne 0 ] && echo "$errmsg ssh $user@$remotehost failed." && exit 55

########################################################################
# Do the transfers from local $src to remote $dest:
########################################################################

pushd $src > /dev/null

[ $(pwd) != "$src" ] && echo "$errmsg Failed to get to $src." | $tee && exit 10

# transfer *.hipo data files older than some minutes:

find . -type f -cmin +$rsync_minutes -name '*.hipo' > $tmpfile 2>&1 | $tee
[ $? -ne 0 ] && echo "$errmsg find *.hipo failed, aborting." | $tee && exit 5

if [ -s $tmpfile ]; then

  echo "$infomsg Files to Transfer:" >> $logfile ; cat $tmpfile >> $logfile
  rsync -a -R --files-from=$tmpfile $rsync_opts $src $dest 2>&1 | $tee
  [ $? -ne 0 ] && echo "$errmsg rsync *.hipo failed, aborting." | $tee && exit 6

  # first rsync claimed sucess, run it again with local deletion:
  if [ $dryrun -eq 0 ]; then
    rsync -a -R --remove-source-files --files-from=$tmpfile $rsync_opts $src $dest 2>&1 | $tee
    [ $? -ne 0 ] && echo "$errmsg rsync *.hipo failed, aborting." | $tee && exit 6
  fi

  # transfer the top-level nodeScript.sh from each submission:
  # (one day we can remove this, after job specifications are in HIPO)

  find . -type f -cmin +$rsync_minutes -regex '.*/job_[0-9]+/nodeScript\.sh' > $tmpfile 2>&1 | $tee
  [ $? -ne 0 ] && echo "$errmsg find nodeScript failed, aborting." | $tee && exit 7

  if [ -s $tmpfile ]; then
    cat $tmpfile >> $logfile
    rsync -a -R --files-from=$tmpfile $rsync_opts $src $dest 2>&1 | $tee
    [ $? -ne 0 ] && echo "$errmsg rsync nodeScript failed, aborting." | $tee && exit 8
  fi

else
  echo "$infomsg No Files to Transfer." >> $logfile
fi

popd > /dev/null

########################################################################
# Cleanup old stuff on local $src filesystem:
########################################################################

# delete files older than some number of days:
find $src -mindepth 1 -mtime +$delete_days -type f > $tmpfile
if [ -s $tmpfile ]; then
  echo "$infomsg Local Files to Delete:" >> $logfile ; cat $tmpfile >> $logfile
  if [ $dryrun -eq 0 ]; then
    xargs rm -f < $tmpfile 2>&1 >> $logfile
    [ $? -ne 0 ] && echo "$errmsg delete failed (1)." | $tee
  fi
else
  echo "$infomsg No Local Files to Delete." >> $logfile
fi

# delete old, empty directories:
if [ $dryrun -eq 0 ]; then
  find $src -mindepth 1 -mtime +$delete_days -type d -empty -delete 2>&1 | $tee
  [ $? -ne 0 ] && echo "$errmsg delete failed (2)." | $tee
fi

########################################################################
# Done
########################################################################

[ $dryrun -ne 0 ] && cat $logfile

exit 0

