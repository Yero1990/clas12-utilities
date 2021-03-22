#!/usr/bin/env python3

import re
import os
import sys
import time
import argparse
import subprocess

DEFAULT_IGNORES = ['^.*\.hipo$','^.*/job_[0-9]+/nodeScript.sh$']

CLI = argparse.ArgumentParser(epilog='Note, this preserves directory modification times, unlike a `find -delete`.')
CLI.add_argument('-path', required=True, type=str, help='Path to search recursively for deletions.')
CLI.add_argument('-days', required=True, type=int, metavar='#', help='Age threshold in days.')
CLI.add_argument('-delete', default=False, action='store_true', help='Actually delete.  The default is just to print.')
CLI.add_argument('-gzip', default=False, action='store_true', help='Gzip files instead of deleting or just printing.')
CLI.add_argument('-empty', default=False, action='store_true', help='Delete empty directories, regardless their age.')
CLI.add_argument('-noignore', default=False, action='store_true', help='Disable the -ignore option, i.e. ignore nothing.')
CLI.add_argument('-ignore', default=[], type=str, action='append', help='Regular expressions of paths to ignore, repeatable. (default=%s)'%DEFAULT_IGNORES)

ARGS = CLI.parse_args(sys.argv[1:])

NOW = time.time()

if len(ARGS.ignore)==0:
  ARGS.ignore = DEFAULT_IGNORES
elif ARGS.noignore:
  ARGS.ignore = []

def old(path):
  age_days = float(NOW - os.path.getmtime(path))/60/60/24
  return age_days > ARGS.days

def ignore(path):
  ret = False
  for x in ARGS.ignore:
    if re.fullmatch(x, path) is not None:
      ret = True
      break
  return ret

# some directories may become empty along the way,
# so we iterate until nothing gets deleted:
while True:
  
  deleted = False

  for dirpath,dirnames,filenames in os.walk(ARGS.path):

    dirpath_deleted = False

    # store the top directory's modification time:
    dirpath_mtime = os.path.getmtime(dirpath)

    # delete old files from the top directory:
    for filename in filenames:
      fullfilepath = dirpath+'/'+filename
      if old(fullfilepath) and not ignore(fullfilepath):
        print(fullfilepath)
        if ARGS.gzip:
          if not fullfilepath.endswith('.gz'):
            subprocess.run(['gzip',fullfilepath])
        elif ARGS.delete:
          os.remove(fullfilepath)
          deleted = True
          dirpath_deleted = True

    # delete empty directories from the top directory:
    for dirname in dirnames:
      fulldirpath = dirpath+'/'+dirname
      if len(os.listdir(fulldirpath))==0:
        if old(fulldirpath) or ARGS.empty:
          print(fulldirpath)
          if ARGS.delete:
            os.rmdir(fulldirpath)
            deleted = True
            dirpath_deleted = True

    # restore the top directory's modification time:
    if dirpath_deleted:
      os.utime(dirpath, (dirpath_mtime, dirpath_mtime))

  # if we didn't delete anything on this iteration, stop:
  if deleted or not ARGS.delete:
    break

