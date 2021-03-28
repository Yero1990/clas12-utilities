#!/usr/bin/env python3

import re
import os
import sys
import glob
import argparse
import subprocess

log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'

cli = argparse.ArgumentParser()

sub_clis = cli.add_subparsers(dest='command')

cli_list = sub_clis.add_parser('list')
cli_list.add_argument('-condor', default=[], action='append', type=str, help='limit by condor cluster id')
cli_list.add_argument('-gemc', default=[], action='append', type=str, help='limit by gemc job id')
cli_list.add_argument('-user', default=[], action='append', type=str, help='limit by user name')

cli_dump = sub_clis.add_parser('dump', epilog='You probably want to pipe this to "more".')
cli_dump.add_argument('-condor', default=[], action='append', type=str, help='limit by condor cluster id')

args = cli.parse_args(sys.argv[1:])

# store whether user requested any condor jobs or only clusters:
subjob_requested = False
for x in args.condor:
  if x.find('.')>0:
    subjob_requested = True
    break

# crawl the log directory, linking condor/gemc job ids, user names, and log files:
data = {}
for dirpath,dirnames,filenames in os.walk('/osgpool/hallb/clas12/gemc'):

  for filename in filenames:

    fullfilepath = dirpath+'/'+filename
    m = re.search(log_regex,fullfilepath)
    if m is None:
      continue

    user = m.group(1)
    gemc = m.group(2)
    if subjob_requested:
      condor = m.group(3)+'.'+m.group(4)
    else:
      condor = m.group(3)

    if condor not in data:
      data[condor] = {'gemc':gemc, 'user':user, 'logs':[]}

    data[condor]['logs'].append(fullfilepath)

# print request based on command-line arguments:
for key,val in sorted(data.items()):

  if len(args.condor) > 0 and key not in args.condor:
    subjob_matched = False
    for x in args.condor:
      if x == str(key).split('.').pop(0):
        subjob_matched = True
        break
    if not subjob_matched:
      continue
 
  # just list jobs: 
  if args.command == 'list':    
    if len(args.gemc) > 0 and val['gemc'] not in args.gemc:
      continue
    if len(args.user) > 0 and val['user'] not in args.user:
      continue
    print('%16s %10s %12s' % (key,val['gemc'],val['user']))

  # not only list, but also dump logs and directory contents:
  elif args.command == 'dump':
    print('\n\nNext Job:::::::::::::::::::::::::::::::::::::\n')
    print('%16s %10s %12s' % (key,val['gemc'],val['user']))
    print()
    for x in val['logs']:
      print('Log file location:  ',x)
      with open(x,'r') as f:
        for line in f.readlines():
          print(line.strip())



