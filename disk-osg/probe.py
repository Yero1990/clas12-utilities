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
sub_clis.required = True

cli_list = sub_clis.add_parser('list')
cli_list.add_argument('-condor', default=[], action='append', type=str, help='limit by condor cluster id')
cli_list.add_argument('-gemc', default=[], action='append', type=str, help='limit by gemc job id')
cli_list.add_argument('-user', default=[], action='append', type=str, help='limit by user name')

cli_dump = sub_clis.add_parser('dump')
cli_dump.add_argument('-condor', default=[], action='append', type=str, help='limit by condor cluster id')
cli_dump.add_argument('-tail', default=0, metavar='#', type=int, help='dump last # lines of logs (default=0, all=negative)')

args = cli.parse_args(sys.argv[1:])

def readlines_reverse(filename,max_lines):
  n_lines = 0
  with open(filename) as qfile:
    qfile.seek(0, os.SEEK_END)
    position = qfile.tell()
    line = ''
    while position >= 0:
      if n_lines > max_lines and max_lines>0:
        break
      qfile.seek(position)
      next_char = qfile.read(1)
      if next_char == "\n":
         n_lines += 1
         yield line[::-1]
         line = ''
      else:
         line += next_char
      position -= 1
  yield line[::-1]

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
    condor = m.group(3)+'.'+m.group(4)

    if condor not in data:
      data[condor] = {'gemc':gemc, 'user':user, 'logs':[]}

    data[condor]['logs'].append(fullfilepath)

# print request based on command-line arguments:
for key,val in sorted(data.items()):

  if len(args.condor) > 0:
    if key not in args.condor:
      if key.split('.').pop(0) not in args.condor:
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
    print('\n\n::: Next Job ::::::::::::::::::::::::::::::::::::::::::::::::\n')
    print('%16s %10s %12s' % (key,val['gemc'],val['user']))
    print()
    for x in val['logs']:
      print('::: Next Log :::\n')
      print(x)
      if args.tail != 0:
        print('\n'.join(readlines_reverse(x, args.tail)))
      print()


