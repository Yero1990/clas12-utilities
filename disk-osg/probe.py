#!/usr/bin/env python3

import re
import os
import sys
import glob
import json
import argparse
import subprocess

json_format =  {'indent':2, 'separators':(',',': '), 'sort_keys':True}
log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'

condor_data = None

def condor_query(cluster_id):
  '''Get the JSON for a particular job from condor_q
  cluster_id must be fully qualified, e.g. #####.##'''
  global condor_data
  if condor_data is None:
    cmd = ['condor_q','gemc','-nobatch','-json']
    condor_data = json.loads(subprocess.check_output(cmd).decode('UTF-8'))
  for x in condor_data:
    if 'ClusterId' in x and 'ProcId' in x:
      if '%d.%d' % (x['ClusterId'],x['ProcId']) == cluster_id:
        return x
  return None

def readlines_reverse(filename,max_lines):
  '''Get the trailing lines from a file, stopping
  after max_lines unless max_lines is negative'''
  n_lines = 0
  with open(filename) as qfile:
    qfile.seek(0, os.SEEK_END)
    position = qfile.tell()
    line = ''
    while position >= 0:
      if n_lines > max_lines and max_lines>0:
        break
      qfile.seek(position)
      try:
        next_char = qfile.read(1)
      except:
        next_char = ''
      if next_char == "\n":
         n_lines += 1
         yield line[::-1]
         line = ''
      else:
         line += next_char
      position -= 1
  yield line[::-1]

def crawl():
  '''Crawl the log directory, linking condor/gemc
  job ids, user names, and log files'''
  ret = {}
  for dirpath,dirnames,filenames in os.walk('/osgpool/hallb/clas12/gemc'):
    for filename in filenames:
      fullfilepath = dirpath+'/'+filename
      m = re.search(log_regex,fullfilepath)
      if m is None:
        continue
      user = m.group(1)
      gemc = m.group(2)
      condor = m.group(3)+'.'+m.group(4)
      if condor not in ret:
        ret[condor] = {'gemc':gemc, 'user':user, 'logs':[]}
      ret[condor]['logs'].append(fullfilepath)
  return ret

if __name__ == '__main__':

  cli = argparse.ArgumentParser()
  cli.add_argument('-condor', default=[], metavar='# or #.#', action='append', type=str, help='limit by condor cluster id')
  cli.add_argument('-gemc', default=[], metavar='#', action='append', type=str, help='limit by gemc job id')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by user name')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='dump last # lines of logs (all=negative, 0=just-names)')
  cli.add_argument('-json', default=False, action='store_true', help='dump JSON')

  args = cli.parse_args(sys.argv[1:])

  for condor,val in sorted(crawl().items()):

    if len(args.condor) > 0:
      if condor not in args.condor:
        if condor.split('.').pop(0) not in args.condor:
          continue

    if len(args.gemc) > 0 and val['gemc'] not in args.gemc:
      continue

    if len(args.user) > 0 and val['user'] not in args.user:
      continue

    if args.held is not None:
      if condor_query(condor) is None:
        continue
      if condor_query(condor).get('JobStatus') != 5:
        continue

    print('%16s %10s %12s' % (condor,val['gemc'],val['user']))

    if args.json:
      print(json.dumps(condor_query(condor),**json_format))

    if args.tail is not None:
      for x in val['logs']:
        print(x)
        if args.tail != 0:
          print('\n'.join(readlines_reverse(x, args.tail)))
          print()

