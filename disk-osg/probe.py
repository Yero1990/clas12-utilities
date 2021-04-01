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

cvmfs_errors=[
  'Transport endpoint is not connected',
  'Loaded environment state is inconsistent'
  #'No such file or directory',
]

condor_data = None

def condor_load(constraints=[], opts=[]):
  '''Load the JSON condor_q'''
  global condor_data
  if condor_data is None:
    condor_data = {}
    cmd = ['condor_q','gemc']
    cmd.extend(constraints)
    cmd.extend(opts)
    cmd.extend(['-nobatch','-json'])
    try:
      tmp = json.loads(subprocess.check_output(cmd).decode('UTF-8'))
      for x in tmp:
        if 'ClusterId' in x and 'ProcId' in x:
          condor_data['%d.%d'%(x['ClusterId'],x['ProcId'])] = x
        else:
          pass
    except:
      print('Error running command:  '+' '.join(cmd))
      sys.exit(1)
    condor_munge()

def condor_munge():
  ''' Assign custom parameters based on parsing
  some condor parameters '''
  for condor_id,job in condor_data.items():
    if 'UserLog' in job:
      m = re.search(log_regex, job['UserLog'])
      if m is not None:
        job['user'] = m.group(1)
        job['gemc'] = m.group(2)
        job['condor'] = m.group(3)+'.'+m.group(4)
        job['stderr'] = job['UserLog'][0:-4]+'.err'
        job['stdout'] = job['UserLog'][0:-4]+'.out'
        if condor_id != job['condor']:
          print('WTF!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

def condor_match(job, args):
  ''' Apply job constraints, on top of those condor knows about'''
  if len(args.condor)==0:
    if len(args.user)==0:
      if len(args.gemc)==0:
        return True
  for x in args.condor:
    if x == job['condor'] or x == job['condor'].split('.').pop(0):
      return True
  for x in args.user:
    if x == job['user']:
      return True
  for x in args.gemc:
    if x == job['gemc']:
      return True
  return False

def condor_get(condor_id):
  ret = []
  if condor_id in condor_data:
    ret.append(condor_data[condor_id])
  for key,val in condor_data.items():
    if key.split('.').pop(0) == condor_id:
      ret.append(condor_data[key])
  return ret

def readlines_reverse(filename, max_lines):
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

def check_cvmfs(job):
  ''' Check for CVMFS errors in logs.  True is good, no error. '''
  if os.path.isfile(job['stderr']):
    with open(job['stderr'],'r',errors='replace') as f:
      for line in f.readlines():
        for x in cvmfs_errors:
          if line.find(x) >= 0:
            return False
  return True

def __crawl():
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

  cli = argparse.ArgumentParser('Links condor with gemc job ids and users and log files')
  cli.add_argument('-condor', default=[], metavar='# or #.#', action='append', type=str, help='limit by condor cluster id')
  cli.add_argument('-gemc', default=[], metavar='#', action='append', type=str, help='limit by gemc job id')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by user name')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-running', default=False, action='store_true', help='limit to jobs currently in running state')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='dump last # lines of logs (all=negative, 0=just-names)')
  cli.add_argument('-json', default=False, action='store_true', help='dump JSON')
  cli.add_argument('-cvmfs', default=False, action='store_true', help='print hostnames from logs with CVMFS errors')

  args = cli.parse_args(sys.argv[1:])

  opts, constraints = [], []

  if args.held:
    opts.append('-hold')
  elif args.running:
    opts.append('-run')

  constraints.extend(args.condor)

  condor_load(constraints=constraints, opts=opts)

  cvmfs_hosts = []

  for x,y in condor_data.items():

    if not condor_match(y, args):
      continue

    if args.cvmfs:
      if not check_cvmfs(y):
        if 'LastRemoteHost' in y:
          cvmfs_hosts.append(y.get('MATCH_GLIDEIN_Site')+' '+y['LastRemoteHost']+' '+x)

    else:
      print('%16s %10s %12s' % (x,y['gemc'],y['user']))

      if args.tail is not None:
        for x in (y['UserLog'],y['stdout'],y['stderr']):
          print(x)
          if args.tail != 0:
            print('\n'.join(readlines_reverse(x, args.tail)))
            print()

      elif args.json:
        print(json.dumps(y, **json_format))

  if args.cvmfs and len(cvmfs_hosts) > 0:
    print('\n'.join(cvmfs_hosts))

