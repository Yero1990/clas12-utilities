#!/usr/bin/env python3
#
# N. Baltzell, April 2021
#
# Wrap condor_q and condor_history commands into one, provide additional
# query features specific to CLAS12 jobs, provide searching logs for CVMFS
# issues, provide printing tails of logs.
#

import re
import os
import sys
import glob
import json
import argparse
import datetime
import subprocess
import collections

json_format =  {'indent':2, 'separators':(',',': '), 'sort_keys':True}
log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'

cvmfs_errors=[
  'Transport endpoint is not connected',
  'Loaded environment state is inconsistent',
  'No such file or directory'
]

###########################################################
###########################################################

condor_data = None

def condor_load(constraints=[], opts=[], days=0, completed=False):
  '''Load data from condor_q and condor_history'''
  global condor_data
  condor_data = {}
  if not completed:
    condor_q(constraints=constraints, opts=opts)
  if days > 0:
    condor_history(constraints=constraints, days=days)
  condor_munge()

def condor_add_json(cmd):
  '''Add JSON condor data to local dictionary'''
  global condor_data
  tmp = None
  try:
    tmp = subprocess.check_output(cmd).decode('UTF-8')
    if len(tmp) > 0:
      tmp = json.loads(tmp)
      for x in tmp:
        if 'ClusterId' in x and 'ProcId' in x:
          condor_data['%d.%d'%(x['ClusterId'],x['ProcId'])] = x
        else:
          pass
  except:
    print('Error running command:  '+' '.join(cmd)+':')
    print(tmp)
    sys.exit(1)

def condor_q(constraints=[], opts=[]):
  '''Get the JSON from condor_q'''
  cmd = ['condor_q','gemc']
  cmd.extend(constraints)
  cmd.extend(opts)
  cmd.extend(['-nobatch','-json'])
  condor_add_json(cmd)

def condor_history(constraints=[], days=1):
  '''Get the JSON from condor_history'''
  global condor_data
  # calculate start time:
  now = datetime.datetime.now()
  start = now + datetime.timedelta(days = -days)
  start = str(int(start.timestamp()))
  cmd = ['condor_history','gemc']
  cmd.extend(constraints)
  cmd.extend(opts)
  cmd.extend(['-json','-completedsince',start])
  condor_add_json(cmd)

def condor_munge():
  ''' Assign custom parameters based on parsing some condor parameters '''
  for condor_id,job in condor_data.items():
    job['user'] = None
    job['gemc'] = None
    job['condor'] = None
    job['stderr'] = None
    job['stdout'] = None
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

###########################################################
###########################################################

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
        next_char = '?'
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
  if job.get('stderr') is not None:
    if os.path.isfile(job['stderr']):
      with open(job['stderr'],'r',errors='replace') as f:
        for line in f.readlines():
          for x in cvmfs_errors:
            if line.find(x) >= 0:
              return False
  return True

###########################################################
###########################################################

def __condor_get(condor_id):
  ret = []
  if condor_id in condor_data:
    ret.append(condor_data[condor_id])
  for key,val in condor_data.items():
    if key.split('.').pop(0) == condor_id:
      ret.append(condor_data[key])
  return ret

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

###########################################################
###########################################################

table_attributes = collections.OrderedDict()
table_attributes['JobStatus'] = ['stat',4]
table_attributes['ExitCode'] = ['exit',4]
table_attributes['NumJobStarts'] = ['#',3]
table_attributes['JobCurrentStartDate'] = ['start',12]
table_attributes['CompletionDate'] = ['end',12]
table_attributes['Cmd'] = ['cmd',20]
table_attributes['Args'] = ['args',50]
table_format = '%-15s'
table_header = ['clusterid']
for key in table_attributes.keys():
  table_format += ' %%-%ds'%table_attributes[key][1]
  if key == 'Cmd':
    table_header.append('gemc')
  else:
    table_header.append(table_attributes[key][0])
table_header = table_format % tuple(table_header)

def human_date(timestamp):
  x = datetime.datetime.fromtimestamp(int(timestamp))
  return x.strftime('%m/%d %H:%M')

def tabulate_row(job):
  cols = [ '%d.%d' % (job['ClusterId'],job['ProcId']) ]
  for att in table_attributes.keys():
    x = job.get(att)
    if x is None:
      x = 'n/a'
    if type(x) is str:
      x = x.replace('undefined','n/a')
    if att.endswith('Date'):
      if x == 0 or x == '0':
        x = 'n/a'
      else:
        x = human_date(x)
    elif att == 'Cmd':
      x = '/'.join(x.split('/')[5:7])
    cols.append(x)
  return table_format % tuple(cols)

###########################################################
###########################################################

if __name__ == '__main__':

  cli = argparse.ArgumentParser('Wrap condor_q and condor_history and add features for CLAS12.')
  cli.add_argument('-condor', default=[], metavar='# or #.#', action='append', type=str, help='limit by condor cluster id')
  cli.add_argument('-gemc', default=[], metavar='#', action='append', type=str, help='limit by gemc submission id')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by portal submitter user name')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-running', default=False, action='store_true', help='limit to jobs currently in running state')
  cli.add_argument('-completed', default=False, action='store_true', help='limit to completed jobs')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='dump last # lines of logs (all=negative, 0=just-names)')
  cli.add_argument('-json', default=False, action='store_true', help='dump JSON')
  cli.add_argument('-cvmfs', default=False, action='store_true', help='print hostnames from logs with CVMFS errors')
  cli.add_argument('-days', default=0, type=int, help='look back # days for completed jobs (default=0)')

  args = cli.parse_args(sys.argv[1:])

  opts, constraints = [], []

  if args.held:
    opts.append('-hold')
  elif args.running:
    opts.append('-run')

  constraints.extend(args.condor)

  condor_load(constraints=constraints, opts=opts, days=args.days, completed=args.completed)

  cvmfs_hosts = []
  table_body = []
  json_data = []

  for cid,job in condor_data.items():

    if not condor_match(job, args):
      continue

    table_body.append(tabulate_row(job))

    if args.cvmfs:
      if not check_cvmfs(job):
        if 'LastRemoteHost' in job:
          cvmfs_hosts.append(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.json:
      json_data.append(job)

    elif args.tail is not None:
      print(table_header)
      print(tabulate_row(job))
      for x in (job['UserLog'],job['stdout'],job['stderr']):
        if x is not None and os.path.isfile(x):
          print(x)
          if args.tail != 0:
            print('\n'.join(readlines_reverse(x, args.tail)))

  if args.cvmfs:
    if len(cvmfs_hosts) > 0:
      print('\n'.join(cvmfs_hosts))

  elif args.json:
    print(json.dumps(json_data, **json_format))

  elif args.tail is None:
    if len(table_body) > 0:
      print(table_header)
      print('\n'.join(table_body))
      print(table_header)

