#!/usr/bin/env python3
#
# N. Baltzell, April 2021
#
# Wrap condor_q and condor_history commands into one, with convenenience
# options for common uses, e.g. query criteria specific to CLAS12 jobs,
# searching logs for CVMFS issues, and printing tails of logs.
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
job_states = { 0:'U', 1:'I', 2:'R', 3:'X', 4:'C', 5:'H', 6:'E' }
cvmfs_errors=[
  'Loaded environment state is inconsistent',
  'Command not found',
  'CVMFS ERROR'
#  'Transport endpoint is not connected',
#  'No such file or directory'
]

###########################################################
###########################################################

condor_data = None

def condor_load(constraints=[], opts=[], days=0, completed=False):
  '''Load data from condor_q and condor_history'''
  global condor_data
  condor_data = collections.OrderedDict()
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
    # doesn't seem to be a reliable way to get wall time, so calculate it:
    job['wallhr'] = condor_calc_wallhr(job)

def condor_calc_wallhr(job):
  ret = None
  start = job.get('JobCurrentStartDate')
  end = job.get('CompletionDate')
  if start is not None and start > 0:
    start = datetime.datetime.fromtimestamp(int(start))
    if end is not None and end > 0:
      end = datetime.datetime.fromtimestamp(int(end))
    else:
      end = datetime.datetime.now()
    ret = '%5.1f' % ((end - start).total_seconds()/60/60)
  return ret

def condor_match(job, args):
  ''' Apply job constraints, on top of those condor knows about'''
  # these are AND'd:::::::::::
  if args.idle:
    return job_states.get(job['JobStatus']) == 'I'
  if args.completed:
    return job_states.get(job['JobStatus']) == 'C'
  # support regexes for site, because some are very long strings:
  if len(args.site) > 0:
    if job.get('MATCH_GLIDEIN_Site') is not None:
      for site in args.site:
        if re.fullmatch(site,job['MATCH_GLIDEIN_Site']) is not None:
          return True
      return False
  # the rest are OR'd:::::::::
  if len(args.condor)==0:
    if len(args.user)==0:
      if len(args.gemc)==0:
        if len(args.site)==0:
          return True
  if job['condor'] in args.condor:
    return True
  if job['condor'].split('.').pop(0) in args.condor:
    return True
  if job['user'] in args.user:
    return True
  if job['gemc'] in args.gemc:
    return True
  return False

job_counts = {'done':0,'run':0,'idle':0,'held':0,'other':0,'total':0}

def condor_summary():
  ret = collections.OrderedDict()
  for condor_id,job in condor_data.items():
    cluster_id = condor_id.split('.').pop(0)
    if cluster_id not in ret:
      ret[cluster_id] = job.copy()
      ret[cluster_id].update(job_counts.copy())
    if job_states[job['JobStatus']] == 'H':
      ret[cluster_id]['held'] += 1
    elif job_states[job['JobStatus']] == 'I':
      ret[cluster_id]['idle'] += 1
    elif job_states[job['JobStatus']] == 'R':
      ret[cluster_id]['run'] += 1
    else:
      ret[cluster_id]['other'] += 1
    ret[cluster_id]['done'] = ret[cluster_id]['TotalSubmitProcs']
    ret[cluster_id]['done'] -= ret[cluster_id]['other']
    ret[cluster_id]['done'] -= ret[cluster_id]['held']
    ret[cluster_id]['done'] -= ret[cluster_id]['idle']
    ret[cluster_id]['done'] -= ret[cluster_id]['run']
  return ret

def condor_site_summary():
  sites = collections.OrderedDict()
  for condor_id,job in condor_data.items():
    site = job.get('MATCH_GLIDEIN_Site')
    if site not in sites:
      sites[site] = job.copy()
      sites[site].update(job_counts.copy())
    sites[site]['total'] += 1
    if job_states[job['JobStatus']] == 'H':
      sites[site]['held'] += 1
    elif job_states[job['JobStatus']] == 'I':
      sites[site]['idle'] += 1
    elif job_states[job['JobStatus']] == 'R':
      sites[site]['run'] += 1
    #elif job_states[job['JobStatus']] == 'C':
    #  sites[site]['done'] += 1
    else:
      sites[site]['other'] += 1
    sites[site]['done'] = sites[site]['total']
    #sites[site]['done'] -= sites[site]['other']
    sites[site]['done'] -= sites[site]['held']
    sites[site]['done'] -= sites[site]['idle']
    sites[site]['done'] -= sites[site]['run']
  return sort_dict(sites, 'total')

###########################################################
###########################################################

def sort_dict(dictionary, subkey):
  '''Sort a dictionary of sub-dictionaries by one of the keys
  in the sub-dictionaries'''
  ret = collections.OrderedDict()
  ordered_keys = []
  for k,v in dictionary.items():
    if len(ordered_keys) == 0:
      ordered_keys.append(k)
    else:
      inserted = False
      for i in range(len(ordered_keys)):
        if v[subkey] > dictionary[ordered_keys[i]][subkey]:
          ordered_keys.insert(i,k)
          inserted = True
          break
      if not inserted:
        ordered_keys.append(k)
  for x in ordered_keys:
    ret[x] = dictionary[x]
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

summary_columns = collections.OrderedDict()
summary_columns['TotalSubmitProcs'] = ['total',8]
summary_columns['QDate'] = ['submit',12]
summary_columns['done'] = ['done',8]
summary_columns['run'] = ['run',8]
summary_columns['idle'] = ['idle',8]
summary_columns['held'] = ['held',8]
summary_columns['user'] = ['user',10]
summary_columns['gemc'] = ['gemc',6]

site_columns = collections.OrderedDict()
site_columns['total'] = ['total',8]
site_columns['done'] = ['done',8]
site_columns['run'] = ['run',8]
site_columns['idle'] = ['idle',8]
site_columns['held'] = ['held',8]

table_columns = collections.OrderedDict()
table_columns['MATCH_GLIDEIN_Site'] = ['site',10]
table_columns['JobStatus'] = ['stat',4]
table_columns['ExitCode'] = ['exit',4]
table_columns['NumJobStarts'] = ['#',3]
table_columns['wallhr'] = ['wallhr',6]
table_columns['JobCurrentStartDate'] = ['start',12]
table_columns['CompletionDate'] = ['end',12]
table_columns['user'] = ['user',10]
table_columns['gemc'] = ['gemc',6]
table_columns['Args'] = ['args',30]

summary_format = '%-11.11s'
summary_header = ['clusterid']
for val in summary_columns.values():
  summary_format += ' %%-%d.%ds'%(val[1],val[1])
  summary_header.append(val[0])
summary_header = summary_format % tuple(summary_header)

site_format = '%-26.26s'
site_header = ['site']
for val in site_columns.values():
  site_format += ' %%-%d.%ds'%(val[1],val[1])
  site_header.append(val[0])
site_header = site_format % tuple(site_header)

table_format = '%-14.14s'
table_header = ['clusterid']
for val in table_columns.values():
  table_format += ' %%-%d.%ds'%(val[1],val[1])
  table_header.append(val[0])
table_header = table_format % tuple(table_header)

def human_date(timestamp):
  ret = 'n/a'
  if timestamp != 0 and timestamp != '0':
    try:
      x = datetime.datetime.fromtimestamp(int(timestamp))
      ret = x.strftime('%m/%d %H:%M')
    except:
      pass
  return ret

def tabulate_row(job, summary=False):
  if summary is None:
    cols = [ '%s' % job.get('MATCH_GLIDEIN_Site') ]
    atts = site_columns
    fmt = site_format
  elif summary:
    cols = [ '%d' % job['ClusterId'] ]
    atts = summary_columns
    fmt = summary_format
  else:
    cols = [ '%d.%d' % (job['ClusterId'],job['ProcId']) ]
    atts = table_columns
    fmt = table_format
  for att in atts.keys():
    x = job.get(att)
    if x is None:
      x = 'n/a'
    if type(x) is str:
      x = x.replace('undefined','n/a')
    if att.endswith('Date'):
      x = human_date(x)
    elif att == 'JobStatus':
      x = job_states.get(x)
    elif att == 'Args':
      x = ' '.join(x.split()[1:])
    cols.append(x)
  return fmt % tuple(cols)

###########################################################
###########################################################

if __name__ == '__main__':

  cli = argparse.ArgumentParser(description='Wrap condor_q and condor_history and add features for CLAS12.')
  cli.add_argument('-condor', default=[], metavar='# or #.#', action='append', type=str, help='limit by condor id')
  cli.add_argument('-gemc', default=[], metavar='#', action='append', type=str, help='limit by gemc submission id')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by portal submitter\'s username')
  cli.add_argument('-site', default=[], action='append', type=str, help='limit by OSG site name (can be a regular expression)')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-idle', default=False, action='store_true', help='limit to jobs currently in idle state')
  cli.add_argument('-running', default=False, action='store_true', help='limit to jobs currently in running state')
  cli.add_argument('-completed', default=False, action='store_true', help='limit to completed jobs')
  cli.add_argument('-days', default=0, metavar='#', type=int, help='look back # days for completed jobs (default=0)')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='print last # lines of logs (negative=all, 0=filenames)')
  cli.add_argument('-json', default=False, action='store_true', help='print condor\'s full data in JSON format')
  cli.add_argument('-cvmfs', default=False, action='store_true', help='print hostnames from logs with CVMFS errors')
  cli.add_argument('-summary', default=False, action='store_true', help='tabulate by cluster id instead of per-job')
  cli.add_argument('-sitesummary', default=False, action='store_true', help='tabulate by site instead of per-job')

  args = cli.parse_args(sys.argv[1:])

  opts, constraints = [], []

  if args.held:
    opts.append('-hold')
  elif args.running:
    opts.append('-run')

  if args.completed and args.days <= 0:
    cli.error('-completed requires -days is greater than zero')

  constraints.extend(args.condor)

  condor_load(constraints=constraints, opts=opts, days=args.days, completed=args.completed)

  cvmfs_hosts = []
  table_body = []
  json_data = []

  for cid,job in condor_data.items():

    if not condor_match(job, args):
      continue

    if args.cvmfs:
      if not check_cvmfs(job):
        if 'LastRemoteHost' in job:
          cvmfs_hosts.append(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.json:
      json_data.append(job)

    elif args.tail is not None:
      print(''.ljust(80,'#'))
      print(''.ljust(80,'#'))
      print(table_header)
      print(tabulate_row(job))
      for x in (job['UserLog'],job['stdout'],job['stderr']):
        if x is not None and os.path.isfile(x):
          print(''.ljust(80,'>'))
          print(x)
          if args.tail != 0:
            print('\n'.join(reversed(list(readlines_reverse(x, args.tail)))))

    else:
      table_body.append(tabulate_row(job))

  if args.cvmfs:
    if len(cvmfs_hosts) > 0:
      print('\n'.join(cvmfs_hosts))

  elif args.json:
    print(json.dumps(json_data, **json_format))

  elif args.tail is None:
    if len(table_body) > 0:
      if args.summary or args.sitesummary:
        if args.summary:
          print(summary_header)
          print('\n'.join([tabulate_row(x,True) for x in condor_summary().values()]))
        else:
          print(site_header)
          print('\n'.join([tabulate_row(x,None) for x in condor_site_summary().values()]))
      else:
        print(table_header)
        print('\n'.join(table_body))
        print(table_header)

