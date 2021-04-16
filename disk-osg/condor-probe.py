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
import time
import argparse
import datetime
import subprocess
import collections

json_format =  {'indent':2, 'separators':(',',': '), 'sort_keys':True}
log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'
job_states = { 0:'U', 1:'I', 2:'R', 3:'X', 4:'C', 5:'H', 6:'E' }
cvmfs_error_strings = [
  'Loaded environment state is inconsistent',
  'Command not found',
  'Unable to access the Singularity image',
  'CVMFS ERROR'
#  'No such file or directory'
#  'Transport endpoint is not connected',
]

###########################################################
###########################################################

condor_data = None

def condor_load(constraints=[], opts=[], hours=0, completed=False):
  '''Load data from condor_q and condor_history'''
  global condor_data
  condor_data = collections.OrderedDict()
  if not completed:
    condor_q(constraints=constraints, opts=opts)
  if hours > 0:
    condor_history(constraints=constraints, hours=hours)
  condor_munge()

def condor_read_file(path):
  global condor_data
  condor_data = json.load(open(path,'r'))

def condor_write_file(path):
  with open(path,'w') as f:
    f.write(json.dumps(condor_data, **json_format))

def condor_add_json(cmd):
  '''Add JSON condor data to local dictionary'''
  global condor_data
  response = None
  try:
    response = subprocess.check_output(cmd).decode('UTF-8')
    if len(response) > 0:
      for x in json.loads(response):
        if 'ClusterId' in x and 'ProcId' in x:
          condor_data['%d.%d'%(x['ClusterId'],x['ProcId'])] = x
        else:
          pass
  except:
    print('Error running command:  '+' '.join(cmd)+':')
    print(response)
    sys.exit(1)

def condor_vacate_job(job):
  cmd = ['condor_vacate_job', '-fast', job.get('condorid')]
  try:
    response = subprocess.check_output(cmd).decode('UTF-8').rstrip()
    if re.fullmatch('Job %s fast-vacated'%job.get('condorid'), response) is None:
      raise ValueError()
  except:
    print('ERROR running command "%s":\n%s'%(' '.join(cmd),response))
  print(str(job.get('MATCH_GLIDEIN_Site'))+' '+str(job.get('LastRemoteHost'))+' '+str(job.get('condorid')))

def condor_q(constraints=[], opts=[]):
  '''Get the JSON from condor_q'''
  cmd = ['condor_q','gemc']
  cmd.extend(constraints)
  cmd.extend(opts)
  cmd.extend(['-nobatch','-json'])
  condor_add_json(cmd)

def condor_history(constraints=[], hours=1):
  '''Get the JSON from condor_history'''
  global condor_data
  now = datetime.datetime.now()
  start = now + datetime.timedelta(hours = -hours)
  start = str(int(start.timestamp()))
  cmd = ['condor_history','gemc']
  cmd.extend(constraints)
  cmd.extend(opts)
  cmd.extend(['-json','-since',"CompletionDate!=0&&CompletionDate<%s"%start])
  condor_add_json(cmd)

def condor_munge():
  '''Assign custom parameters based on parsing some condor parameters'''
  for condor_id,job in condor_data.items():
    job['condorid'] = '%d.%d'%(job['ClusterId'],job['ProcId'])
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
    job['wallhr'] = condor_calc_wallhr(job)

def condor_calc_wallhr(job):
  '''Use available info to calculate wall hours, since there does
  not seem to be a more reliable way'''
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
  if len(args.condor)>0:
    if job['condor'] not in args.condor:
      if job['condor'].split('.').pop(0) not in args.condor:
        return False
  if len(args.user)>0:
    if job['user'] not in args.user:
      return False
  if len(args.gemc)>0:
    if job['gemc'] not in args.gemc:
      return False
  if len(args.site) > 0:
    if job.get('MATCH_GLIDEIN_Site') is None:
      return False
    matched = False
    for site in args.site:
      if job['MATCH_GLIDEIN_Site'].find(site) >= 0:
        matched = True
        break
    if not matched:
      return False
  if args.idle:
    return job_states.get(job['JobStatus']) == 'I'
  if args.completed:
    return job_states.get(job['JobStatus']) == 'C'
  return True

job_counts = {'done':0,'run':0,'idle':0,'held':0,'other':0,'total':0}

def condor_cluster_summary(args):
  '''Tally jobs by condor's ClusterId'''
  ret = collections.OrderedDict()
  for condor_id,job in condor_data.items():
    if not condor_match(job,args):
      continue
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

def condor_site_summary(args):
  '''Tally jobs by site'''
  sites = collections.OrderedDict()
  for condor_id,job in condor_data.items():
    if not condor_match(job,args):
      continue
    site = job.get('MATCH_GLIDEIN_Site')
    if site not in sites:
      sites[site] = job.copy()
      sites[site].update(job_counts.copy())
      sites[site]['wallhr'] = []
    sites[site]['total'] += 1
    if job_states[job['JobStatus']] == 'H':
      sites[site]['held'] += 1
    elif job_states[job['JobStatus']] == 'I':
      sites[site]['idle'] += 1
    elif job_states[job['JobStatus']] == 'R':
      sites[site]['run'] += 1
    elif job_states[job['JobStatus']] == 'C':
      #sites[site]['done'] += 1
      try:
        x = float(job.get('wallhr'))
        sites[site]['wallhr'].append(x)
      except:
        pass
    else:
      sites[site]['other'] += 1
    sites[site]['done'] = sites[site]['total']
    #sites[site]['done'] -= sites[site]['other']
    sites[site]['done'] -= sites[site]['held']
    sites[site]['done'] -= sites[site]['idle']
    sites[site]['done'] -= sites[site]['run']
  for site in sites.keys():
    if len(sites[site]['wallhr']) > 0:
      x = sum(sites[site]['wallhr'])
      sites[site]['wallhr'] = x / len(sites[site]['wallhr'])
      sites[site]['wallhr'] = '%5.1f' % sites[site]['wallhr']
    else:
      sites[site]['wallhr'] = 'n/a'
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
  ''' Return wether a CVMFS error is detected'''
  if job.get('stderr') is not None:
    if os.path.isfile(job['stderr']):
      with open(job['stderr'],'r',errors='replace') as f:
        for line in f.readlines():
          for x in cvmfs_error_strings:
            if line.find(x) >= 0:
              return False
  return True

###########################################################
###########################################################

class Column():
  def __init__(self, name, width, tally=None):
    self.name = name
    self.width = width
    self.tally = tally
    self.fmt = '%%-%d.%ds' % (self.width, self.width)

class Table():
  max_width = 100
  def __init__(self):
    self.columns = []
    self.rows = []
    self.tallies = []
    self.width = 0
  def add_column(self, column, tally=None):
    if not isinstance(column, Column):
      raise TypeError()
    self.columns.append(column)
    self.tallies.append([])
    self.fmt = ' '.join([x.fmt for x in self.columns])
    self.width = sum([x.width for x in self.columns]) + len(self.columns) - 1
  def add_row(self, values):
    self.rows.append(self.values_to_row(values).rstrip())
    self.tally(values)
  def tally(self, values):
    for i in range(len(values)):
      if self.columns[i].tally is not None:
        try:
          x = float(values[i])
          self.tallies[i].append(x)
        except:
          pass
  def values_to_row(self, values):
    return self.fmt % tuple([str(x) for x in values])
  def get_tallies(self):
    # assume it's never appropriate to tally the 1st column
    values = ['tally']
    for i in range(1,len(self.columns)):
      if self.columns[i].tally is not None:
        values.append(sum(self.tallies[i]))
        if values[-1] > 0:
          if self.columns[i].tally is 'avg':
            values[-1] = '%.1f' % (values[-1]/len(self.tallies[i]))
          else:
            values[-1] = int(values[-1])
      else:
        values.append('')
    return (self.fmt % tuple(values)).rstrip()
  def get_header(self):
    ret = ''.ljust(min(Table.max_width,self.width),'-')
    ret += '\n' + (self.fmt % tuple([x.name for x in self.columns])).rstrip()
    ret += '\n' + ''.ljust(min(Table.max_width,self.width),'-')
    return ret
  def __str__(self):
    rows = [self.get_header()]
    rows.extend(self.rows)
    rows.append(self.get_tallies())
    rows.append(self.get_header())
    return '\n'.join(rows)

class CondorColumn(Column):
  def __init__(self, name, varname, width, tally=None):
    super().__init__(name, width, tally)
    self.varname = varname

class CondorTable(Table):
  def add_column(self, name, varname, width, tally=None):
    super().add_column(CondorColumn(name, varname, width, tally))
  def job_to_values(self, job):
    return [self.munge(x.varname, job.get(x.varname)) for x in self.columns]
  def job_to_row(self, job):
    return self.values_to_row(self.job_to_values(job))
  def add_job(self, job):
    self.add_row(self.job_to_values(job))
    return self
  def add_jobs(self,jobs):
    for k,v in jobs.items():
      self.add_job(v)
    return self
  def munge(self, name, value):
    ret = value
    if value is None or value == 'undefined':
      ret = 'n/a'
    elif name == 'Args':
      ret = ' '.join(value.split()[1:])
    elif name == 'ExitBySignal':
      if value:
        ret = 'Y'
      else:
        ret = 'N'
    elif name == 'JobStatus':
      try:
        ret = job_states[value]
      except:
        pass
    elif name.endswith('Date'):
      if value == '0' or value == 0:
        ret = 'n/a'
      else:
        try:
          x = datetime.datetime.fromtimestamp(int(value))
          ret = x.strftime('%m/%d %H:%M')
        except:
          pass
    return ret

###########################################################
###########################################################

summary_table = CondorTable()
summary_table.add_column('id','ClusterId',11)
summary_table.add_column('total','TotalSubmitProcs',8,tally='sum')
summary_table.add_column('submit','QDate',12)
summary_table.add_column('done','done',8,tally='sum')
summary_table.add_column('run','run',8,tally='sum')
summary_table.add_column('idle','idle',8,tally='sum')
summary_table.add_column('held','held',8,tally='sum')
summary_table.add_column('user','user',10)
summary_table.add_column('gemc','gemc',6)

site_table = CondorTable()
site_table.add_column('site','MATCH_GLIDEIN_Site',26)
site_table.add_column('total','total',8,tally='sum')
site_table.add_column('done','done',8,tally='sum')
site_table.add_column('run','run',8,tally='sum')
site_table.add_column('idle','idle',8,tally='sum')
site_table.add_column('held','held',8,tally='sum')
site_table.add_column('wallhr','wallhr',8,tally='avg')

job_table = CondorTable()
job_table.add_column('id','condorid',14)
job_table.add_column('site','MATCH_GLIDEIN_Site',10)
job_table.add_column('stat','JobStatus',4)
job_table.add_column('exit','ExitCode',4)
job_table.add_column('sig','ExitBySignal',4)
job_table.add_column('#','NumJobStarts',3,tally='avg')
job_table.add_column('wallhr','wallhr',6,tally='avg')
job_table.add_column('start','JobCurrentStartDate',12)
job_table.add_column('end','CompletionDate',12)
job_table.add_column('user','user',10)
job_table.add_column('gemc','gemc',6)
job_table.add_column('args','Args',30)

###########################################################
###########################################################

if __name__ == '__main__':

  cli = argparse.ArgumentParser(description='Wrap condor_q and condor_history and add features for CLAS12.')
  cli.add_argument('-condor', default=[], metavar='# or #.#', action='append', type=str, help='limit by condor id')
  cli.add_argument('-gemc', default=[], metavar='#', action='append', type=str, help='limit by gemc submission id')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by portal submitter\'s username')
  cli.add_argument('-site', default=[], action='append', type=str, help='limit by OSG site name (pattern matched)')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-idle', default=False, action='store_true', help='limit to jobs currently in idle state')
  cli.add_argument('-running', default=False, action='store_true', help='limit to jobs currently in running state')
  cli.add_argument('-completed', default=False, action='store_true', help='limit to completed jobs')
  cli.add_argument('-hours', default=0, metavar='#', type=int, help='look back # hours for completed jobs (default=0)')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='print last # lines of logs (negative=all, 0=filenames)')
  cli.add_argument('-json', default=False, action='store_true', help='print condor\'s full data in JSON format')
  cli.add_argument('-cvmfs', default=False, action='store_true', help='print hostnames from logs with CVMFS errors')
  cli.add_argument('-summary', default=False, action='store_true', help='tabulate by cluster id instead of per-job')
  cli.add_argument('-sitesummary', default=False, action='store_true', help='tabulate by site instead of per-job')
  cli.add_argument('-vacate', default=-1, metavar='#', type=float, help='vacate jobs with wall hours greater than #')
  #cli.add_argument('-output', default=False, type=str, help='write condor query results to a JSON file')
  #cli.add_argument('-input', default=False, type=str, help='read condor query results from a JSON file')

  args = cli.parse_args(sys.argv[1:])

  opts, constraints = [], []

  if args.held:
    opts.append('-hold')
  elif args.running:
    opts.append('-run')

  if args.completed and args.hours <= 0:
    cli.error('-completed requires -hours is greater than zero')

  constraints.extend(args.condor)

  #if args.input:
  #  condor_read_file(args.input)
  #else:
  condor_load(constraints=constraints, opts=opts, hours=args.hours, completed=args.completed)
  #if args.output:
  #  condor_write_file(args.output)

  cvmfs_hosts = []
  json_data = []

  for cid,job in condor_data.items():

    if not condor_match(job, args):
      continue

    if args.vacate>0:
      if job.get('wallhr') is not None:
        if float(job.get('wallhr')) > args.vacate:
          if job_states.get(job['JobStatus']) == 'R':
            condor_vacate_job(job)

    elif args.cvmfs:
      if not check_cvmfs(job):
        if 'LastRemoteHost' in job:
          cvmfs_hosts.append(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.json:
      json_data.append(job)

    elif args.tail is not None:
      print(''.ljust(80,'#'))
      print(''.ljust(80,'#'))
      print(job_table.get_header())
      print(job_table.job_to_row(job))
      for x in (job['UserLog'],job['stdout'],job['stderr']):
        if x is not None and os.path.isfile(x):
          print(''.ljust(80,'>'))
          print(x)
          if args.tail != 0:
            print('\n'.join(reversed(list(readlines_reverse(x, args.tail)))))

    else:
      job_table.add_job(job)

  if args.cvmfs:
    if len(cvmfs_hosts) > 0:
      print('\n'.join(cvmfs_hosts))

  elif args.json:
    print(json.dumps(json_data, **json_format))

  elif args.tail is None:
    if len(job_table.rows) > 0:
      if args.summary or args.sitesummary:
        if args.summary:
          print(summary_table.add_jobs(condor_cluster_summary(args)))
        else:
          print(site_table.add_jobs(condor_site_summary(args)))
      else:
        print(job_table)

