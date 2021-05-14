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
import math
import socket
import getpass
import argparse
import datetime
import subprocess
import collections

json_format =  {'indent':2, 'separators':(',',': '), 'sort_keys':True}
log_regex = '/([a-z]+)/job_([0-9]+)/log/job\.([0-9]+)\.([0-9]+)\.'
job_states = {0:'U', 1:'I', 2:'R', 3:'X', 4:'C', 5:'H', 6:'E'}
job_counts = {'done':0, 'run':0, 'idle':0, 'held':0, 'other':0, 'total':0}
null_field = '-'
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

condor_data = collections.OrderedDict()

def condor_query(constraints=[], opts=[], hours=0, completed=False):
  '''Load data from condor_q and condor_history'''
  if not completed:
    condor_q(constraints=constraints, opts=opts)
  if hours > 0:
    condor_history(constraints=constraints, hours=hours)
  condor_munge()

def condor_read(path):
  global condor_data
  condor_data = json.load(open(path,'r'))
  condor_munge()

def condor_write(path):
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
  response = None
  try:
    response = subprocess.check_output(cmd).decode('UTF-8').rstrip()
    if re.fullmatch('Job %s fast-vacated'%job.get('condorid'), response) is None:
      raise ValueError()
  except:
    print('ERROR running command "%s":\n%s'%(' '.join(cmd),response))
  print(str(job.get('MATCH_GLIDEIN_Site'))+' '+str(job.get('RemoteHost'))+' '+str(job.get('condorid')))

def condor_hold_job(job):
  cmd = ['condor_hold', job.get('condorid')]
  response = None
  try:
    response = subprocess.check_output(cmd).decode('UTF-8').rstrip()
    print(response)
  except:
    print('ERROR running command "%s":\n%s'%(' '.join(cmd),response))

def condor_q(constraints=[], opts=[]):
  '''Get the JSON from condor_q'''
  cmd = ['condor_q','gemc']
  cmd.extend(constraints)
  cmd.extend(opts)
  cmd.extend(['-nobatch','-json'])
  condor_add_json(cmd)

def condor_history(constraints=[], hours=1):
  '''Get the JSON from condor_history'''
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
    job['user'] = None
    job['gemc'] = None
    job['host'] = None
    job['condor'] = None
    job['stderr'] = None
    job['stdout'] = None
    job['efficiency'] = None
    job['generator'] = get_generator(job)
    job['wallhr'] = condor_calc_wallhr(job)
    job['condorid'] = '%d.%d'%(job['ClusterId'],job['ProcId'])
    job['gemcjob'] = '.'.join(job.get('Args').split()[0:2])
    if job.get('RemoteHost') is not None:
      job['host'] = job.get('RemoteHost').split('@').pop()
    if 'UserLog' in job:
      m = re.search(log_regex, job['UserLog'])
      if m is not None:
        job['user'] = m.group(1)
        job['gemc'] = m.group(2)
        job['condor'] = m.group(3)+'.'+m.group(4)
        job['stderr'] = job['UserLog'][0:-4]+'.err'
        job['stdout'] = job['UserLog'][0:-4]+'.out'
        if condor_id != job['condor']:
          raise ValueError('condor ids do not match.')
    if job.get('RemoteWallClockTime') > 0:
      job['efficiency'] = float(job.get('RemoteUserCpu'))/job.get('RemoteWallClockTime')

def condor_calc_wallhr(job):
  '''Use available info to calculate wall hours, since there does
  not seem to be a more reliable way'''
  ret = None
  if job_states[job['JobStatus']] == 'X':
    return ret
  if job_states[job['JobStatus']] == 'H':
    return ret
  if job_states[job['JobStatus']] == 'E':
    return ret
  start = job.get('JobCurrentStartDate')
  end = job.get('CompletionDate')
  if start is not None and start > 0:
    start = datetime.datetime.fromtimestamp(int(start))
    if end is not None and end > 0:
      end = datetime.datetime.fromtimestamp(int(end))
    else:
      end = datetime.datetime.now()
    ret = '%.1f' % ((end - start).total_seconds()/60/60)
  return ret

###########################################################
###########################################################

def condor_yield(args):
  for condor_id,job in condor_data.items():
    if condor_match(job, args):
      yield (condor_id, job)

def condor_match(job, args):
  ''' Apply job constraints, on top of those condor knows about'''
  if len(args.condor)>0 and job['condor'] not in args.condor:
    if job['condor'].split('.').pop(0) not in args.condor:
      return False
  if len(args.gemc)>0 and job['gemc'] not in args.gemc:
    if job['gemcjob'] not in args.gemc:
      return False
  if len(args.user)>0 and job['user'] not in args.user:
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
  if args.idle and job_states.get(job['JobStatus']) != 'I':
    return False
  if args.completed and job_states.get(job['JobStatus']) != 'C':
    return False
  if args.running and job_states.get(job['JobStatus']) != 'R':
    return False
  if args.held and job_states.get(job['JobStatus']) != 'H':
    return False
  return True

def get_status_key(job):
  if job_states[job['JobStatus']] == 'H':
    return 'held'
  elif job_states[job['JobStatus']] == 'I':
    return 'idle'
  elif job_states[job['JobStatus']] == 'R':
    return 'run'
  elif job_states[job['JobStatus']] == 'C':
    return 'done'
  else:
    return 'other'

def average(alist):
  if len(alist) > 0:
    return '%.1f' % (sum(alist) / len(alist))
  else:
    return null_field

def stddev(alist):
  if len(alist) > 0:
    m = average(alist)
    s = sum([ (x-float(m))*(x-float(m)) for x in alist ])
    return '%.1f' % math.sqrt(s / len(alist))
  else:
    return null_field

def condor_cluster_summary(args):
  '''Tally jobs by condor's ClusterId'''
  ret = collections.OrderedDict()
  for condor_id,job in condor_yield(args):
    cluster_id = condor_id.split('.').pop(0)
    if cluster_id not in ret:
      ret[cluster_id] = job.copy()
      ret[cluster_id].update(job_counts.copy())
    ret[cluster_id][get_status_key(job)] += 1
    ret[cluster_id]['done'] = ret[cluster_id]['TotalSubmitProcs']
    ret[cluster_id]['done'] -= ret[cluster_id]['held']
    ret[cluster_id]['done'] -= ret[cluster_id]['idle']
    ret[cluster_id]['done'] -= ret[cluster_id]['run']
  return ret

def condor_site_summary(args):
  '''Tally jobs by site.  Note, including completed jobs
  here is only possible if condor_history is included.'''
  sites = collections.OrderedDict()
  for condor_id,job in condor_yield(args):
    site = job.get('MATCH_GLIDEIN_Site')
    if site not in sites:
      sites[site] = job.copy()
      sites[site].update(job_counts.copy())
      sites[site]['wallhr'] = []
      sites[site]['attempt'] = []
    sites[site]['attempt'].append(job['NumJobStarts'])
    sites[site]['total'] += 1
    sites[site][get_status_key(job)] += 1
    if args.running or job_states[job['JobStatus']] == 'C':
      try:
        x = float(job.get('wallhr'))
        sites[site]['wallhr'].append(x)
      except:
        pass
  for site in sites.keys():
    sites[site]['ewallhr'] = stddev(sites[site]['wallhr'])
    sites[site]['eattempt'] = stddev(sites[site]['attempt'])
    sites[site]['wallhr'] = average(sites[site]['wallhr'])
    sites[site]['attempt'] = average(sites[site]['attempt'])
    if args.hours <= 0:
      sites[site]['done'] = null_field
  return sort_dict(sites, 'total')

root_store = []
def condor_plot(args):
  global root_store
  import ROOT
  ROOT.gStyle.SetCanvasColor(0)
  ROOT.gStyle.SetPadColor(0)
  ROOT.gStyle.SetTitleFillColor(0)
  ROOT.gStyle.SetTitleBorderSize(0)
  ROOT.gStyle.SetFrameBorderMode(0)
  ROOT.gStyle.SetPaintTextFormat(".0f")
  ROOT.gStyle.SetLegendBorderSize(1)
  ROOT.gStyle.SetLegendFillColor(ROOT.kWhite)
  ROOT.gStyle.SetTitleFontSize(0.04)
  ROOT.gStyle.SetPadTopMargin(0.05)
  ROOT.gStyle.SetPadLeftMargin(0.11)
  ROOT.gStyle.SetPadBottomMargin(0.12)
  ROOT.gStyle.SetTitleXSize(0.05)
  ROOT.gStyle.SetTitleYSize(0.05)
  ROOT.gStyle.SetTextFont(42)
  ROOT.gStyle.SetStatFont(42)
  ROOT.gStyle.SetLabelFont(42,"x")
  ROOT.gStyle.SetLabelFont(42,"y")
  ROOT.gStyle.SetLabelFont(42,"z")
  ROOT.gStyle.SetTitleFont(42,"x")
  ROOT.gStyle.SetTitleFont(42,"y")
  ROOT.gStyle.SetTitleFont(42,"z")
  ROOT.gStyle.SetHistLineWidth(2)
  ROOT.gStyle.SetGridColor(15)
  ROOT.gStyle.SetPadGridX(1)
  ROOT.gStyle.SetPadGridY(1)
  ROOT.gStyle.SetOptStat(0)
  ROOT.gStyle.SetHistMinimumZero(ROOT.kTRUE)
  ROOT.gROOT.ForceStyle()
  can = ROOT.TCanvas('can','',900,500)
  can.Divide(3,2)
  can.Draw()
  h1wall_site = {}
  h1eff_gen = {}
  h1eff_site = {}
  h1eff = ROOT.TH1D('h1eff',';Efficiency',200,0,1.5)
  h2eff = ROOT.TH2D('h2eff',';Wall Hours;Efficiency',100,0,20,100,0,1.5)
  h1att = ROOT.TH1D('h1att',';Attempts',30,0.5,30.5)
  h1wall = ROOT.TH1D('h1wall',';Wall Hours',100,0,20)
  for condor_id,job in condor_yield(args):
    if job.get('efficiency') is not None:
      gen = job.get('generator')
      eff = float(job.get('efficiency'))
      wall = float(job.get('wallhr'))
      site = job.get('MATCH_GLIDEIN_Site')
      if gen not in h1eff_gen:
        h1eff_gen[gen] = h1eff.Clone('h1eff_gen_%s'%gen)
        h1eff_gen[gen].Reset()
      if site not in h1eff_site:
        h1eff_site[site] = h1eff.Clone('h1eff_site_%s'%site)
        h1wall_site[site] = h1wall.Clone('h1wall_site_%s'%site)
        h1eff_site[site].Reset()
        h1wall_site[site].Reset()
      h1eff.Fill(eff)
      h1wall.Fill(wall)
      h2eff.Fill(wall, eff)
      h1att.Fill(job.get('NumJobStarts'))
      h1eff_gen[gen].Fill(eff)
      h1eff_site[site].Fill(eff)
      h1wall_site[site].Fill(wall)
  set_histos_max(h1eff_gen.values())
  set_histos_max(h1eff_site.values())
  set_histos_max(h1wall_site.values())
  leg_gen = ROOT.TLegend(0.11,0.95-len(h1eff_gen)*0.05,0.3,0.95)
  leg_site = ROOT.TLegend(0.05,0.05,0.95,0.95)
  root_store = [h1eff, h2eff, h1att, h1wall, leg_gen, leg_site]
  root_store.extend(h1eff_gen.values())
  root_store.extend(h1eff_site.values())
  root_store.extend(h1wall_site.values())
  can.cd(6)
  h2eff.Draw('COLZ')
  can.cd(2)
  h1att.Draw()
  can.cd(1)
  max_sites = []
  for site in h1eff_site.keys():
    if site not in max_sites:
      inserted = False
      for ii,ss in enumerate(max_sites):
        if h1eff_site[site].GetEntries() > h1eff_site[ss].GetEntries():
          inserted = True
          max_sites.insert(ii, site)
          break
      if not inserted:
        max_sites.append(site)
  opt = ''
  for ii,gen in enumerate(sorted(h1eff_gen.keys())):
    h1eff_gen[gen].SetLineColor(ii+1)
    leg_gen.AddEntry(h1eff_gen[gen], gen, "l")
    h1eff_gen[gen].Draw(opt)
    opt = 'SAME'
  leg_gen.Draw()
  opt = ''
  for ii,site in enumerate(max_sites):
    if ii > 10:
      break
    leg_site.AddEntry(h1eff_site[site], '%s %d'%(site,h1eff_site[site].GetEntries()), "l")
    h1eff_site[site].SetLineColor(ii+1)
    h1wall_site[site].SetLineColor(ii+1)
    can.cd(4)
    h1eff_site[site].Draw(opt)
    can.cd(3)
    h1wall_site[site].Draw(opt)
    opt = 'SAME'
  can.cd(5)
  leg_site.Draw()
  can.Update()
  return can

def set_histos_max(histos):
  hmax = -999
  for h in histos:
    if h.GetMaximum() > hmax:
      hmax = h.GetMaximum()
  for h in histos:
    h.SetMaximum(hmax*1.1)

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

def readlines(filename):
  if filename is not None:
    if os.path.isfile(filename):
      with open(filename, errors='replace') as f:
        for line in f.readlines():
          yield line.strip()

def readlines_reverse(filename, max_lines):
  '''Get the trailing lines from a file, stopping
  after max_lines unless max_lines is negative'''
  n_lines = 0
  with open(filename, errors='replace') as qfile:
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

###########################################################
###########################################################

def check_cvmfs(job):
  ''' Return wether a CVMFS error is detected'''
  for line in readlines(job.get('stderr')):
    for x in cvmfs_error_strings:
      if line.find(x) >= 0:
        return False
  return True

# cache generator names to only parse log once per cluster
generators = {}
def get_generator(job):
  if job.get('ClusterId') not in generators:
    generators['ClusterId'] = null_field
    if job.get('UserLog') is not None:
      job_script = os.path.dirname(os.path.dirname(job.get('UserLog')))+'/nodeScript.sh'
      for line in readlines(job_script):
        m = re.search('events with generator >(.*)< with options', line)
        if m is not None:
          if m.group(1).startswith('clas12-'):
            generators['ClusterId'] = m.group(1)[7:]
          else:
            generators['ClusterId'] = m.group(1)
          break
        if line.find('echo LUND Event File:') == 0:
          generators['ClusterId'] = 'lund'
          break
  return generators.get('ClusterId')

def clas12mon(args):
  data = job_counts.copy()
  for job in condor_cluster_summary(args).values():
    for x in data.keys():
      data[x] += job[x]
  data.pop('done')
  data.pop('total')
  data['update_ts'] = int(datetime.datetime.now().timestamp())
  print(json.dumps(data, **json_format))
  return
  if getpass.getuser() is not 'gemc':
    print('ERROR:  Only user=gemc can publish to clas12mon.')
    sys.exit(1)
  auth = os.getenv('HOME')+'/.clas12mon.auth'
  if not os.path.isfile(auth):
    print('ERROR:  Authorization file does not exist:  '+auth)
  url = 'https://clas12mon.jlab.org/api/OSGEntries'
  auth = open(auth).read().strip()
  return requests.post(url, data=data, headers={'Authorization':auth})

def tail_log(job, nlines):
  print(''.ljust(80,'#'))
  print(''.ljust(80,'#'))
  print(job_table.get_header())
  print(job_table.job_to_row(job))
  for x in (job['UserLog'],job['stdout'],job['stderr']):
    if x is not None and os.path.isfile(x):
      print(''.ljust(80,'>'))
      print(x)
      if args.tail > 0:
        print('\n'.join(reversed(list(readlines_reverse(x, args.tail)))))
      elif args.tail < 0:
        for x in readlines(x):
          print(x)

###########################################################
###########################################################

class Column():
  def __init__(self, name, width, tally=None):
    self.name = name
    self.width = width
    self.tally = tally
    self.fmt = '%%-%d.%ds' % (self.width, self.width)

class Table():
  max_width = 114
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
    return self.fmt % tuple([str(x).strip() for x in values])
  def get_tallies(self):
    # assume it's never appropriate to tally the 1st column
    values = ['tally']
    for i in range(1,len(self.columns)):
      if self.columns[i].tally is not None and len(self.tallies[i]) > 0:
        values.append(sum(self.tallies[i]))
        if self.columns[i].tally is 'avg':
          if values[-1] > 0:
            values[-1] = '%.1f' % (values[-1]/len(self.tallies[i]))
        else:
          values[-1] = int(values[-1])
      else:
        values.append(null_field)
    return (self.fmt % tuple(values)).rstrip()
  def get_header(self):
    ret = ''.ljust(min(Table.max_width,self.width), null_field)
    ret += '\n' + (self.fmt % tuple([x.name for x in self.columns])).rstrip()
    ret += '\n' + ''.ljust(min(Table.max_width,self.width), null_field)
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
      ret = null_field
    elif name == 'Args':
      ret = ' '.join(value.split()[2:])
    elif name == 'ExitBySignal':
      ret = {True:'Y',False:'N'}[value]
    elif name == 'JobStatus':
      try:
        ret = job_states[value]
      except:
        pass
    elif name.endswith('Date'):
      if value == '0' or value == 0:
        ret = null_field
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
summary_table.add_column('condor','ClusterId',9)
summary_table.add_column('gemc','gemc',6)
summary_table.add_column('submit','QDate',12)
summary_table.add_column('total','TotalSubmitProcs',8,tally='sum')
summary_table.add_column('done','done',8,tally='sum')
summary_table.add_column('run','run',8,tally='sum')
summary_table.add_column('idle','idle',8,tally='sum')
summary_table.add_column('held','held',8,tally='sum')
summary_table.add_column('user','user',10)
summary_table.add_column('gen','generator',9)
summary_table.add_column('eff','efficiency',4)

site_table = CondorTable()
site_table.add_column('site','MATCH_GLIDEIN_Site',26)
site_table.add_column('total','total',8,tally='sum')
site_table.add_column('done','done',8,tally='sum')
site_table.add_column('run','run',8,tally='sum')
site_table.add_column('idle','idle',8,tally='sum')
site_table.add_column('held','held',8,tally='sum')
site_table.add_column('wallhr','wallhr',6)
site_table.add_column('stddev','ewallhr',7)
site_table.add_column('eff','efficiency',4)

job_table = CondorTable()
job_table.add_column('condor','condorid',13)
job_table.add_column('gemc','gemc',6)
job_table.add_column('site','MATCH_GLIDEIN_Site',10)
#job_table.add_column('host','host',20)
job_table.add_column('stat','JobStatus',4)
job_table.add_column('exit','ExitCode',4)
job_table.add_column('sig','ExitBySignal',4)
job_table.add_column('att','NumJobStarts',4,tally='avg')
job_table.add_column('wallhr','wallhr',6,tally='avg')
job_table.add_column('eff','efficiency',4,tally='avg')
job_table.add_column('start','JobCurrentStartDate',12)
job_table.add_column('end','CompletionDate',12)
job_table.add_column('user','user',10)
job_table.add_column('gen','generator',9)
#job_table.add_column('args','Args',30)

###########################################################
###########################################################

if __name__ == '__main__':

  cli = argparse.ArgumentParser(description='Wrap condor_q and condor_history and add features for CLAS12.',
      epilog='''Per-site wall-hour tallies ignore running jobs, unless -running is specified.
                Repeatable "limit" options are first OR\'d independently, then all "limit" options are AND'd together.''')
  cli.add_argument('-condor', default=[], metavar='# or #.#', action='append', type=str, help='limit by condor id (repeatable)')
  cli.add_argument('-gemc', default=[], metavar='# or #.#', action='append', type=str, help='limit by gemc submission id (repeatable)')
  cli.add_argument('-user', default=[], action='append', type=str, help='limit by portal submitter\'s username (repeatable)')
  cli.add_argument('-site', default=[], action='append', type=str, help='limit by OSG site name, pattern matched (repeatable)')
  cli.add_argument('-held', default=False, action='store_true', help='limit to jobs currently in held state')
  cli.add_argument('-hold', default=False, action='store_true', help='send matching jobs to hold state')
  cli.add_argument('-idle', default=False, action='store_true', help='limit to jobs currently in idle state')
  cli.add_argument('-running', default=False, action='store_true', help='limit to jobs currently in running state')
  cli.add_argument('-completed', default=False, action='store_true', help='limit to completed jobs')
  cli.add_argument('-summary', default=False, action='store_true', help='tabulate by cluster id instead of per-job')
  cli.add_argument('-sitesummary', default=False, action='store_true', help='tabulate by site instead of per-job')
  cli.add_argument('-hours', default=0, metavar='#', type=float, help='look back # hours for completed jobs (default=0)')
  cli.add_argument('-tail', default=None, metavar='#', type=int, help='print last # lines of logs (negative=all, 0=filenames)')
  cli.add_argument('-cvmfs', default=False, action='store_true', help='print hostnames from logs with CVMFS errors')
  cli.add_argument('-vacate', default=-1, metavar='#', type=float, help='vacate jobs with wall hours greater than #')
  cli.add_argument('-json', default=False, action='store_true', help='print full condor data in JSON format')
  cli.add_argument('-input', default=False, metavar='PATH', type=str, help='read condor data from a JSON file instead of querying')
  cli.add_argument('-clas12mon', default=False, action='store_true', help='publish results to clas12mon for timelines')
  cli.add_argument('-plot', default=False, const=True, nargs='?', help='generate plots (requires ROOT)')

  args = cli.parse_args(sys.argv[1:])

  if args.held + args.idle + args.running + args.completed > 1:
    cli.error('Only one of -held/idle/running/completed is allowed.')

  if (bool(args.vacate>=0) + bool(args.tail is not None) + bool(args.cvmfs) + bool(args.json)) > 1:
    cli.error('Only one of -cvmfs/vacate/tail/json is allowed.')

  if args.completed and args.hours <= 0 and not args.input:
    cli.error('-completed requires -hours is greater than zero or -input.')

  if socket.gethostname() != 'scosg16.jlab.org' and not args.input:
    cli.error('You must be on scosg16 unless using the -input option.')

  opts, constraints = [], []

  if args.held:
    opts.append('-hold')

  if args.running:
    opts.append('-run')

  constraints.extend(args.condor)

  if args.input:
    condor_read(args.input)
  else:
    condor_query(constraints=constraints, opts=opts, hours=args.hours, completed=args.completed)

  if args.clas12mon:
    clas12mon(args)
    sys.exit(0)

  if args.json:
    print(json.dumps(condor_data, **json_format))
    sys.exit(0)

  if args.plot is not False:
    c = condor_plot(args)
    if args.plot is not True:
      c.SaveAs(args.plot)
    print('Done Plotting.  Press Return to close.')
    input()
    sys.exit(0)

  for cid,job in condor_yield(args):

    if args.hold:
      condor_hold_job(job)

    if args.vacate>0:
      if job.get('wallhr') is not None:
        if float(job.get('wallhr')) > args.vacate:
          if job_states.get(job['JobStatus']) == 'R':
            condor_vacate_job(job)

    elif args.cvmfs:
      if not check_cvmfs(job):
        if 'LastRemoteHost' in job:
          print(job.get('MATCH_GLIDEIN_Site')+' '+job['LastRemoteHost']+' '+cid)

    elif args.tail is not None:
      tail_log(job, args.tail)

    else:
      job_table.add_job(job)

  if args.tail is None and not args.cvmfs:
    if len(job_table.rows) > 0:
      if args.summary or args.sitesummary:
        if args.summary:
          print(summary_table.add_jobs(condor_cluster_summary(args)))
        else:
          print(site_table.add_jobs(condor_site_summary(args)))
      else:
        print(job_table)

  sys.exit(0)

