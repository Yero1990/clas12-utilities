#!/usr/bin/env python3

import sys
import argparse
import datetime
import subprocess
import collections

cli = argparse.ArgumentParser()
cli.add_argument('-q', default=False, action='store_true', help='use condor_q instead of condor_history')
cli.add_argument('-days', default=1, type=int, help='# days to look back (default=1)')
cli.add_argument('-condor', default=None, type=str, help='condor cluster id')
args = cli.parse_args(sys.argv[1:])

# calculate start time:
now = datetime.datetime.now()
start = now + datetime.timedelta(days=-args.days)
start = str(int(start.timestamp()))

# choose attributes to display, their aliases, and field lengths:
# https://research.cs.wisc.edu/htcondor/manual/v7.8/11_Appendix_A.html
attributes = collections.OrderedDict()
attributes['JobStatus'] = ['stat',4]
attributes['ExitCode'] = ['exit',4]
attributes['NumJobStarts'] = ['#',3]
attributes['JobCurrentStartDate'] = ['start',12]
attributes['CompletionDate'] = ['end',12]
attributes['Cmd'] = ['cmd',20]
attributes['Args'] = ['args',50]
# not useful:
#attributes['ExitSignal'] = ['sig',5]
#attributes['NumRestarts']
#attributes['MachineAttrMachine0']

# generate formatting and header:
fmt = '%15s'
header = ['clusterid']
for key in attributes.keys():
  fmt += ' %%%ds'%attributes[key][1]
  header.append(attributes[key][0])

# store some indices to manipulate:
cmd_index = list(attributes.keys()).index('Cmd')+1
args_index = list(attributes.keys()).index('Args')+1

# run condor_history:
if args.condor is not None:
  constraint = args.condor
else:
  constraint = 'gemc'
if args.q:
  cmd=['condor_q',constraint,'-af:j']
else:
  cmd=['condor_history',constraint,'-backwards','-completedsince',start,'-af:j']
cmd.extend(attributes.keys())
proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,universal_newlines=True)

print(fmt % tuple(header))

for line in iter(proc.stdout.readline, ''):

  if len(line.strip())>0:

    cols = line.strip().split()
    cols = [ x.replace('undefined','n/a') for x in cols ]

    # make human-readable dates:
    for i,x in enumerate(attributes.keys()):
      if x.endswith('Date'):
        if cols[i+1] == '0':
          cols[i+1] = 'n/a'
        else:
          # not sure about locale on timestamp:
          cols[i+1] = datetime.datetime.fromtimestamp(int(cols[i+1]))
          #cols[i+1] = datetime.datetime.utcfromtimestamp(int(cols[i+1]))
          cols[i+1] = cols[i+1].strftime('%m/%d %H:%M')

    # shorten the command name:    
    cmd = cols[cmd_index].split('/')
    cols[cmd_index] = '/'.join(cmd[5:7])

    # compress the args into one column:
    args = cols[args_index:]
    cols = cols[0:args_index+1]
    cols[args_index] = ' '.join(args)

    print(fmt % tuple(cols))

proc.wait()

print(fmt % tuple(header))

