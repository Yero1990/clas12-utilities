#!/usr/bin/env python2
import os
import sys
import ccdb
import argparse

# Generate a table of all effective run ranges for a given CCDB table.
# Didn't appear to be an easy way to do this directly using CCDB API, but maybe there is.
# Meanwhile, it's easy enough to do it manually, just slow for large run ranges.

cli = argparse.ArgumentParser(description='Generate table of effective run ranges for a given CCDB table.',
    epilog='For example:  ccdb-ranges.py -min 4900 -max 5010 -table /runcontrol/fcup')
cli.add_argument('-min', metavar='#', help='minimum run number', type=int, required=True)
cli.add_argument('-max', metavar='#', help='maximum run number', type=int, required=True)
cli.add_argument('-table', help='table name', type=str, required=True)
cli.add_argument('-variation', help='variation name (default=default)', type=str, required=False, default='default')
#cli.add_argument('-timestamp', metavar='MM/DD/YYYY-HH:MM:SS', help='timestamp', type=str, required=False, default=None)
args = cli.parse_args(sys.argv[1:])

class CCDBRange():
  # Could use ccdb.Assignment instead, but did not want
  # to mess with all the sqlalchemy stuff
  def __init__(self,run_min,run_max,id,created,comment):
    self.run_min = run_min
    self.run_max = run_max
    self.id = id
    self.created = created
    self.comment = comment.strip().strip('"')
  @staticmethod
  def header():
    return '%6s %6s %7s  %19s  %s'%('min','max','id','created','comment')
  def __str__(self):
    return '%6d %6d %7d  %19s  %s'%(self.run_min,self.run_max,self.id,self.created,self.comment)

provider = ccdb.AlchemyProvider()
provider.connect(os.getenv('CCDB_CONNECTION'))

ranges = []

for run in range(args.min,args.max+1):
  assignment = provider.get_assignment(args.table,run,args.variation)
  if len(ranges)==0 or ranges[-1].id != assignment.id:
    ranges.append(CCDBRange(run,run,assignment.id,assignment.created,assignment.comment))
  elif ranges[-1].run_max == run-1:
    ranges[-1].run_max = run
  else:
    raise Exception('This should be impossible.')

print(CCDBRange.header())
print('\n'.join([str(r) for r in ranges]))

