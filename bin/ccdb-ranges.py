#!/usr/bin/env python2
import os
import sys
import ccdb
import argparse

class Range():
  def __init__(self, run_min, run_max, assignment):
    self.run_min = run_min
    self.run_max = run_max
    self.assignment = assignment
  @staticmethod
  def header():
    return '%6s %6s %7s  %19s  %s' % ('min','max','id','created','comment')
  def __str__(self):
    return '%6d %6d %7d  %19s  %s' % (self.run_min, self.run_max,
        self.assignment.id, self.assignment.created, self.assignment.comment)

cli = argparse.ArgumentParser(description='Generate table of effective run ranges for a given CCDB table.',
    epilog='For example:  ccdb-ranges.py -min 4900 -max 5010 -table /runcontrol/fcup')
cli.add_argument('-min', metavar='#', help='minimum run number', type=int, required=True)
cli.add_argument('-max', metavar='#', help='maximum run number', type=int, required=True)
cli.add_argument('-table', help='table name', type=str, required=True)
cli.add_argument('-variation', help='variation name (default=default)', type=str, required=False, default='default')
args = cli.parse_args(sys.argv[1:])

ranges = []
provider = ccdb.AlchemyProvider()
provider.connect(os.getenv('CCDB_CONNECTION'))

for run in range(args.min, args.max+1):
  assignment = provider.get_assignment(args.table, run, args.variation)
  if len(ranges)==0 or ranges[-1].assignment.id != assignment.id:
    ranges.append( Range(run, run, assignment) )
  elif ranges[-1].run_max == run-1:
    ranges[-1].run_max = run
  else:
    raise Exception('This should be impossible.')

print(Range.header()+'\n'+'\n'.join([str(r) for r in ranges]))

