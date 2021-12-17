#!/usr/bin/env python2
import os
import sys
import glob
import logging
import datetime
import argparse

sys.path.append('/group/clas12/packages/rcdb/1.0/python')
import rcdb

def find_directory(r):
  for d in dirs:
    if d.endswith(str('%.6d'%r)):
      return d
  return None

cli = argparse.ArgumentParser(description='Check for missing data on tape for recent runs in RCDB.',formatter_class=argparse.ArgumentDefaultsHelpFormatter)
cli.add_argument('path', help='e.g. /mss/clas12/rg-m/data')
cli.add_argument('-e', metavar='#', help='minimum hours since run start time for existence', type=float, default=4)
cli.add_argument('-c', metavar='#', help='minimum hours since run end time for completion', type=float, default=8)
cli.add_argument('-n', metavar='#', help='mininum number of events per run', type=int, default=1e5)
cli.add_argument('-f', metavar='#', help='minimum number of files per run', type=int, default=5)
cli.add_argument('-d', metavar='#', help='number of days to look back in RCDB', type=float, default=5)
cli.add_argument('-v', help='verbose mode, else only print failures', default=False, action='store_true')

args = cli.parse_args(sys.argv[1:])

if args.v:
  logging.basicConfig(level=logging.INFO,format='%(levelname)-9s: %(message)s')
else:
  logging.basicConfig(level=logging.CRITICAL,format='%(levelname)-9s: %(message)s')

logger = logging.getLogger()

if not os.path.isdir(args.path):
  logger.critical('Invalid path:  '+args.path)
  sys.exit(1)

dirs = glob.glob(args.path+'/*')

now = datetime.datetime.now()

db = rcdb.RCDBProvider('mysql://rcdb@clasdb.jlab.org/rcdb')

run = 1e9

while True:

  run = db.get_prev_run(run)

  try:
    run_start_time = db.get_condition(run, 'run_start_time').value
    age_hours_start = (now-run_start_time).total_seconds() / 60 / 60
    event_count = db.get_condition(run, 'event_count').value
    evio_files_count = db.get_condition(run, 'evio_files_count').value
  except AttributeError:
    logger.warning('Ignoring run with unknown RCDB parameters: '+str(run.number))
    continue

  try:
    run_end_time = db.get_condition(run, 'run_end_time').value
    age_hours_end = (now-run_end_time).total_seconds() / 60 / 60
  except AttributeError:
    run_end_time = None
    age_hours_end = None

  if event_count < args.n or evio_files_count < args.f:
    logger.warning('Ignoring small run: '+str(run.number))
    continue

  if age_hours_start > args.e:

    d = find_directory(run.number)

    if d is None:
      logger.critical('Run %d started more than %.1f hours ago in RCDB but missing /mss directory.'%(run.number,args.e))
      continue

    if age_hours_end is not None and age_hours_end > args.c and len(glob.glob(d+'/*')) < evio_files_count:
      logger.critical('Run %d ended more than %.1f hours ago in RCDB but missing /mss files.'%(run.number,args.c))
      continue

  logger.info('Run %d ran from %s to %s and complete at /mss.'%(run.number,run_start_time,run_end_time))

  if age_hours_start > args.d*24:
    break

