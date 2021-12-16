#!/usr/bin/env python2
import rcdb
import sys
import glob
import datetime
import argparse

# if start time is older than this, it should already have a directory:
max_hours_to_exist = 4

# if start time is older than this, all its files should exist:
max_hours_to_complete = 8

# ignore runs smaller than these:
min_event_count = 1e5
min_evio_files_count = 5

# walking back through RCDB, stop after we get this many days old:
max_lookback_days = 10

################################################################

cli = argparse.ArgumentParser(description='Check for missing data on tape.')
cli.add_argument('path', help='e.g. /mss/clas12/rg-m/data')
args = cli.parse_args(sys.argv[1:])
now = datetime.datetime.now()
dirs = glob.glob(args.path+'/*')

def find_directory(r):
  for d in dirs:
    if d.endswith(str('%.6d'%r)):
      return d
  return None

################################################################

db = rcdb.RCDBProvider('mysql://rcdb@clasdb.jlab.org/rcdb')

run = 1e9

while True:

  run = db.get_prev_run(run)
  run_start_time = None
  run_end_time = None
  event_count = None
  evio_files_count = None

  try:
    run_start_time = db.get_condition(run, 'run_start_time').value
  except AttributeError:
    pass
  try:
    run_end_time = db.get_condition(run, 'run_end_time').value
  except AttributeError:
    pass
  try:
    event_count = db.get_condition(run, 'event_count').value
  except AttributeError:
    pass
  try:
    evio_files_count = db.get_condition(run, 'evio_files_count').value
  except AttributeError:
    pass

  if run_start_time is None:
    continue

  # ignore small runs:
  if event_count is None or event_count < min_event_count:
    continue
  if evio_files_count is None or evio_files_count < min_evio_files_count:
    continue

  age_hours = (now-run_start_time).total_seconds() / 60 / 60

  if age_hours > max_hours_to_exist:

    d = find_directory(run.number)

    if d is None:
      print('ERROR:  Run %d older than %d hours in RCDB but missing directory.'%(run.number,max_hours_to_exist))
      continue

    if age_hours > max_hours_to_complete and len(glob.glob(d+'/*')) < evio_files_count:
      print('ERROR:  Run %d older than %d hours in RCDB but missing files.'%(run.number,max_hours_to_complete))
      continue

  #print('Run %d ok'%run.number)

  if age_hours > max_lookback_days*24:
    break



