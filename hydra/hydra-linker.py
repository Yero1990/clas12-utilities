#!/usr/bin/env python3
import re,os,glob,datetime

input_dir = '/home/clasrun/CLAS12MON/output'
output_dir = '/home/baltzell/hydra'
blacklist_file = '/home/baltzell/hydra-blacklist.txt'
dir_regex = '^clas12mon_(\d+)_(\d+-\d+-\d+_\d+\.\d+\.\d+_[APM]+)$'
time_format = '%m-%d-%Y_%H.%M.%S_%p'

data = {}

with open(blacklist_file,'r') as f:
  blacklist = [x.strip() for x in f.readlines()]

for dirpath,dirnames,filenames in os.walk(input_dir):

  for dirname in dirnames:

    fulldirname = dirpath + '/' + dirname

    m = re.match(dir_regex, dirname)
    if m is None:
      continue

    runno = m.group(1)
    timestamp = m.group(2)

    try:
      timestamp = datetime.datetime.strptime(timestamp, time_format)
    except ValueError:
      continue

    if runno not in data:
      data[runno] = {}

    data[runno][timestamp] = fulldirname

    for png in glob.glob(fulldirname + '/*.png'):
      pass

  break

n_additions = 0

with open(blacklist_file,'a') as f:

  for runno in sorted(data.keys()):

    for chunk,timestamp in enumerate(sorted(data[runno].keys())):

      stub = runno + ' ' + str(chunk)
      if stub in blacklist:
        continue
      else:
        f.write(stub+'\n')

      idir = data[runno][timestamp]
      odir = output_dir + '/' + runno

      os.makedirs(odir, exist_ok=True)
      os.chmod(odir, 0o777)

      n_additions += 1

      for png in glob.glob(idir+'/*.png'):

        png = os.path.basename(png)

        if re.match('.*\d+-\d+-\d+_\d+\.\d+\.\d+.*', png) is not None:
          continue

        new_png = png[:-4] + '_%.4d.png'%chunk

        src = idir + '/' + png
        dst = odir + '/' + new_png

        os.symlink(src, dst)

print('Added %d runchunks.'%n_additions)

