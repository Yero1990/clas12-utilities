#!/bin/bash

# this doesn't work in a cron job, due to x-display
#!/bin/bash -l

# this doesn't work without a login shell, due to module command:
#source /site/env/sysprofile

# so just do this:
export PATH=/apps/bin:${PATH}

limit=30

outdir=$HOME/disk/hps-`date +%Y%m%d`
mkdir -p $outdir
cd $outdir

TOP=/work/hallb/hps

touch log
echo "STARTING ..." >> log
date >> log

rm -f hps.log hps.html data.log data.html

../disk-database.pl $TOP/ hps >& hps.log
../disk-report.pl hps $limit > hps.html

../disk-database.pl $TOP/data/ hpsdata >& hpsdata.log
../disk-report.pl hpsdata $limit > hpsdata.html

rm -f du.txt du2.txt perms.txt perms2.txt

du -s $TOP/* $TOP/data/* 2> perms.txt 1> du.txt

awk -F'[‘’]' '{print$2}' ./perms.txt > perms2.txt

sort -r -n du.txt | awk '{printf"%12s %s\n",$1,$2}' > du2.txt

mv -f du2.txt du.txt

echo 'These private directories occupy an unknown amount of disk space:' > perms.txt
echo '   ("chgrp hps" and "chmod -R g+r" to fix it)' >> perms.txt
echo >> perms.txt
cat perms2.txt >> perms.txt
rm -f perms2.txt

scp *.txt *.html hps@ifarm1901:/group/hps/www/hpsweb/html/disk/work/

echo "FINISHED." >> log
date >> log

