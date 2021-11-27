#!/bin/bash

# this doesn't work in a cron job, due to x-display
#!/bin/bash -l

# this doesn't work without a login shell, due to module command:
#source /site/env/sysprofile

# so just do this:
export PATH=/apps/bin:${PATH}

limit=30

outdir=$HOME/disk/clas-`date +%Y%m%d`
mkdir -p $outdir
cd $outdir

touch log
echo "STARTING ..." >> log
date >> log

d=/w/hallb-scifs17exp/clas/

../disk-database.pl $d clas >& clas.log
../disk-report.pl clas 30 > index.html

du -s $d/* 2> perms.txt 1> du.txt
awk -F'[‘’]' '{print$2}' ./perms.txt > perms2.txt
sort -r -n du.txt | awk '{printf"%12s %s\n",$1,$2}' > du2.txt
mv -f du2.txt du.txt
mv -f perms2.txt perms.txt

scp *.txt *.html jlabl5:~/public_html/clas/disk/work

echo "FINISHED." >> log
date >> log

