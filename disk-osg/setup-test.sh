#!/bin/sh

src=/volatile/clas12/osg
dest=/osgpool/hallb/clas12/gemc-test

rm -rf $dest/moranp
mkdir -p $dest/moranp/
cp -r -p $src/moranp/job_2455 $dest/moranp/

rm -rf $dest/tkutz
mkdir -p $dest/tkutz
cp -r -p $src/tkutz/job_2453 $dest/tkutz/

rm -rf /volatile/clas12/users/gemc/osg-test/*

