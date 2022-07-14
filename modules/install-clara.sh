#!/bin/sh

top=/group/clas12/packages/

# version numbers to install:
#clara=4.3.12
clara=5.0.2
coatjava=8.2.0
grapes=2.12
stub=
#stub=_tmp

# the installation location:
export CLARA_HOME=$top/clara/${clara}_${coatjava}${stub}

# download the clara installer script:
#rm -f install-claracre-clas.sh
#wget https://claraweb.jlab.org/clara/_downloads/install-claracre-clas.sh 
#chmod +x install-claracre-clas.sh

# run the installer:
./install-claracre-clas.sh -f ${clara} -v ${coatjava} -g ${grapes} -j 11
#./install-claracre-clas.sh -f ${clara} -v ${coatjava} -g ${grapes}

# generate coatjava install via symbolic link:
cd ../coatjava
if [ -e ./${coatjava} ]
then
  echo "WARNING:  coatjava/${coatjava} already exists."
  echo '          Not generating coatjava symlink nor module' 
  exit
fi
ln -s ../clara/${clara}_${coatjava}/plugins/clas12 ${coatjava}

# generate new modulefile for coatjava(+clara) based on a previous one:
cd ../local/etc/modulefiles/coatjava
sed "s/8\.1\.2/${coatjava}/" 8.1.2 | sed "s/5\.0\.2/${clara}/" > ${coatjava}

# sometimes clara comes with broken permissions, fix them:
chmod -R +r $CLARA_HOME
find $CLARA_HOME -type d -exec chmod 755 "{}" \;

# link in all available fieldmaps:
cd $CLARA_HOME/plugins/clas12/etc/data/magfield
rm -f *.dat
ln -s $top/local/share/magfield/binary/*.dat .
cd -

