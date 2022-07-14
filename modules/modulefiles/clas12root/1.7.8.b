#%Module1.0
proc ModulesHelp { } {
    puts stderr "This module sets up environment for clas12root"
}

conflict clas12root

prereq gcc/9.2.0
prereq root/6.24.06
prereq rcdb
prereq ccdb

module load qadb/1.2.0

set version 1.7.8.b

setenv CLAS12ROOT /group/clas12/packages/clas12root/$version

prepend-path PATH $env(CLAS12ROOT)/bin
prepend-path LD_LIBRARY_PATH $env(CLAS12ROOT)/lib

