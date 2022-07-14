singularity shell --home ${PWD}:/srv --pwd /srv --bind /cvmfs --contain \
      --ipc --pid /cvmfs/singularity.opensciencegrid.org/jeffersonlab/clas12software:production
