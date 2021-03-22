#!/bin/bash

##############################################

path=/volatile/clas12/osg
days=7
excludes=('*.hipo' 'nodeScript.sh')

##############################################

usage="Usage:  $(basename $0) [-d (dry run)]"
delete_args=-delete
while getopts "d" opt; do
  case "${opt}" in
    d)
      delete_args=
      ;;
    h)
      echo $usage
      exit 0
      ;;
    *)
      echo $usage
      echo "Incorrect arguments:  $@"
      exit 1
      ;;
  esac
done

exclude_args=
for x in ${excludes[*]}; do
  exclude_args="$exclude_args -not -name '$x'"
done

find $path -type f -mtime +$days $exclude_args $delete_args
find $path -mindepth 1 -mtime +$days -type d -empty $delete_args

