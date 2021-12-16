#!/bin/bash

source /group/clas12/packages/setup.sh
module load rcdb
`dirname $0`/check-tape.py $@

