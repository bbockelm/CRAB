#! /bin/sh
# CRAB related Stuff
export CRABDIR=`\pwd`/..
export CRABSCRIPT=${CRABDIR}

CRABPATH=${CRABDIR}/python
CRABPYTHON=${CRABDIR}/python

if [ -z "$PATH" ]; then
export PATH=${CRABPATH}
else
export PATH=${CRABPATH}:${PATH}
fi
if [ -z "$PYTHONPATH" ]; then
export PYTHONPATH=${CRABPYTHON}
else
export PYTHONPATH=${CRABPYTHON}:${PYTHONPATH}
fi
