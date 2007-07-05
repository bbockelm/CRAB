#! /bin/sh
# CRAB related Stuff
export CRABDIR=`\pwd`/..
export CRABSCRIPT=${CRABDIR}

CRABPATH=${CRABDIR}/python
CRABPYTHON=${CRABDIR}/python
export CRABPSETPYTHON=${CRABDIR}/PsetCode

if [ -z "$PATH" ]; then
export PATH=${CRABPATH}
else
export PATH=${CRABPATH}:${PATH}
fi
if [ -z "$PYTHONPATH" ]; then
export PYTHONPATH=${CRABPYTHON}:${CRABDBSAPIPYTHON}:${CRABDLSAPIPYTHON}:${CRABPSETPYTHON}
else
export PYTHONPATH=${PYTHONPATH}:${CRABPYTHON}:${CRABDBSAPIPYTHON}:${CRABDLSAPIPYTHON}:${CRABPSETPYTHON}
fi

