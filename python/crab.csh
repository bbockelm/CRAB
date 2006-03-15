#! /bin/csh
# CRAB related Stuff
setenv CRABDIR `\pwd`/..
setenv CRABSCRIPT ${CRABDIR}

set CRABPATH=${CRABDIR}/python
set CRABPYTHON=${CRABDIR}/python

if ( ! $?path ) then
set path=${CRABPATH}
else
set path=( ${CRABPATH} ${path} )
endif
if ( ! $?PYTHONPATH ) then
setenv PYTHONPATH ${CRABPYTHON}
else
setenv PYTHONPATH ${CRABPYTHON}:${PYTHONPATH}
endif

which boss >& /dev/null
if ( $? ) then
  echo "boss env not set"
endif
