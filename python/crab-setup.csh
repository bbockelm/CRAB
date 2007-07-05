#! /bin/csh
# CRAB related Stuff
setenv CRABDIR `\pwd`/..
setenv CRABSCRIPT ${CRABDIR}

set CRABPATH=${CRABDIR}/python
setenv CRABPYTHON ${CRABDIR}/python
setenv CRABPSETPYTHON ${CRABDIR}/PsetCode

if ( ! $?path ) then
set path=${CRABPATH}
else
set path=( ${CRABPATH} ${path} )
endif
if ( ! $?PYTHONPATH ) then
setenv PYTHONPATH ${CRABPYTHON}:${CRABDBSAPIPYTHON}:${CRABDLSAPIPYTHON}:${CRABPSETPYTHON}
else
setenv PYTHONPATH ${CRABPYTHON}:${PYTHONPATH}:${CRABDBSAPIPYTHON}:${CRABDLSAPIPYTHON}:${CRABPSETPYTHON}
endif

