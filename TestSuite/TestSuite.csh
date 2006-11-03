#!/bin/csh
setenv TestSuiteDIR `pwd`
set TestSuitePATH=${TestSuiteDIR}
set TestSuitePYTHON=${TestSuiteDIR}

if ( ! $?path ) then
set path=${TestSuitePATH}
else
set path=( ${TestSuitePATH} ${path} )
endif
if ( ! $?PYTHONPATH ) then
setenv PYTHONPATH ${TestSuitePYTHON}
else
setenv PYTHONPATH ${PYTHONPATH}:${TestSuitePYTHON}
endif
