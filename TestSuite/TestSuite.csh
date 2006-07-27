#! /bin/csh


setenv RoboDIR `\pwd`
set RoboPATH=${RoboDIR}
set RoboPYTHON=${RoboDIR}

if ( ! $?path ) then
set path=${RoboPATH}
else
set path=( ${RoboPATH} ${path} )
endif
if ( ! $?PYTHONPATH ) then
setenv PYTHONPATH ${RoboPYTHON}
else
setenv PYTHONPATH ${RoboPYTHON}:${PYTHONPATH}
endif
