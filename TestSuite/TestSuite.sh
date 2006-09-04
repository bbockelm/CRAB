#!/bin/sh
export TestSuiteDIR=`pwd`
TestSuitePATH=${TestSuiteDIR}
TestSuitePYTHON=${TestSuiteDIR}

if [ -z "$PATH" ]; then
export PATH=${TestSuitePATH}
else
export PATH=${TestSuitePATH}:${PATH}
fi
if [ -z "$PYTHONPATH" ]; then
export PYTHONPATH=${TestSuitePYTHON}
else
export PYTHONPATH=${TestSuitePYTHON}:${PYTHONPATH}
fi
