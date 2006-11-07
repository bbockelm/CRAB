#!/bin/sh
export TestSuiteDIR=$CRABDIR/TestSuite
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
export PYTHONPATH=${PYTHONPATH}:${TestSuitePYTHON}
fi
