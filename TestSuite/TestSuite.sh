#! /bin/sh
export RoboDIR=`\pwd`
RoboPATH=${RoboDIR}
RoboPYTHON=${RoboDIR}

if [ -z "$PATH" ]; then
export PATH=${RoboPATH}
else
export PATH=${RoboPATH}:${PATH}
fi
if [ -z "$PYTHONPATH" ]; then
export PYTHONPATH=${RoboPYTHON}
else
export PYTHONPATH=${RoboPYTHON}:${PYTHONPATH}
fi
