#!/bin/bash
cd $1
CLAD=$2 

mv crab.cfg crab.orig_cfg
SEDCMD="s/server_mode \+= \+1/server_mode=9999/g; "
SEDCMD=$SEDCMD"s/dont_check_proxy \+= \+0/dont_check_proxy=1/g; "
SEDCMD=$SEDCMD"s/use_central_bossDB \+= \+[0-1]/use_central_bossDB=2/g; "
# SEDCMD=$SEDCMD"?boss_clads?s?^.*$?boss_clads="$CLAD"?g"
SEDCMD=$SEDCMD"/boss_clads/s|^.*$|boss_clads="$CLAD"|g"
# echo $SEDCMD

sed "$SEDCMD" crab.orig_cfg > crab.cfg

if [ $# = 4 ]
then
    mv cmssw.xml cmssw.orig_xml
    SEDCMD="s|"$3"|"$4"|g"
    sed "$SEDCMD" cmssw.orig_xml > cmssw.xml
fi

cd -
