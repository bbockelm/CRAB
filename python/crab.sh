#! /bin/sh
# CRAB related Stuff
export CRABDIR=/home_local/fanzago/CRAB_1_0_5
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

# BOSS related Stuff
source /home_local/fanzago/CRAB_1_0_5/Boss/boss-v3_6_3/bossenv.sh

# check whether central boss db is configured

# check if .bossrc dir exists

if [ ! -d ~/.bossrc ]; then
  mkdir ~/.bossrc
fi 

# check if *clad files exist

if [ ! -e ~/.bossrc/BossConfig.clad ]; then
  if [ -e ~/BossConfig.clad ]; then
    cp  ~/BossConfig.clad ~/.bossrc/BossConfig.clad
  else
    echo "User-boss DB not installed: run configureBoss"
  fi
fi
if [ ! -e ~/.bossrc/SQLiteConfig.clad ]; then
  if [ -e ~/SQLiteConfig.clad ]; then
    cp ~/SQLiteConfig.clad ~/.bossrc/SQLiteConfig.clad
  else
    echo "User-boss DB not installed: run configureBoss"
  fi
fi
if [ ! -e ~/.bossrc/MySQLRTConfig.clad ]; then
  if [ -e ~/MySQLRTConfig.clad ]; then
    cp  ~/MySQLRTConfig.clad  ~/.bossrc/MySQLRTConfig.clad
  else
    echo "User-boss DB not installed: run configureBoss"
  fi
fi
# now check a boss command to see if boss DB is up and running
if [ `boss clientID 1>/dev/null | grep -c "not correctly configured"` -ne 0 ]; then
  echo "User-boss DB not installed: run configureBoss"
fi
