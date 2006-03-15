#! /bin/csh
# CRAB related Stuff
setenv CRABDIR /home_local/fanzago/CRAB_1_0_5
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

# BOSS related Stuff
source /home_local/fanzago/CRAB_1_0_5/Boss/boss-v3_6_3/bossenv.csh

# check whether central boss db is configured

# check if .bossrc dir exists

if ( ! -d ~/.bossrc ) then
  mkdir ~/.bossrc
endif 

# check if *clad files exist

if ( ! -e ~/.bossrc/BossConfig.clad ) then
  if ( -e ~/BossConfig.clad ) then
    cp ~/BossConfig.clad ~/.bossrc/BossConfig.clad
  else
    echo "User-boss DB not installed: run configureBoss"
    exit 1
  endif
endif
if ( ! -e ~/.bossrc/SQLiteConfig.clad ) then
  if ( -e ~/SQLiteConfig.clad ) then
    cp ~/SQLiteConfig.clad ~/.bossrc/SQLiteConfig.clad
  else
    echo "User-boss DB not installed: run configureBoss"
    exit 1
  endif
endif
if ( ! -e ~/.bossrc/MySQLRTConfig.clad ) then
  if ( -e ~/MySQLRTConfig.clad ) then
    cp ~/MySQLRTConfig.clad  ~/.bossrc/MySQLRTConfig.clad
  else
    echo "User-boss DB not installed: run configureBoss"
    exit 1
  endif
endif
# now check a boss command to see if boss DB is up and running
if ( `boss clientID |& grep -c "not correctly configured"` ) then
  echo "User-boss DB not installed: run configureBoss"
  exit 1
endif
