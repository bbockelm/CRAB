#!/bin/sh

#CRAB title

#
# HEAD
#
#
echo "Running $0 with $# positional parameters: $*"

dumpStatus() {
    echo ">>> info for dashboard:"
    echo "***** Cat $1 *****"
    cat $1
    echo "***** End Cat jobreport *****"
    chmod a+x $RUNTIME_AREA/report.py

    $RUNTIME_AREA/report.py $(cat $1)
    rm -f $1
    echo "MonitorJobID=`echo $MonitorJobID`" > $1
    echo "MonitorID=`echo $MonitorID`" >> $1
}


### REMOVE THE WORKING_DIR IN OSG SITES ###
remove_working_dir() {
    cd $RUNTIME_AREA
    echo ">>> working dir = $WORKING_DIR"
    echo ">>> current directory (RUNTIME_AREA): $RUNTIME_AREA"
    echo ">>> Remove working directory: $WORKING_DIR"
    /bin/rm -rf $WORKING_DIR
    if [ -d $WORKING_DIR ] ;then
        echo "ERROR ==> OSG $WORKING_DIR could not be deleted on WN `hostname`"
        job_exit_code=10017
    fi
}

#CRAB func_exit


#CRAB initial_environment

RUNTIME_AREA=`pwd`
export RUNTIME_AREA

echo "Today is `date`"
echo "Job submitted on host `hostname`"
uname -a
echo ">>> current directory (RUNTIME_AREA): `pwd`"
echo ">>> current directory content:"
ls -Al
echo ">>> current user: `id`"
echo ">>> voms proxy information:"
voms-proxy-info -all

repo=jobreport.txt


#CRAB untar_software

#
# SETUP ENVIRONMENT
#

#CRAB setup_scheduler_environment

#CRAB setup_jobtype_environment

#
# END OF SETUP ENVIRONMENT
#
#
# PREPARE AND RUN EXECUTABLE
#

#CRAB build_executable


#
# END OF PREPARE AND RUN EXECUTABLE
#

#
# COPY INPUT
#

#CRAB copy_input

#
# Rewrite cfg or cfgpy file
#

#CRAB rewrite_cmssw_cfg

echo ">>> Executable $executable"
which $executable
res=$?
if [ $res -ne 0 ];then
#  echo "SET_EXE 1 ==> ERROR executable not found on WN `hostname`"
#  echo "JOB_EXIT_STATUS = 50110"
#  echo "JobExitStatus=50110" | tee -a $RUNTIME_AREA/$repo
#  dumpStatus $RUNTIME_AREA/$repo
#  exit

  echo "ERROR ==> executable not found on WN `hostname`"
  job_exit_code=10035
  func_exit
else
  echo "ok executable found"
fi

#echo "SET_EXE 0 ==> ok executable found"

echo "ExeStart=$executable" >>  $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
#cat  pset.py
echo ">>> $executable started at `date -u`"
start_exe_time=`date +%s`
#CRAB run_executable
executable_exit_status=$?
CPU_INFOS=`tail -n 1 cpu_timing.txt`
stop_exe_time=`date +%s`
echo ">>> $executable ended at `date -u`"

#### dashboard add timestamp!
echo "ExeEnd=$executable" >> $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo

let "TIME_EXE = stop_exe_time - start_exe_time"
echo "TIME_EXE = $TIME_EXE sec"
echo "ExeTime=$TIME_EXE" >> $RUNTIME_AREA/$repo


#CRAB parse_report


#
# PROCESS THE PRODUCED RESULTS
#

#CRAB rename_output

#CRAB copy_output

echo ">>> current dir: `pwd`"
echo ">>> current dir content:"
ls -Al

#CRAB modify_report

#
# END OF PROCESS THE PRODUCED RESULTS
#

#CRAB clean_env

func_exit
