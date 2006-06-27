#!/bin/sh
#CRAB title
#
# HEAD
#
#

RUNTIME_AREA=`pwd`
dumpStatus() {
echo ">>>>>>> Cat $1"
cat $1
echo ">>>>>>> End Cat jobreport"
$RUNTIME_AREA/report.py $(cat $1)
}

echo "Today is `date`"
echo "Job submitted on host `hostname`" 
uname -a
echo "Working directory `pwd`"
ls -Al
repo=jobreport.txt

#
# END OF HEAD
#

#
# SETUP ENVIRONMENT
#

#CRAB setup_scheduler_environment

#CRAB setup_jobtype_environment

#
# END OF SETUP ENVIRONMENT
#

#
# COPY INPUT
#

#CRAB copy_input 

#
# PREPARE AND RUN EXECUTABLE
#

#CRAB build_executable

echo "Executable $executable"
which $executable
res=$?
if [ $res -ne 0 ];then 
  echo "SET_EXE 1 ==> ERROR executable not found on WN `hostname`" 
  echo "JOB_EXIT_STATUS = 50110"
  echo "JobExitStatus=50110" | tee -a $RUNTIME_AREA/$repo
  dumpStatus $RUNTIME_AREA/$repo
  rm -f $RUNTIME_AREA/$repo
  echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo
  echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo
  exit 
fi

echo "SET_EXE 0 ==> ok executable found"

echo "ExeStart=$executable" | tee -a $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
rm -f $RUNTIME_AREA/$repo
echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo
echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo
echo "$executable started at `date`"
start_exe_time=`date +%s`
#CRAB run_executable
executable_exit_status=$?
stop_exe_time=`date +%s`
#TIME_EXE=$stop_exe_time - $start_exe_time
let "TIME_EXE = stop_exe_time - start_exe_time"
echo "TIME_EXE = $TIME_EXE sec"
echo "EXECUTABLE_EXIT_STATUS = $executable_exit_status"
echo "ExeEnd=$executable" | tee -a $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
rm -f $RUNTIME_AREA/$repo
echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo
echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo
echo "$executable ended at `date`"
if [ $executable_exit_status -ne 0 ]; then
  echo "Warning: Processing of job failed with exit code $executable_exit_status"
fi
exit_status=$executable_exit_status
echo "ExeExitCode=$exit_status" | tee -a $RUNTIME_AREA/$repo
echo "ExeTime=$TIME_EXE" | tee -a $RUNTIME_AREA/$repo

#
# END OF PREPARE AND RUN EXECUTABLE
#

#
# PROCESS THE PRODUCED RESULTS
#

#CRAB rename_output

#CRAB copy_output 

#CRAB register_output

#
# END OF PROCESS THE PRODUCED RESULTS
#

#
# TAIL
#
pwd
echo "ls -Al"
ls -Al

echo "JobExitCode=$exit_status" | tee -a $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
rm -f $RUNTIME_AREA/$repo
echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo
echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo

echo "JOB_EXIT_STATUS = $exit_status"
exit $exit_status
