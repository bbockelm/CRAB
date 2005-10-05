#!/bin/sh
#CRAB title
#
# HEAD
#
#
RUNTIME_AREA=`pwd`

echo "Today is `date`"
echo "Job submitted on host `hostname`" 
uname -a
echo "Working directory `pwd`"
ls -Al
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
# PREPARE AND RUN EXECUTABLE
#

#CRAB build_executable

echo "Executable $executable"
which $executable
res=$?
if [ $res -ne 0 ];then 
  echo "SET_EXE 1 ==> ERROR executable not found on WN `hostname`" 
  exit 1 
fi

echo "SET_EXE 0 ==> ok executable found"

echo "$executable started at `date`"
start_exe_time=`date +%s`
#CRAB run_executable
executable_exit_status=$?
stop_exe_time=`date +%s`
#TIME_EXE=$stop_exe_time - $start_exe_time
let "TIME_EXE = stop_exe_time - start_exe_time"
echo "TIME_EXE = $TIME_EXE sec"
echo "EXECUTABLE_EXIT_STATUS = $executable_exit_status"
echo "$executable ended at `date`"
if [ $executable_exit_status -ne 0 ]; then
  echo "Warning: Processing of job failed with exit code $executable_exit_status"
fi
exit_status=$executable_exit_status

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

echo "JOB_EXIT_STATUS = $exit_status"
exit $exit_status
