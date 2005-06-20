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
  echo "The executable not found on WN `hostname`" 
  exit 1 
fi

echo "$executable started at `date`"
#CRAB run_executable
executable_exit_status=$?
echo "$executable ended at `date`"
if [ $executable_exit_status -ne 0 ]; then
  echo "Processing of job failed with exit code $executable_exit_status"
fi
exit_status=$executable_exit_status

#
# END OF PREPARE AND RUN EXECUTABLE
#

#
# PROCESS THE PRODUCED RESULTS
#

#CRAB rename_output

#CRAB register_results

#
# END OF PROCESS THE PRODUCED RESULTS
#

#
# TAIL
#
pwd
echo "ls -Al"
ls -Al

exit $exit_status
