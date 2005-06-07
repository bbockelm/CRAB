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

#CRAB setup_monitoring
#CRAB setup_scheduler_environment
#CRAB setup_jobtype_environment

#
# END OF SETUP ENVIRONMENT
#

#
# COPY INPUT DATA
#

#CRAB copy_input_data

#
# END OF COPY INPUT DATA
#

#
# ADDITIONAL IN-SCRIPT FILES ???
#

#
# END OF ADDITIONAL IN-SCRIPT FILES
#

#
# PREPARE AND RUN EXECUTABLE
#

#CRAB build_executable

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
exit_status=$executable_exit_status

#CRAB stop_monitoring

#
# END OF PREPARE AND RUN EXECUTABLE
#

#
# MOVE OUTPUT
#

#CRAB move_output

#
# END OF MOVE OUTPUT
#

#
# REGISTER RESULTS
#

#CRAB register_results

#
# END OF REGISTER RESULTS
#

#
# TAIL
#
pwd
echo "ls -Al"
ls -Al

#CRAB make_summary

#CRAB notify

exit $exit_status
