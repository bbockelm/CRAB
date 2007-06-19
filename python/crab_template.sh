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
chmod a+x $RUNTIME_AREA/report.py 
$RUNTIME_AREA/report.py $(cat $1)
}

echo "Today is `date`"
echo "Job submitted on host `hostname`" 
uname -a
echo "Working directory `pwd`"
ls -Al
echo "current user is `id`"
echo "voms-proxy-info"
voms-proxy-info -all

repo=jobreport.txt

echo "tar zxvf MLfiles.tgz"
tar zxvf MLfiles.tgz
if [ $? -ne 0 ]; then
    echo "Warning: Failed to untar ML files"
fi    

#
# END OF HEAD
#

#
# SETUP ENVIRONMENT
#

#
# PREPARE AND RUN EXECUTABLE
#

#CRAB setup_scheduler_environment

#CRAB setup_jobtype_environment

#CRAB build_executable

#
# END OF SETUP ENVIRONMENT
#

#
# COPY INPUT
#

#CRAB copy_input 

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

echo "Parse FrameworkJobReport crab_fjr.xml"
# check for crab_fjr.xml in pwd
if [ -s crab_fjr.xml ]; then
    # check for ProdAgentApi in pwd
    if [ -d ProdAgentApi ]; then
	# check for parseCrabFjr.xml in $RUNTIME_AREA
	if [ -s $RUNTIME_AREA/parseCrabFjr.py ]; then
	    cmd_out=`python $RUNTIME_AREA/parseCrabFjr.py --input crab_fjr.xml --MonitorID $MonitorID --MonitorJobID $MonitorJobID`
	    echo "Result of parsing the FrameworkJobReport crab_fjr.xml: $cmd_out"
	    executable_exit_status=`echo $cmd_out | awk -F\; '{print $1}'`
	    echo "Extracted ExitStatus from FrameworkJobReport parsing output: $executable_exit_status"
	else
	    echo "CRAB python script to parse CRAB FrameworkJobReport crab_fjr.xml is not available, using exit code of executable from command line."
	fi
    else
	echo "ProdAgent api to parse CRAB FrameworkJobreport crab_fjr.xml is not available, using exit code of executable from command line."
    fi
else
    echo "CRAB FrameworkJobReport crab_fjr.xml is not available, using exit code of executable from command line."
fi

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
   echo "ExeExitCode=$executable_exit_status" | tee -a $RUNTIME_AREA/$repo
   echo "ExeTime=$TIME_EXE" | tee -a $RUNTIME_AREA/$repo
   echo "JOB_EXIT_STATUS = $executable_exit_status"
   echo "JobExitCode=60302" | tee -a $RUNTIME_AREA/$repo
   dumpStatus $RUNTIME_AREA/$repo
   exit $executable_exit_status
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

### FEDE FOR DBS OUTPUT PUBLICATION
#CRAB modify_report
#######################

echo "JobExitCode=$exit_status" | tee -a $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
rm -f $RUNTIME_AREA/$repo
echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo
echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo

echo "JOB_EXIT_STATUS = $exit_status"
exit $exit_status
