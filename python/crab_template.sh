#!/bin/sh
#CRAB title
#
# HEAD
#
#

function cmscp {
## SL 17-Aug-2007 Stefano Lacaprara  <lacaprara@pd.infn.it>  INFN Padova
## safe copy of local file in current directory to remote SE via srmcp, including success checking
## input:
##    $1 local file (with respect to current working directory)
##    $2 remote SE
##    $3 remote SE_PATH (absolute)
##    $4 remote file name
##    $5 grid environment: LCG (default) | OSG
## output:
##      return 0 if all ok
##      return 1 if srmcp failed
##      return 2 if copy sucessful but size mismatch
###########################
  max_retry_num=0
  sleep_time=10

  if [ $# -le 4 ]; then
    echo -e "\t$0 usage:"
    echo -e "\t$0 source <remote SE> <remote SE PATH> <remote output file name> <grid env: LCG(default)|OSG>"
    exit 1
  fi 
  file=$1
  SE=$2
  SEPath=$3
  remoteFile=$4
  middleware='LCG'
  if [ $# == 5 ]; then
    middleware=$5
  fi

# Set OSG certificates directory
  if [ $middleware == OSG ]; then
    echo "X509_USER_PROXY = $X509_USER_PROXY"
    echo "source $OSG_APP/glite/setup_glite_ui.sh"
    source $OSG_APP/glite/setup_glite_ui.sh
    export X509_CERT_DIR=$OSG_APP/glite/etc/grid-security/certificates
    echo "export X509_CERT_DIR=$X509_CERT_DIR"
  fi

## do the actual copy
  opt=" -report ./srmcp.report "
  opt="${opt} -retry_timeout 480000"
  if [ $middleware == OSG ]; then
    opt="${opt} -retry_timeout 240000 -x509_user_trusted_certificates"
  fi

  exit_status=0
  retry_num=0
  max_retry_num=0
  while [ $retry_num -le $max_retry_num ]
  do
    let retry_num=retry_num+1
    echo "Attempt #$retry_num"
    source=file:///`pwd`/$file
    destination=srm://${SE}:8443${SE_PATH}$out_file

    cmd="srmcp $opt $source $destination"
    echo $cmd
    exitstring=`$cmd 2>&1`
    copy_exit_status=$?
    if [ $copy_exit_status -ne 0 ]; then
      echo "Problem copying $source to $destination"
      echo "StageOutExitStatus = $copy_exit_status"
      echo "StageOutExitStatusReason = $exitstring"
      echo "StageOutReport = `cat ./srmcp.report`"
      exit_status=1
      continue
    fi

## Put into an array the remote file metadata
    remoteMetadata=(`srm-get-metadata $destination`)
    remoteSize=`echo ${remoteMetadata[5]}| tr -d :`

## ditto for local file
    localFileSize=(`ls -l $file`)
    localSize=${localFileSize[5]}

    if [ $localSize != $remoteSize ]; then
    echo "Local fileSize $localSize does not match remote fileSize $remoteSize"
      echo "Copy failed: removing remote file $destiantion"
      srmrm $destination
      exit_status=2
      continue
    fi

    sleep $sleep_time
  done

  return $exit_status
}

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

#CRAB clean_env 

echo "JobExitCode=$exit_status" | tee -a $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
rm -f $RUNTIME_AREA/$repo
echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo
echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo

echo "JOB_EXIT_STATUS = $exit_status"
exit $exit_status
