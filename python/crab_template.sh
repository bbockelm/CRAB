#!/bin/sh
#CRAB title
#
# HEAD
#
#

function cmscp {
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
##      return 2 if file already exists in the SE
###########################
  if [ $# -le 4 ]; then
    echo -e "\t$0 usage:"
    echo -e "\t$0 source <remote SE> <remote SE PATH> <remote output file name> <grid env: LCG(default)|OSG>"
    exit 1
  fi
  path_out_file=$1
  #out_file=$1
  echo "path_out_file = $path_out_file"
  SE=$2
  SE_PATH=$3
  #remoteFile=$4
  name_out_file=$4
  middleware='LCG'
  if [ $# == 5 ]; then
    middleware=$5
  fi

# Set OSG certificates directory
  if [ $middleware == OSG ]; then
    echo "source OSG GRID setup script"
    source $OSG_GRID/setup.sh
  fi

## do the actual copy
  opt=" -report ./srmcp.report -streams_num=1 "
  opt="${opt} -retry_timeout 480000 -retry_num 3 "

  copy_exit_status=1
  #destination=srm://${SE}:8443${SE_PATH}$out_file
  destination=srm://${SE}:8443${SE_PATH}$name_out_file
  echo "destination = $destination"

  echo "--> Check if the file already exists in the storage element $SE"
  srm-get-metadata -retry_num 0 $destination
  if [ $? -eq 0 ]; then
      copy_exit_status=2
      StageOutExitStatusReason='file already exists'
  else
      echo "Starting copy of the output to $SE, middleware is $middleware"
      #cmd="srmcp $opt file:///`pwd`/$out_file $destination"
      cmd="srmcp $opt file:///$path_out_file $destination"
      #full_filename="$out_file"
      #if [ $middleware == OSG ]; then
      #  echo "Copying directly from OSG worker node"
      #  cmd="srmcp $opt file:///$SOFTWARE_DIR/$out_file $destination"
      #  full_filename="$SOFTWARE_DIR/$out_file"
      #fi
      echo $cmd
      exitstring=`$cmd 2>&1`
      copy_exit_status=$?
      if [ $copy_exit_status -eq 0 ]; then
          ## Put into an array the remote file metadata
          remoteMetadata=(`srm-get-metadata -retry_num 0 $destination | grep -v WARNING`)
          remoteSize=`echo ${remoteMetadata[5]}| tr -d :`
          echo "--> remoteSize = $remoteSize"
          ## for local file
          #localSize=$(stat -c%s "$full_filename")
          localSize=$(stat -c%s "$path_out_file")
          echo "-->  localSize = $localSize"
          if [ $localSize != $remoteSize ]; then
              echo "Local fileSize $localSize does not match remote fileSize $remoteSize"
              echo "Copy failed: removing remote file $destination"
              srm-advisory-delete $destination
              copy_exit_status=1
              #echo "Problem copying $source to $destination with srmcp command"
              echo "Problem copying $path_out_file to $destination with srmcp command"
              StageOutExitStatusReason='remote and local file dimension not match'
              echo "StageOutReport = `cat ./srmcp.report`"
          fi
          StageOutExitStatusReason='copy ok with srm utils'
      else
          copy_exit_status=1
          #echo "Problem copying $source to $destination with srmcp command"
          echo "Problem copying $path_out_file to $destination with srmcp command"
          StageOutExitStatusReason=$exitstring
          echo "StageOutReport = `cat ./srmcp.report`"
      fi
  fi

  if [ $copy_exit_status -eq 1 ]; then
      #cmd="lcg-cp --vo $VO -t 2400 --verbose file://`pwd`/$out_file $destination"
      cmd="lcg-cp --vo $VO -t 2400 --verbose file://$path_out_file $destination"
      echo $cmd
      exitstring=`$cmd 2>&1`
      copy_exit_status=$?
      if [ $copy_exit_status -ne 0 ]; then
          #echo "Problem copying $source to $destination with lcg-cp command"
          echo "Problem copying $path_out_file to $destination with lcg-cp command"
          StageOutExitStatusReason=$exitstring
          cmd="echo $StageOutExitStatusReason | grep exists"
          tmpstring=`$cmd 2>&1`
          exit_status=$?
          if [ $exit_status -eq 0 ]; then
              copy_exit_status=2
              StageOutExitStatusReason='file already exists'
          fi
      else
         StageOutExitStatusReason='copy ok with lcg utils'

      fi

  fi

  ########## to use when new lcg-utils will be available and to improve ###########
  #if [ $copy_exit_status -eq 1 ]; then
  #    echo "Try the output copy using lcg-utils"
  #    #/afs/cern.ch/project/gd/egee/glite/ui_PPS_testing/bin/lcg-ls -b -D srmv1 ${destination}${nome_file}
  #    cmd="lcg-ls -b -D srmv1 $destination"
  #    echo $cmd
  #    exitstring=`$cmd 2>&1`
  #    if [ $? -eq 0 ]; then
  #        echo "--> file already exists!!"
  #        copy_exit_status=2
  #    else
  #        cmd="lcg-cp -b -D srmv1 --vo cms file:$out_file $destination"
  #        echo $cmd
  #        exitstring=`$cmd 2>&1`
  #        copy_exit_status=$?
  #        if [ $copy_exit_status -eq 0 ]; then
  #            lcg-ls -b -D srmv1 $destination
  #            if [ $? -eq 0 ]; then
  #                copy_exit_status=0
  #            else
  #                copy_exit_status=1
  #            fi
  #        fi
  #    fi
  #fi
  ##################################################################

  echo "StageOutExitStatus = $copy_exit_status"
  echo "StageOutExitStatusReason = $StageOutExitStatusReason"
  return $copy_exit_status
}

RUNTIME_AREA=`pwd`
dumpStatus() {
echo ">>> info for dashboard:"
echo "***** Cat $1 *****"
cat $1
echo "***** End Cat jobreport *****"
chmod a+x $RUNTIME_AREA/report.py
$RUNTIME_AREA/report.py $(cat $1)
rm -f $1
echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $1
echo "MonitorID=`echo $MonitorID`" | tee -a $1
}

echo "Today is `date`"
echo "Job submitted on host `hostname`"
uname -a
echo ">>> current directory (RUNTIME_AREA): `pwd`"
echo ">>> current directory content:"
ls -Al
echo ">>> current user: `id`"
echo ">>> voms proxy information:"
which voms-proxy-info
voms-proxy-info -all

repo=jobreport.txt

echo ">>> tar zxvf MLfiles.tgz:"
tar zxvf MLfiles.tgz
if [ $? -ne 0 ]; then
    echo "Warning: Failed to untar ML files"
fi

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

echo ">>> Executable $executable"
which $executable
res=$?
if [ $res -ne 0 ];then
  echo "SET_EXE 1 ==> ERROR executable not found on WN `hostname`"
  echo "JOB_EXIT_STATUS = 50110"
  echo "JobExitStatus=50110" | tee -a $RUNTIME_AREA/$repo
  dumpStatus $RUNTIME_AREA/$repo
  exit
fi

echo "SET_EXE 0 ==> ok executable found"

echo "ExeStart=$executable" | tee -a $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
echo ">>> $executable started at `date`"
start_exe_time=`date +%s`
#CRAB run_executable
executable_exit_status=$?
stop_exe_time=`date +%s`

echo ">>> Parse FrameworkJobReport crab_fjr.xml"
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
echo ">>> $executable ended at `date`"

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
# PROCESS THE PRODUCED RESULTS
#

#CRAB rename_output

#CRAB copy_output

#CRAB register_output

echo ">>> current dir: `pwd`"
echo ">>> current dir content:"
ls -Al

#CRAB modify_report

#CRAB check_output_limit

#
# END OF PROCESS THE PRODUCED RESULTS
#

#CRAB clean_env

echo "JobExitCode=$exit_status" | tee -a $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
echo "JOB_EXIT_STATUS = $exit_status"
exit $exit_status
