#!/bin/sh

#CRAB title

#
# HEAD
#
#
echo "Running $0 with $# positional parameters: $*"


function createSePath {
#######
## Only for rfio protocol: function called by verifySePath
## to create a new directory
## input: 
## $1 = dir path to create
## output:
##  0 if ok
##  60316 if problem with dir creation
#######
  dir=$1
  rfmkdir -p $dir
      if [ $? -eq 0 ]; then
          echo "the $dir has been created \n"
          exit_creation_dir=0
      else
          echo ">>> ERROR: problem with the $dir creation using rfmkdir \n"
          exit_creation_dir=60316
          return $exit_creation_dir
      fi       
}

function verifySePath {
#### --->> To improve error management
#######
## Only for rfio protocol: function to check if the directory where to copy produced output exists in the SE
## input: 
## $1 = se_path
## $1 = se_path_add in case of publication
## output:
##  0 if ok
##  60316 if problem with dir creation
#######
  se_path=$1
  exit_verifySePath=0
  
  rfdir $se_path
  if [ $? -eq 0 ]; then
      echo "the se_path $se_path exists \n"
  else
      createSePath $se_path
      if [ $exit_creation_dir -ne 0 ]; then
         exit_verifySePath=$exit_creation_dir
         return $exit_verifySePath
      fi   
  fi
  return $exit_verifySePath
}

function cmscp {
######
## safe copy of local file in current directory to remote SE via srmcp, including success checking
## version also for CAF using rfcp command to copy the output to SE
## input:
##    $1 middleware (CAF, LSF, LCG, OSG)
##    $2 local file (the physical path of output file respect to current working directory)
##    $3 file name (the output file name)
##    $4 remote SE_PATH (absolute)
##    $5 remote SE
##    $6 srm version (only in the case of LCG or OSG)
## output:
##      return 0 if all ok
##      return 60307 if srmcp failed
##      return 60303 if file already exists in the SE
######

  middleware=$1
  
  if [ $middleware == 'CAF' ] || [ $middleware == 'LSF' ]; then
      if [ $# -le 3 ]; then
        echo -e "\t$0 usage:"
        echo -e "\t$0 source <middleware> <output_file_path> <output_file_name> <remote_se_path>"
        exit 1
      fi
      
      path_out_file=$2
      name_out_file=$3
      echo "path_out_file = $path_out_file"
      SE_PATH=$4
    
      cmd="rfcp $path_out_file ${SE_PATH}/$name_out_file"
      echo $cmd
      exitstring=`$cmd 2>&1`
      cmscp_exit_status=$?
      if [ $cmscp_exit_status -ne 0 ]; then
          cmscp_exit_status=60307
          echo "Problem copying $path_out_file to $destination with rfcp command"
          echo "Error message:    $exitstring "
          StageOutExitStatusReason=$exitstring
      else
          StageOutExitStatusReason='copy ok with rfcp'
      fi
      cmscp_exit_status=$copy_exit_status
      
  else
      if [ $# -le 5 ]; then
          echo -e "\t$0 usage:"
          echo -e "\t$0 source <grid env: LCG(default)|OSG> <remote SE> <output_file_path> <output_file_name> <remote_se_path>> <remote_se> <srm version 1(default)|2> "
          exit 1
      fi
 
      path_out_file=$2
      echo "path_out_file = $path_out_file"
      SE=$3
      SE_PATH=$4
      name_out_file=$5
      srm_ver=$6

      # Set OSG certificates directory
      if [ $middleware == OSG ]; then
          echo "source OSG GRID setup script"
          source $OSG_GRID/setup.sh
      fi

      ## do the actual copy
      destination=srm://${SE}:8443${SE_PATH}$name_out_file
      echo "destination = $destination"

      ################ lcg-utils ##########################
      if [ $srm_ver -eq  1 ] ; then
          lcgOpt=" -b -D srmv1 --vo $VO -t 2400 --verbose "
      else
          Dsrm_ver="2"
          lcgOpt=" -b -D srmv2 --vo $VO -t 2400 --verbose "
      fi
      echo "lcg-cp --version "
      lcg-cp --version
      echo "---------------- "
      cmd="lcg-cp $lcgOpt file://$path_out_file $destination"
      echo $cmd
      exitstring=`$cmd 2>&1`
      cmscp_exit_status=$?
      if [ $cmscp_exit_status -ne 0 ]; then
          cmscp_exit_status=60307
          echo "Problem copying $path_out_file to $destination with lcg-cp command"
          echo "Error message:    $exitstring "
          StageOutExitStatusReason=$exitstring
          cmd="lcg-ls -D srmv${Dsrm_ver} $destination"
          tmpstring=`$cmd 2>&1`
          exit_status=$?
          if [ $exit_status -eq 0 ]; then
              echo $tmpstring | grep -v 'No such file' 
              exit_status=$?
              if [ $exit_status -eq 0 ]; then
                  cmscp_exit_status=60303
                  StageOutExitStatusReason='file already exists'
              fi
          fi
      else
          StageOutExitStatusReason='copy ok with lcg utils'
      fi

      ############# now try srm utils ##############
      if [ $cmscp_exit_status -ne 0 ]; then
          opt=" -debug=true -report ./srmcp.report "
          opt="${opt} -retry_timeout 480000 -retry_num 3 "

          ################ srmv2 ##########################
          if [ $srm_ver -eq  2 ] || [ $srm_ver -eq 0 ]; then
              unset SRM_PATH
              echo "--> Check if the file already exists in the storage element $SE, using SRM2"
              srmls -retry_num 0 $destination | grep 'does not exist' >/dev/null
              if [ $? -eq  0 ]; then
                  echo "Starting to copy  the output to $SE using srmv2"
                  cmd="srmcp -srm_protocol_version 2  $opt file:///$path_out_file $destination"
                  echo $cmd
                  exitstring=`$cmd 2>&1`
                  cmscp_exit_status=$?
                  if [ $cmscp_exit_status -eq 0 ]; then
                      remoteMetadata=(`srmls -retry_num=0 $destination | grep -v WARNING 2>/dev/null`)
                      remoteSize=`echo ${remoteMetadata}`
                      echo "--> remoteSize = $remoteSize"
                      ## for local file
                      localSize=$(stat -c%s "$path_out_file")
                      echo "-->  localSize = $localSize"
                      if [ $localSize != $remoteSize ]; then
                          echo "Local fileSize $localSize does not match remote fileSize $remoteSize"
                          echo "Copy failed: removing remote file $destination"
                              srmrm $destination
                              cmscp_exit_status=60307
                              echo "Problem copying $path_out_file to $destination with srmcp command"
                              StageOutExitStatusReason='remote and local file dimension not match'
                              echo "StageOutReport = `cat ./srmcp.report`"
                      fi
                      StageOutExitStatusReason='copy ok with srm utils'
                  else
                      cmscp_exit_status=60307
                      echo "Problem copying $path_out_file to $destination with srmcp command"
                      StageOutExitStatusReason=$exitstring
                      echo "StageOutReport = `cat ./srmcp.report`"
                  fi
              else
                  cmscp_exit_status=60303
                  StageOutExitStatusReason='file already exists'
              fi
          fi

          if [ $srm_ver -eq 1 ] ; then
              echo "--> Check if the file already exists in the storage element $SE, using SRM1"
              srm-get-metadata -retry_num 0 $destination
              if [ $? -eq 0 ]; then
                  cmscp_exit_status=60303
                  StageOutExitStatusReason='file already exists'
              else
                  echo "Starting copy of the output to $SE, middleware is $middleware"
                  cmd="srmcp $opt -streams_num=1 file:///$path_out_file $destination"
                  echo $cmd
                  exitstring=`$cmd 2>&1`
                  cmscp_exit_status=$?
                  if [ $cmscp_exit_status -eq 0 ]; then
                      ## Put into an array the remote file metadata
                      remoteMetadata=(`srm-get-metadata -retry_num 0 $destination | grep -v WARNING`)
                      remoteSize=`echo ${remoteMetadata[5]}| tr -d :`
                      echo "--> remoteSize = $remoteSize"
                      ## for local file
                      localSize=$(stat -c%s "$path_out_file")
                      echo "-->  localSize = $localSize"
                      if [ $localSize != $remoteSize ]; then
                          echo "Local fileSize $localSize does not match remote fileSize $remoteSize"
                          echo "Copy failed: removing remote file $destination"
                          srm-advisory-delete $destination
                          cmscp_exit_status=60307
                          echo "Problem copying $path_out_file to $destination with srmcp command"
                          StageOutExitStatusReason='remote and local file dimension not match'
                          echo "StageOutReport = `cat ./srmcp.report`"
                      fi
                      StageOutExitStatusReason='copy ok with srm utils'
                  else
                      cmscp_exit_status=60307
                      echo "Problem copying $path_out_file to $destination with srmcp command"
                      StageOutExitStatusReason=$exitstring
                      echo "StageOutReport = `cat ./srmcp.report`"
                  fi
              fi
          fi
      fi
  fi

  echo "StageOutExitStatus = $cmscp_exit_status" | tee -a $RUNTIME_AREA/$repo
  echo "StageOutExitStatusReason = $StageOutExitStatusReason" | tee -a $RUNTIME_AREA/$repo
  echo "StageOutSE = $SE" >> $RUNTIME_AREA/$repo\n
  return $cmscp_exit_status
}

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
  job_exit_code=50110
  func_exit
else
  echo "ok executable found"
fi

#echo "SET_EXE 0 ==> ok executable found"

echo "ExeStart=$executable" >>  $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo
#cat  pset.py
echo ">>> $executable started at `date`"
start_exe_time=`date +%s`
#CRAB run_executable
executable_exit_status=$?
CRAB_EXE_CPU_TIME=`tail -n 1 cpu_timing.txt`
stop_exe_time=`date +%s`
echo ">>> $executable ended at `date`"

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
