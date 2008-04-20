#!/bin/sh
# CRAB title
#
# HEAD
#
#
echo "Running $0 with $# positional parameters: $*"

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
##      return 60307 if srmcp failed
##      return 60303 if file already exists in the SE
###########################
  if [ $# -le 4 ]; then
    echo -e "\t$0 usage:"
    echo -e "\t$0 source <remote SE> <remote SE PATH> <remote output file name> <srm version 1(default)|2> <grid env: LCG(default)|OSG>"
    exit 1
  fi
  path_out_file=$1
  echo "path_out_file = $path_out_file"
  SE=$2
  SE_PATH=$3
  name_out_file=$4
  srm_ver=$5
  middleware='LCG'
  if [ $# == 6 ]; then
    middleware=$6
  fi

# Set OSG certificates directory
  if [ $middleware == OSG ]; then
    echo "source OSG GRID setup script"
    source $OSG_GRID/setup.sh
  fi

## do the actual copy
  opt=" -debug=true -report ./srmcp.report "
  opt="${opt} -retry_timeout 480000 -retry_num 3 "

  destination=srm://${SE}:8443${SE_PATH}$name_out_file
  echo "destination = $destination"

  if [ $srm_ver -eq 1 ] || [ $srm_ver -eq 0 ]; then
      echo "--> Check if the file already exists in the storage element $SE"
      srm-get-metadata -retry_num 0 $destination
      if [ $? -eq 0 ]; then
          #copy_exit_status=60303
          cmscp_exit_status=60303
          StageOutExitStatusReason='file already exists'
      else
          echo "Starting copy of the output to $SE, middleware is $middleware"
          cmd="srmcp $opt -streams_num=1 file:///$path_out_file $destination"
          echo $cmd
          exitstring=`$cmd 2>&1`
          #copy_exit_status=$?
          #if [ $copy_exit_status -eq 0 ]; then
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
                  #copy_exit_status=60307
                  cmscp_exit_status=60307
                  echo "Problem copying $path_out_file to $destination with srmcp command"
                  StageOutExitStatusReason='remote and local file dimension not match'
                  echo "StageOutReport = `cat ./srmcp.report`"
              fi
              StageOutExitStatusReason='copy ok with srm utils'
          else
              #copy_exit_status=60307
              cmscp_exit_status=60307
              echo "Problem copying $path_out_file to $destination with srmcp command"
              StageOutExitStatusReason=$exitstring
              echo "StageOutReport = `cat ./srmcp.report`"
          fi
      fi
  fi

 ################ srmv2 ##########################
 #if [ $copy_exit_status -eq 60307 ]  || [ $srm_ver -eq  2 ] && [ $srm_ver -ne 1 ] ; then
 if [ $cmscp_exit_status -eq 60307 ]  || [ $srm_ver -eq  2 ] && [ $srm_ver -ne 1 ] ; then
      echo "--> Check if the file already exists in the storage element $SE"
      srmls -retry_num 0 $destination | grep 'does not exist' >/dev/null
      if [ $? -eq  0 ]; then
          echo "Starting to copy  the output to $SE using srmv2"
          cmd="srmcp -2 $opt file:///$path_out_file $destination"
          echo $cmd
          exitstring=`$cmd 2>&1`
          #copy_exit_status=$?
          #if [ $copy_exit_status -eq 0 ]; then
          cmscp_exit_status=$?
          if [ $cmscp_exit_status -eq 0 ]; then
              remoteMetadata=(`srmls -retry_num=0 $destination 2>/dev/null`)
              remoteSize=`echo ${remoteMetadata}`
              echo "--> remoteSize = $remoteSize"
              ## for local file
              localSize=$(stat -c%s "$path_out_file")
              echo "-->  localSize = $localSize"
              if [ $localSize != $remoteSize ]; then
                  echo "Local fileSize $localSize does not match remote fileSize $remoteSize"
                  echo "Copy failed: removing remote file $destination"
                      srmrm $destination
                      #copy_exit_status=60307
                      cmscp_exit_status=60307
                      echo "Problem copying $path_out_file to $destination with srmcp command"
                      StageOutExitStatusReason='remote and local file dimension not match'
                      echo "StageOutReport = `cat ./srmcp.report`"
              fi
              StageOutExitStatusReason='copy ok with srm utils'
          else
              #copy_exit_status=60307
              cmscp_exit_status=60307
              echo "Problem copying $path_out_file to $destination with srmcp command"
              StageOutExitStatusReason=$exitstring
              echo "StageOutReport = `cat ./srmcp.report`"
          fi
      else
          #copy_exit_status=60303
          cmscp_exit_status=60303
          StageOutExitStatusReason='file already exists'
      fi
  fi
 ################ lcg-utils ##########################
  #if [ $copy_exit_status -eq 60307 ]; then
  if [ $cmscp_exit_status -eq 60307 ]; then
      cmd="lcg-cp --vo $VO -t 2400 --verbose file://$path_out_file $destination"
      echo $cmd
      exitstring=`$cmd 2>&1`
      #copy_exit_status=$?
      #if [ $copy_exit_status -ne 0 ]; then
      #    copy_exit_status=60307
      cmscp_exit_status=$?
      if [ $cmscp_exit_status -ne 0 ]; then
          cmscp_exit_status=60307
          echo "Problem copying $path_out_file to $destination with lcg-cp command"
          StageOutExitStatusReason=$exitstring
          cmd="echo $StageOutExitStatusReason | grep exists"
          tmpstring=`$cmd 2>&1`
          exit_status=$?
          if [ $exit_status -eq 0 ]; then
              #copy_exit_status=60303
              cmscp_exit_status=60303
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

  #echo "StageOutExitStatus = $copy_exit_status"
  #echo "StageOutExitStatusReason = $StageOutExitStatusReason"
  #return $copy_exit_status
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


### CRAB UPDATE THE FJR WITH WRAPPER_EXIT_CODE ###
update_fjr() {
    if [ ! -s $RUNTIME_AREA/JobReportErrorCode.py ]; then
        echo "WARNING: it is not possible to create crab_fjr.xml to final report"
    else
        echo "PYTHONPATH = $PYTHONPATH"
        chmod a+x $RUNTIME_AREA/JobReportErrorCode.py
        python $RUNTIME_AREA/JobReportErrorCode.py $RUNTIME_AREA/crab_fjr_$NJob.xml $job_exit_code $executable_exit_status
    fi
}
                                            
### REMOVE THE WORKING_DIR IN OSG SITES ###
remove_working_dir() {
    cd $RUNTIME_AREA
    echo "############### WORKING_DIR = $WORKING_DIR #####################"
    echo ">>> current directory (RUNTIME_AREA): $RUNTIME_AREA"
    echo ">>> Remove working directory: $WORKING_DIR"
    /bin/rm -rf $WORKING_DIR
    if [ -d $WORKING_DIR ] ;then
        echo "ERROR ==> OSG $WORKING_DIR could not be deleted on WN `hostname`"
        job_exit_code=10017
    fi
}
                                            
### CRAB EXIT FUNCTION ###
func_exit() {
    if [ $PYTHONPATH ]; then
        update_fjr
    fi
##################
    cd $RUNTIME_AREA    
    for file in $filesToCheck ; do
        if [ -e $file ]; then
            echo tarring file $file in  $out_files
        else
            echo "WARNING: output file $file not found!"
        fi
    done
    if [ $middleware == 'OSG' ]; then
        tar zcvf ${out_files}.tgz $filesToCheck
    else
        tar zcvf ${out_files}.tgz $filesToCheck .BrokerInfo
    fi

#    tmp_size=`ls -gGrta ${outDir}.tgz | awk \'{ print $3 }\'`
#    size=`expr $tmp_size`
#    echo "Total Output dimension: $size";
#    limit=550000   
#    echo "WARNING: output files size limit is set to: $limit";
#    if [ $limit -lt $sum ]; then
#        job_exit_code=70000
#    else
#        echo "Total Output dimension $sum is fine."
#    fi
#    echo "Ending output sandbox limit check"
#################

    if [ $middleware == 'OSG' ] && [ $WORKING_DIR]; then
        remove_working_dir
    fi
    echo "JOB_EXIT_STATUS = $job_exit_code"
    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo
    dumpStatus $RUNTIME_AREA/$repo
    exit $job_exit_code
}

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
stop_exe_time=`date +%s`
echo ">>> $executable ended at `date`"

#### dashboard add timestamp!
echo "ExeEnd=$executable" >> $RUNTIME_AREA/$repo
dumpStatus $RUNTIME_AREA/$repo

let "TIME_EXE = stop_exe_time - start_exe_time"
echo "TIME_EXE = $TIME_EXE sec"
echo "ExeTime=$TIME_EXE" >> $RUNTIME_AREA/$repo

echo ">>> Parse FrameworkJobReport crab_fjr.xml"
#if [ -s crab_fjr.xml ]; then
### FEDE ###
if [ -s $RUNTIME_AREA/crab_fjr_$NJob.xml ]; then
#########################
      if [ -s $RUNTIME_AREA/parseCrabFjr.py ]; then
          #cmd_out=`python $RUNTIME_AREA/parseCrabFjr.py --input crab_fjr.xml --MonitorID $MonitorID --MonitorJobID $MonitorJobID`
          ### FEDE ###
          cmd_out=`python $RUNTIME_AREA/parseCrabFjr.py --input $RUNTIME_AREA/crab_fjr_$NJob.xml --MonitorID $MonitorID --MonitorJobID $MonitorJobID`
          ####################
          echo "Result of parsing the FrameworkJobReport crab_fjr.xml: $cmd_out"
          executable_exit_status=`echo $cmd_out | awk -F\; '{print $1}'`
          if [ $executable_exit_status -eq 50115 ];then
              echo ">>> crab_fjr.xml contents: "
              #cat crab_fjr.xml
              #### FEDE ###### 
              cat $RUNTIME_AREA/crab_fjr_NJob.xml
              ################
              echo "Wrong FrameworkJobReport --> does not contain useful info. ExitStatus: $executable_exit_status"
          else
              echo "Extracted ExitStatus from FrameworkJobReport parsing output: $executable_exit_status"
          fi
      else
          echo "CRAB python script to parse CRAB FrameworkJobReport crab_fjr.xml is not available, using exit code of executable from command line."
      fi
else
    echo "CRAB FrameworkJobReport crab_fjr.xml is not available, using exit code of executable from command line."
fi

echo "ExeExitCode=$executable_exit_status" | tee -a $RUNTIME_AREA/$repo
echo "EXECUTABLE_EXIT_STATUS = $executable_exit_status"
#echo "ExeEnd=$executable" | tee -a $RUNTIME_AREA/$repo
#dumpStatus $RUNTIME_AREA/$repo
#echo ">>> $executable ended at `date`"
job_exit_code=$executable_exit_status

if [ $executable_exit_status -ne 0 ]; then
   echo "ERROR ==> Processing of job failed with exit code $executable_exit_status"
   func_exit
fi


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
