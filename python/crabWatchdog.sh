#!/bin/bash
#
# watchdog for crab jobs
# needs to be forked out by crab job wrapper at
# the beginning of the job
# this script does:
# 1. log resource usage to a file
# 2. kill crab wrapper childs and signal the wrapper if a child goes out of bound
# 
# if all goes well, crab wrapper will get resource
# usage from the file at the end and report them
# if the wrapper is signaled before, it will be
# wrapper's care to close down, cleanup, report etc.
#

#debug=1  # comment this line, i.e. unset $debug to avoid debug printouts

#
wdLogFile=Watchdog_${NJob}.log

# default limits. see https://twiki.cern.ch/twiki/bin/view/CMSPublic/CompOpsPoliciesNodeProtection
let rssLimit=23*100*1000    #   2.3GB (unit = KB)
let vszLimit=100*1000*1000  # 100GB (unit = KB) = no limit
let diskLimit=19*1000       #  19GB (unit = MB)
let cpuLimit=47*3600+40*60  #  47:40h  (unit = sec)
let wallLimit=47*3600+40*60 #  47:40h  (unit = sec)

# for test purposes allow limits to be lowered

if [ -f ${RUNTIME_AREA}/rssLimit ] ; then
  rssLimitUser=`cat ${RUNTIME_AREA}/rssLimit`
  if [ ${rssLimitUser} -lt ${rssLimit} ] ; then rssLimit=${rssLimitUser}; fi
fi
if [ -f ${RUNTIME_AREA}/vszLimit ] ; then
  vszLimitUser=`cat ${RUNTIME_AREA}/vszLimit`
  if [ ${vszLimitUser} -lt ${vszLimit} ] ; then vszLimit=${vszLimitUser}; fi
fi
if [ -f ${RUNTIME_AREA}/diskLimit ] ; then
  diskLimitUser=`cat ${RUNTIME_AREA}/diskLimit`
  if [ ${diskLimitUser} -lt ${diskLimit} ] ; then diskLimit=${diskLimitUser}; fi
fi
if [ -f ${RUNTIME_AREA}/cpuLimit ] ; then
  cpuLimitUser=`cat ${RUNTIME_AREA}/cpuLimit`
  if [ ${cpuLimitUser} -lt ${cpuLimit} ] ; then cpuLimit=${cpuLimitUser}; fi
fi
if [ -f ${RUNTIME_AREA}/wallLimit ] ; then
  wallLimitUser=`cat ${RUNTIME_AREA}/wallLimit`
  if [ ${wallLimitUser} -lt ${wallLimit} ] ; then wallLimit=${wallLimitUser}; fi
fi

CrabJobID=`ps -o ppid -p $$|tail -1` # id of the parent process

echo "# RESOURCE USAGE SUMMARY FOR PROCESS ID ${CrabJobID}" >  ${wdLogFile}

ps u -ww -p ${CrabJobID} >> ${wdLogFile}

echo " "  >> ${wdLogFile}

echo "# LIMITS USED BY THIS WATCHDOG:" >> ${wdLogFile}
echo "# RSS  (KBytes)  : ${rssLimit}"  >> ${wdLogFile}
echo "# VSZ  (KBytes)  : ${vszLimit}"  >> ${wdLogFile}
echo "# DISK (MBytes)  : ${diskLimit}" >> ${wdLogFile}
echo "# CPU  (seconds) : ${cpuLimit}"  >> ${wdLogFile}
echo "# WALL (seconds) : ${wallLimit}" >> ${wdLogFile}

echo " "  >> ${wdLogFile}

echo "# FOLLOWING PRINTOUT TRACKS MAXIMUM VALUES OF RESOURCES USE" >> ${wdLogFile}
echo "# ONE LINE IS PRINTED EACH TIME A MAXIMUM CHANGES"  >> ${wdLogFile}
echo "# IF CHANGE WAS IN RSS OR VSZ ALSO PROCID AND COMMAND ARE PRINTED"  >> ${wdLogFile}
echo "# THERE IS NO PRINTOUT FOR CPU/WALL TIME INCREASE"  >> ${wdLogFile}
echo " "  >> ${wdLogFile}

#
# find all processes in the group of the CrabWrapper
# and for each process do ps and track the maximum
# usage of resources. hen  max goes over limit i.e...
# when any of the processes in the tree goes out of bound
# send a TERM to all child of the wrapper and
# then signal the wrapper itself so it an close up
# with proper error code

maxRss=0
maxVsz=0
maxDisk=0
maxCpu=0
maxTime=0
maxWall=0

iter=0
nLogLines=0
while true
do
 let residue=${nLogLines}%30    # print a header line every 30 lines
 if [ ${residue} = 0 ]; then
  echo -e "# TIME\t\t\tPID\tRSS(KB)\tVSZ(KB)\tDsk(MB)\ttCPU(s)\ttWALL(s)\tCOMMAND" >>  ${wdLogFile}
 fi
 now=`date +'%b %d %T %Z'`
 processGroupID=`ps -o pgrp -p ${CrabJobID}|tail -1|tr -d ' '`
 processes=`pgrep -g ${processGroupID}`

 for pid in ${processes}
 do
   maxChanged=0
   metrics=`ps --no-headers -o pid,etime,cputime,rss,vsize,args  -ww -p ${pid}`
   if [ $? -ne 0 ] ; then continue ; fi # make sure process is still alive

   [ $debug ] && echo metrics = ${metrics}

   wall=`echo ${metrics}|awk '{print $2}'` # in the form [[dd-]hh:]mm:ss
   
   [ $debug ] && echo wall from PS = ${wall}
   
   # convert to seconds
   [[ ${wall} =~ "-" ]] && wallDays=`echo ${wall}|cut -d- -f1`*86400
   [[ ! ${wall} =~ "-" ]] && wallDays=0
   wallHMS=`echo ${wall}|cut -d- -f2` # works even if there's no -
   longFormat=0     # format mm:ss
   [[ ${wallHMS} =~ ".*:.*:.*" ]] && longFormat=1 # format hh:mm:ss
   if [ ${longFormat} == 1 ] ; then
     # since these will be used by "let" protect against leading 0 indicating octal
     wallSeconds=10\#`echo ${wallHMS}|cut -d: -f3`
     wallMinutes=10\#`echo ${wallHMS}|cut -d: -f2`*60
     wallHours=10\#`echo ${wallHMS}|cut -d: -f1`*3600
   else
     wallSeconds=10\#`echo ${wallHMS}|cut -d: -f2`
     wallMinutes=10\#`echo ${wallHMS}|cut -d: -f1`*60
     wallHours=0
   fi
   let wallTime=$wallSeconds+$wallMinutes+$wallHours+$wallDays

   [ $debug ] && echo wallHMS = ${wallHMS}
   [ $debug ] && echo longFormat = ${longFormat}
   [ $debug ] && echo wallDays-Hours-Minutes-Seconds = ${wallDays}-${wallHours}-${wallMinutes}-${wallSeconds}
   [ $debug ] && echo wallTime = ${wallTime}

   cpu=`echo $metrics|awk '{print $3}'`  # in the form [dd-]hh:mm:ss
   #convert to seconds
   [[ $cpu =~ "-" ]] && cpuDays=`echo $cpu|cut -d- -f1`*86400
   [[ ! $cpu =~ "-" ]] && cpuDays=0
   cpuHMS=`echo $cpu|cut -d- -f2` # works even if there's no -
   cpuSeconds=10\#`echo $cpu|cut -d: -f3`
   cpuMinutes=10\#`echo $cpu|cut -d: -f2`*60
   cpuHours=10\#`echo $cpu|cut -d: -f1`*3600
   let cpuTime=$cpuSeconds+$cpuMinutes+$cpuHours+$cpuDays
   
   rss=`echo $metrics|awk '{print $4}'`

   vsize=`echo $metrics|awk '{print $5}'`

   cmd=`echo $metrics|awk '{print $6" "$7" "$8" "$9" "$10" "$11" "$12" "$13" "$14" "$15}'`

   # track max for the metrics
   if [ $rss -gt $maxRss ]; then maxChanged=1; maxRss=$rss; fi
   if [ $vsize -gt $maxVsz ]; then maxChanged=1; maxVsz=$vsize; fi
   if [ $cpuTime -gt $maxCpu ]; then maxCpu=$cpuTime; fi
   if [ $wallTime -gt $maxWall ]; then maxWall=$wallTime; fi

# only add a line to log when max increases

   if [ ${maxChanged} = 1 ]  ; then 
     echo -e " $now\t$pid\t$maxRss\t$maxVsz\t$maxDisk\t$cpuTime\t$wallTime\t$cmd" >>  ${wdLogFile}
     let nLogLines=${nLogLines}+1
   fi
 done  # end loop on processes in the tree

# now check disk

 disk=`du -sm ${RUNTIME_AREA}|awk '{print $1}'`
 if [ $OSG_WN_TMP ]; then
   disk=`du -sm ${OSG_WN_TMP}|awk '{print $1}'`
 fi

 if [ $disk -gt $maxDisk ]; then
     maxDisk=$disk
     echo -e " $now\t---\t$maxRss\t$maxVsz\t$maxDisk\t----\t----\t----" >>  ${wdLogFile}
     let nLogLines=${nLogLines}+1
 fi

# if we hit a limit, make a note and exit the infinite loop
 if [ $maxRss -gt $rssLimit ] ; then
     exceededResource=RSS
     resVal=$maxRss
     resLim=$rssLimit
     break
 fi
 if [ $maxVsz -gt $vszLimit ] ; then
     exceededResource=VSIZE
     resVal=$maxVsz
     resLim=$vszLimit
     break
 fi
 if [ $maxDisk -gt $diskLimit ] ; then
     exceededResource=DISK
     resVal=$maxDisk
     resLim=$diskLimit
     break
 fi
 if [ $cpuTime -gt $cpuLimit ] ; then
     exceededResource=CPU
     resVal=$maxCpu
     resLim=$cpuLimit
     break
 fi
 if [ $wallTime -gt $wallLimit ] ; then
     exceededResource=WALL
     resVal=$maxWall
     resLim=$wallLimit
     break
 fi

 let iter=${iter}+1
 sleep 60
done # infinite loop watching processes

# reach here if something went out of limit


cat >> ${wdLogFile} <<EOF
 ********************************************************
 * JOB HIT PREDEFINED RESOURCE LIMIT ! PROCESSING HALTED
 *   ${exceededResource} value is ${resVal} while limit is ${resLim}
 ********************************************************
EOF

#
# write a file to communicate CrabWrapper that
# we are killing cmsRun, so it does not process
# fjr etc. before being signaled

echo $exceededResource > ${RUNTIME_AREA}/WATCHDOG-SAYS-EXCEEDED-RESOURCE

# send TERM to all childs of crab wrapper but myself

# traverse the process group in reverse tree order
# and stop when we reach CrabWrapper

processes=`pgrep -g ${processGroupID}`
echo "send pkill TERM  to all CrabWrapper childs in this Process Tree" >> ${wdLogFile}
ps --forest -p ${processes} >>  ${wdLogFile}
procTree=`ps --no-header --forest -o pid -p ${processes}`
revProcTree=`echo ${procTree}|tac -s" "`
for pid in ${revProcTree}
do
  if [ $pid -eq ${CrabJobID} ] ; then break; fi # do not go above crab wrapper
  if [ $pid -eq $$ ] ; then continue; fi # do not kill myself
  procCmd=`ps --no-headers -o args -ww -p ${pid}`
  if [ $? -ne 0 ] ; then
    echo " process PID ${pid} already ended" >> ${wdLogFile}
    continue
  fi
  kill -TERM $pid
  echo " Sent TERM to: PID ${pid} executing: ${procCmd}" >> ${wdLogFile}
done
echo " Wait 30 sec to let processes close up ..." >> ${wdLogFile}
sleep 30
for pid in ${revProcTree}
do
  if [ $pid -eq ${CrabJobID} ] ; then break; fi # do not go above crab wrapper
  if [ $pid -eq $$ ] ; then continue; fi # do not kill myself
  ps ${pid} > /dev/null
  if [ $? -ne 0 ] ; then
    echo " OK. Process ${pid} is gone" >> ${wdLogFile}
  else
    echo -n " Process ${pid} is still there. Send KILL" >> ${wdLogFile}
    kill -KILL $pid
    sleep 10
    echo " Did it die ? do a ps ${pid}" >> ${wdLogFile}
    ps  ${pid}  >> ${wdLogFile}
    if [ $? -ne 0 ] ; then
	echo " Process ${pid} gone at last" >> ${wdLogFile}
    else
	echo -n " Process ${pid} is still there. One last KILL and move on" >> ${wdLogFile}
	kill -KILL $pid
    fi
  fi
done

echo "Finally gently signal CrabWrapper with USR2" >> ${wdLogFile}
kill -USR2 ${CrabJobID}

echo "Process cleanup completed, crabWatchdog.sh exiting" >> ${wdLogFile}

