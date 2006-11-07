#!/usr/bin/env python
import sys
import os
#
#check proxy validity
#
# ifile,ofile=os.popen4("voms-proxy-info")
# sfile=ofile.read().strip()
# if sfile=="Couldn't find a valid proxy.":
#     sys.exit()
# elif  sfile.split("timeleft  :")[1].strip()=="0:00:00":
#     sys.exit()
#
# Add GLITE_WMS_LOCATION to the python path
#
try:
    path = os.environ['GLITE_LOCATION']
    libPath=os.path.join(path, "lib")
    sys.path.append(libPath)
    libPath=os.path.join(path, "lib", "python")
    sys.path.append(libPath)
except:
    msg = "Error: the GLITE_LOCATION variable is not set."
#
try:
    path = os.environ['GLITE_WMS_LOCATION']
    libPath=os.path.join(path, "lib")
    sys.path.append(libPath)
    libPath=os.path.join(path, "lib", "python")
    sys.path.append(libPath)
except:
    msg = "Error: the GLITE_WMS_LOCATION variable is not set."
#
from glite_wmsui_LbWrapper import Status
from Job import JobStatus
#
# instatiating status object
status =   Status()
#
# Loading dictionary with available parameters list
states = JobStatus.states
# Defining translation map
statusMap={'Undefined':'UN',
           'Submitted':'SU',
           'Waiting':'SW',
           'Ready':'SR',
           'Scheduled':'SS',
           'Running':'R',
           'Done':'SD',
           'Cleared':'SE',
           'Aborted':'SA',
           'Cancelled':'SK',
           'Unknown':'UN',
           'Done(failed)':'DA'           
           }
#
st=0
for line in sys.stdin:
    try:
        id = line.split('\n')[0]
        if len(id) == 0 :
            continue
        status.getStatus(id,0)
        err, apiMsg = status.get_error()
        if err:
            continue
        result=''
        dest_ce=''
        dest_ce_queue=''
        reason=''
        timestamp=''
        jobidInfo = status.loadStatus(st)
        try:
            result = jobidInfo[states.index('status')]
        except:
            continue
        try:
            reason = jobidInfo[states.index('reason')].replace(" ","-")
            reason = reason.replace("'","''")
        except :
            pass
        try:
            dest_ce = jobidInfo[states.index( 'destination' )].replace("https://", "")
            dest_ce_queue= (dest_ce.split(':')[1]).split("/")[1]
            dest_ce = dest_ce.split(':')[0]
        except :
            pass
        try:
            timestamp = jobidInfo[states.index('stateEnterTimes')]
            pos = timestamp.find(result)
            timestamp = timestamp[timestamp.find('=', pos)+1:timestamp.find(' ', pos)]
        except :
            pass
        try:
            if result == 'Done' and jobidInfo[ states.index('done_code') ] != '0' :
                result = 'Done(failed)'
        except :
            pass
#
        print id,statusMap[result],"SCHED_STATUS="+result,"DEST_CE="+dest_ce,"DEST_QUEUE="+dest_ce_queue,"STATUS_REASON="+reason,"LB_TIMESTAMP="+timestamp
        st=st+1
    except:
        continue

