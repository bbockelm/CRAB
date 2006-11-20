#!/usr/bin/env python
import sys
import os

import warnings
## Get rid of some useless warning
warnings.simplefilter("ignore", RuntimeWarning)
#
# Add EDG_WL_LOCATION to the python path
#
try:
    path = os.environ['EDG_WL_LOCATION']
except:
    msg = "Error: the EDG_WL_LOCATION variable is not set."
    sys.exit(1)
#
libPath=os.path.join(path, "lib")
sys.path.insert(0,libPath)
libPath=os.path.join(path, "lib", "python")
sys.path.append(libPath)
libPath=os.path.join(path, "bin")
sys.path.insert(0,libPath)
#
from edg_wl_userinterface_common_LbWrapper import Status
from Job import JobStatus
#
status = Status()
states = JobStatus.states
#
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
           'Done(failed)':'SA' 
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
        print id,statusMap[result],"SCHED_STATUS="+result,"DEST_CE="+dest_ce,"DEST_QUEUE="+dest_ce_queue,"STATUS_REASON="+reason,"LB_TIMESTAMP="+timestamp
        st=st+1
    except:
        continue

