#!/usr/bin/env python
import sys
import os
#
#check proxy validity
#
ifile,ofile=os.popen4("voms-proxy-info")
sfile=ofile.read().strip()
if sfile=="Couldn't find a valid proxy.":
    sys.exit()
elif  sfile.split("timeleft  :")[1].strip()=="0:00:00":
    sys.exit()
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
            if result == 'Done' and jobidInfo[ states.index('done_code') ] != '0' :
                result = 'Done(failed)'
        except:
            continue
        try:
            reason = jobidInfo[states.index('reason')].replace(" ","-")
        except :
            pass
        try:
            dest_ce = jobidInfo[states.index( 'destination' )].replace("https://", "")
            dest_ce_queue= (dest_ce.split(':')[1]).split("/")[1]
            dest_ce = dest_ce.split(':')[0]
        except :
            pass
        try:
            timestamps = jobidInfo[states.index('stateEnterTimes')].split(' ')
            running = timestamps[5].upper()
            submitted = timestamps[1].upper()
        except :
            pass
        print id,statusMap[result],"SCHED_STATUS="+result,"DEST_CE="+dest_ce,"DEST_QUEUE="+dest_ce_queue,"STATUS_REASON="+reason,submitted,running
        st=st+1
    except:
        continue

