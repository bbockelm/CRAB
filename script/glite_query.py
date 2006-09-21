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
# Add GLITE_WMS_LOCATION to the python path
#
try:
    path = os.environ['GLITE_LOCATION']
except:
    msg = "Error: the GLITE_LOCATION variable is not set."
    sys.exit(1)

libPath=os.path.join(path, "lib")
sys.path.append(libPath)
libPath=os.path.join(path, "lib", "python")
sys.path.append(libPath)

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
           'Done(failed)':'SA'           
           }
#
st=0
for line in sys.stdin:
    id = line.split('\n')[0]
    if len(id) == 0 :
        pass
    try:
        status.getStatus(id,0)
        err, apiMsg = status.get_error()
    except:
        pass
    if err:
        print "cazzo", apiMsg
        pass
    else:
        jobidInfo = status.loadStatus(st)
        result = jobidInfo[states.index('status')]
        if result == 'Done' :
            done_code  = int ( jobidInfo[states.index('done_code')] )
            if done_code != 0 :
                result = 'Done(failed)'
        print id,statusMap[result]
    st=st+1
