#!/usr/bin/env python2.2
import sys
import os
# Add GLITE_WMS_LOCATION to the python path

try:
    path = os.environ['GLITE_LOCATION']
except:
    print "Error: the GLITE_LOCATION variable is not set."
    sys.exit(1)

libPath=os.path.join(path, "lib")
sys.path.append(libPath)
libPath=os.path.join(path, "lib", "python")
sys.path.append(libPath)

from glite_wmsui_LbWrapper import Status

states= [ "Acl", "cancelReason", "cancelling","ce_node","children", \
          "children_hist","children_num","children_states","condorId","condor_jdl", \
          "cpuTime","destination", "done_code","exit_code","expectFrom", \
          "expectUpdate","globusId","jdl","jobId","jobtype", \
          "lastUpdateTime","localId","location", "matched_jdl","network_server", \
          "owner","parent_job", \
          "Payload_Running", "Possible_Ce_Nodes","Possible Destinations",\
          "reason","resubmitted","rsl","seed","stateEnterTime",\
          "stateEnterTimes","subjob_failed","user tags" , "status" , "status_code","hierarchy"]

status =   Status()
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
           'Unknown':'UN'
           }

st=0
#for line in sys.stdin:
#    id = line.split('\n')[0]
id = sys.argv[1]
if len(id) == 0 :
    pass
status.getStatus(id,0)
err, apiMsg = status.get_error()
hstates = {}
if err:
    pass
    print "errore ", apiMsg
else:
    for i in range(len(states)):
        hstates[ states[i] ] = status.loadStatus(st)[i]
    result = status.loadStatus(st)[ states.index('status') ]
    if result == 'Done' :
        done_code  = status.loadStatus(st)[ states.index("done_code") ]
        done_code  = int ( done_code )
        if done_code != 0 :
            result = 'Aborted'
    print id," ",statusMap[result]
print id," ",statusMap[result]
st=st+1
