
from CRAB_Server_API import CRAB_Server_Session
print "Correct import"
asSession = CRAB_Server_Session('lxplus223.cern.ch', 2181)
print "Successful allocation"

ret = asSession.transferTaskAndSubmit('taskXML', 'cmdXML', 'taskUniqName')
print "call transferTaskAndSubmit: ", ret

ret = asSession.sendCommand('cmdXML', 'taskUniqName')
print "call sendCommand: ", ret

ret = asSession.getTaskStatus('taskUniqName')
print "call getTaskStatus: ", ret

