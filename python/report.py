#!/usr/bin/python

"""
This is a simple test program for the 'apmon' Python module.

"""

import apmon
import time, sys, os

apmonInstance = None
def getApmonInstance() :
    global apmonInstance
    if apmonInstance is None :
        #apmonUrl = 'http://monalisa.cern.ch/ARDA/apmon.cms'
	apmonUrl = ('192.91.245.5:58884',)
        logger("ApmReport: Creating ApMon with " + `apmonUrl`)
        apmonInstance = apmon.ApMon(apmonUrl)
    return apmonInstance

def getParamValues(lines, retDict=None) :
    readAll = False
    if retDict is None :
        retDict = {}
        readAll = True
    elif retDict == {} :
        readAll = True
    for line in lines :
        line = line.strip()
        if line.find('=') != -1 :
            split = line.split('=')
            if len(split) > 2 :
                split = [split[0], '='.join(split[1:])]
            split = [x.strip() for x in split]
            if readAll or split[0] in retDict.keys() :
                retDict[split[0]] = split[1]
    return retDict

def readFileContent(fileName) :
    lines = None
    if os.path.exists(fileName) :
        fh = open(fileName, 'r')
        lines = fh.readlines()
        fh.close()
    return lines

def readParamValues(fileNameArr, retDict=None) :
    for fileName in fileNameArr :
        lines = readFileContent(fileName)
        if lines is not None :
            return getParamValues(lines, retDict)
    return retDict

def readTaskIdFile(filename='TASK_ID') :
    taskId = None
    taskIdFile = readFileContent(filename)
    if taskIdFile is not None and len(taskIdFile) > 0 :
        taskId = taskIdFile[0].strip()
    return taskId

def logger(msg) :
    msg = `msg`
    if not msg.endswith('\n') :
        msg = msg + '\n'
    fh = open('report.log','a')
    fh.write(msg)
    fh.close
    #print msg

def getJobID(id, jobId='UNKNOWN') :
    if id.find('.') != -1 :
        split = id.split('.')
        if split[0].find('_') :
            jobId = split[0].split('_')[-1]
        elif len(split[0]) >= 6 : 
            jobId = split[0][-6:]
        else :
            jobId = split[0]
    return jobId

def mlSend(task, job, paramDict) :
    apm = getApmonInstance()
    #logger("ApmReport: Destinations:", apm.destinations) 
    logger("ApmReport: Sending:"+`task`+":"+`job`+":"+`paramDict`)
    apm.sendParameters(task, job, paramDict)

##
## MAIN PROGRAM
##
if __name__ == '__main__' :
    # Default file names and initial values
    mlConfigFileArr = ['MonalisaParameters', '.orcarc']
#    mlDefaultDict = { 'SyncGridName' : os.popen("grid-proxy-info -identity") ,
#                      'SyncGridJobId' : os.environ['EDG_WL_JOBID'] }
    mlDefaultDict = { 'SyncGridName' : 'gigio', 
                      'SyncGridJobId' : 'pippo' }

    args = sys.argv[1:]

    # Read values from file(s) and cmdline
    mlDict = readParamValues(mlConfigFileArr, mlDefaultDict)
    paramDict = getParamValues(args)
    if len(paramDict) == 0 :
        sys.exit(0)
    # Report
    mlSend(mlDict['SyncGridName'], mlDict['SyncGridJobId'], paramDict)

    # Exit
    sys.exit(0)
