#!/usr/bin/env python

import logging 
import os
import os.path
import shutil
import urllib
import traceback

from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Core.GlobalRegistry import registerHandler
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.FwkJobRep.ReportParser import readJobReport

from ProdAgent.WorkflowEntities import JobState

from ErrorHandler.DirSize import dirSize
from ErrorHandler.DirSize import convertSize
from ErrorHandler.Handlers.HandlerInterface import HandlerInterface
from ErrorHandler.TimeConvert import convertSeconds

# BossLite import
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session


class CrabRunFailureHandler(HandlerInterface):
    """
    _CrabRunFailureHandler_

    Handles job run failures. Called by the error handler if a job run failure 
    event is received. We distinguish two classes of failures. A failure 
    that happens during a submission (job could not run), or a failure 
    during running of the job.  the payload for RunFailure is a url to 
    the job report.

    Based on the job report, we can retrieve the job id and use
    that to retrieve the job type in the database. 

    Processing error handler that either generates a new submit event
    or cleans out the job information if the maximum number of retries
    has been reached (and generates a general failure event).

    Using this information we propagate it to different job error handlers 
    associated to different job types, for further processing.

    """

    def __init__(self):
        HandlerInterface.__init__(self)
        self.args={}
        self.blDBsession = None

        # Here we define the error matrix
        self.ErrorMatrix = {}
        # print os.getcwd()
        self.localEMFileName = "ErrorHandlerMatrix.txt"

        ### TEMPORARY
        self.url = "https://cmsweb.cern.ch/crabconf/files/"
        self.initializeActions()
        self.cacheLocation = '' # where the cached ErrorMatrix will be put

    def NoResubmission(self):
        " No resubmission: do nothing "
        from ProdAgent.WorkflowEntities.JobState import doNotAllowMoreSubmissions
        try:
            doNotAllowMoreSubmissions([self.JobSpecId])
            ## logger human readble message
            textmsg = "Do not allowing more resubmission %s"%self.JobSpecId
        except ProdAgentException, ex:
            msg = "Updating max racers fields failed for job %s\n" % self.jobId
            logging.error(msg)
        return textmsg

    def DelayedResubmission(self):
        " resubmit with a delay of X seconds "
        delay = 120 # seconds
        delay = convertSeconds(delay) 
        logging.info(">CrabRunFailureHandler<: re-submitting with delay (h:m:s) "+ str(delay))
        payload = str(self.taskId)+'::'+str(self.jobId) 
        logging.info("--->>> payload = " + payload)         
        self.publishEvent("ResubmitJob",payload,delay)
        ## logger human readble message
        return "Resubmitting with (h:m:s) %s delay" %(str(delay))

    def ResubmitElsewhere(self):
        " resubmit blacklisting the latest CE used "
        task = self.bliteSession.load(self.taskId,self.jobId)
        ce = str(task.jobs[0].runningJob['destination'])
        ce_temp = ce.split(':')

        # fix to avoid empty ce names # Fabio
        if len(ce_temp[0])>0:
            ce_name = ce_temp[0] 
        else:
            ce_name = "#fake_ce#"

        logging.info("--->>> ce = " + str(ce))
        logging.info("--->>> ce_name = " + str(ce_name))
        logging.info(">CrabRunFailureHandler<: re-submitting banning the ce "+ str(ce_name))
        payload = str(self.taskId) + '::' + str(self.jobId) + '::' + str(ce_name)  
        logging.info("--->>> payload = " + payload)         
        self.publishEvent("ResubmitJob",payload)
        ## logger human readble message
        return "Resubmitting banning [%s] ce" %(str(ce_name))

    def updateLocalFile(self):
        """
        if the local file is older than 24 ours (or does not exist): return True
        if not, return False
        """
        import os

        if (not os.path.exists(self.localEMFileNameWithPath)): return True
        statinfo = os.stat(self.localEMFileNameWithPath)
        ## if the file is older then 12 hours it is re-downloaded to update the configuration
        #oldness = 120
        oldness = 24*3600
        import time
        if (time.time() - statinfo.st_ctime) > oldness:
            return True
        return False

    def downloadErrorMatrix(self):
        """
        Download the error matrix definition and overwrite the local cache
        """
        logging.info("Downloading %s to %s  "%((self.url+self.localEMFileName),self.cacheLocation))
        import urllib
        EMFile = urllib.urlopen(self.url+self.localEMFileName)
        localEMFile = open(self.localEMFileNameWithPath, 'w')
        localEMFile.write(EMFile.read())
        localEMFile.close()
        EMFile.close()

    def initializeActions(self):
        """ 
        Define possible actions 
        """
        # define matrix of possible actions
        self.Actions={
            0 : self.NoResubmission ,
            1 : self.DelayedResubmission,
            2 : self.ResubmitElsewhere
        }

        # the default action is Do not resubmit
        self.defaultAction = self.NoResubmission

    def defineErrorMatrix(self):
        """
        Here define the error matrix: a dictionary of dictionaly, in the following format:
        ErrorMatrix = { ExecutableExitStatus : { WrapperExitStatus : Action } }
        """
        self.cacheLocation = str(self.args['ComponentDir'])
        self.localEMFileNameWithPath = self.cacheLocation+"/"+self.localEMFileName

        # get (if needed) error matrix definition from web
        if (not self.updateLocalFile()): return

        self.downloadErrorMatrix()

        # now open ErrorMatrix file
        EMFile = open(self.localEMFileNameWithPath, 'r')

        # read file and fill erro matrix
        for line in EMFile.readlines():
            line = line.strip()
            # Skip comments
            if len(line)==0 or (line[0]=="#"): continue
            elif (len(line.split())==3):
                ExeExitCode, WrapperExitCode, Action = line.split()
                if int(Action) not in self.Actions.keys():
                    logging.info("Action %s. unknown "%Action)
                    continue
                    
                # for new key, define dict
                if not self.ErrorMatrix.has_key(ExeExitCode):
                    self.ErrorMatrix[ExeExitCode] = {}

                self.ErrorMatrix[ExeExitCode][WrapperExitCode]=self.Actions[int(Action)]
            else:
                logging.info("Wrong syntax %s. "%line)
            pass
        logging.info("ErrorMatrix Loaded %s"%(str(self.ErrorMatrix)))

        EMFile.close()

    def infoLogger(self, taskname, jobid, diction):
        """
        _infoLogger_
        Send the default message to log the information in the job logging info
        """
        from IMProv.IMProvAddEntry import Event
        eve = Event( )
        eve.initialize( diction ) 
        import time
        unifilename = os.path.join(self.args['jobReportLocation'], str(time.time())+".pkl")
        eve.dump( unifilename )
        message = "TTXmlLogging"
        payload  = taskname + "::" +str(jobid) + "::" + unifilename
        logging.info("Sending %s."%message)
        self.publishEvent( message, payload )
        logging.info("Registering information:\n%s"%str(diction))

    def handleError(self,payload):
        """
        The payload of a job failure is a url to the job report
        """
        # logging.info("Args %s "%(str(self.args)))
        # logging.info("Params %s "%(str(self.parameters)))

        # Check if cached matrix is fresh enough and get it if not
        self.defineErrorMatrix()

        ## dictionary where to put the information to logs
        loggindict = {}
        textmsg = "No action defined"

        self.bliteSession = BossLiteAPI('MySQL', dbConfig)
        
        logging.info(">CrabRunFailureHandler<:payload %s " % payload)
        jobReportUrl = payload
         
        import re
        r = re.compile("BossJob_(\d+)_(\d+)/")
        m= r.search(payload)
        if (m):
            self.taskId,self.jobId=m.groups()
        else:
            logging.info(">CrabRunFailureHandler<:Cannot parse payload! %s " % payload)
            return
        for x in payload.split('/'):
            if str(x).find('_spec') > 0: temp_specId=x
        self.JobSpecId = temp_specId.replace('_spec','_job%s'%self.jobId)

        #### parse payload to obtain taskId and jobId ####
        logging.info("--->>> taskId = " + str(self.taskId))
        logging.info("--->>> jobId = " + str(self.jobId))        
        
        task = self.bliteSession.load(self.taskId,self.jobId)
        wrapperReturnCode=str(task.jobs[0].runningJob['wrapperReturnCode'])
        applicationReturnCode=str(task.jobs[0].runningJob['applicationReturnCode'])
        loggindict.setdefault

        logging.info("--->>> wrapperReturnCode = " + str(wrapperReturnCode))         
        logging.info("--->>> applicationReturnCode = " + str(applicationReturnCode))

        # first check if the case is handled
        if applicationReturnCode not in self.ErrorMatrix.keys():
            # if not, do default action (NoResubmission)
            #Do default 
            logging.info("Do default action %s"%(str(applicationReturnCode)))
            textmsg = self.defaultAction()
        else:
            if ('ANY' in self.ErrorMatrix[applicationReturnCode].keys()):  wrapperReturnCode = 'ANY'
        
            if (wrapperReturnCode not in  self.ErrorMatrix[applicationReturnCode].keys()) :
                #Do default 
                logging.info("Do default action %s"%(str(wrapperReturnCode)))
                textmsg = self.defaultAction()
            else:
                # else do as defined in errorMatrix
                textmsg = self.ErrorMatrix[applicationReturnCode][wrapperReturnCode]()
            pass # check wrapperReturnCode
        pass # check applicationReturnCode
        logging.info(textmsg)

        ## prepare dictionary for logging.info
        loggindict.setdefault("ev", "Error handling with: [%s]" %(str(self.__class__.__name__)))
        loggindict.setdefault("wrapperReturnCode", str(wrapperReturnCode))
        loggindict.setdefault("applicationReturnCode", str(applicationReturnCode))
        loggindict.setdefault("status", task.jobs[0].runningJob['status'])
        loggindict.setdefault("txt", textmsg)
        ## send information to log
        #self.infoLogger(task['name'], jobId, loggindict)

registerHandler(CrabRunFailureHandler(),"crabRunFailureHandler","ErrorHandler")
