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
from ProdAgent.WorkflowEntities.JobState import doNotAllowMoreSubmissions


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
         
         
    def parseFinalReport(self, input):
        """
        Parses the FJR produced by job in order to retrieve
        the WrapperExitCode and ExeExitCode.
        Updates the BossDB with these values.
        """
        codeValue = {}
        jobReport = readJobReport(input)[0]
        exit_status = ''
        ##### temporary fix for FJR incomplete ####
        fjr = open (input)
        len_fjr = len(fjr.readlines())
        if (len_fjr <= 6):
            codeValue["applicationReturnCode"] = str(50115)
            codeValue["wrapperReturnCode"] = str(50115)
        #####
        if len(jobReport.errors) != 0 :
            for error in jobReport.errors:
                if error['Type'] == 'WrapperExitCode':
                    codeValue["wrapperReturnCode"] = error['ExitStatus']
                elif error['Type'] == 'ExeExitCode':
                    codeValue["applicationReturnCode"] = error['ExitStatus']
                else:
                    continue
        if not codeValue.has_key('wrapperReturnCode'):
            codeValue["wrapperReturnCode"] = '' 
        if not codeValue.has_key('applicationReturnCode'):
            codeValue["applicationReturnCode"] = ''
        return codeValue
         

    def handleError(self,payload):
         """
         The payload of a job failure is a url to the job report
         """
         
         self.bliteSession = BossLiteAPI('MySQL', dbConfig)
         
         logging.info(">CrabRunFailureHandler<:payload %s " % payload)
         jobReportUrl = payload
         
         #### split of payload to obtain taskId and jobId ####
         tmp = payload.split("BossJob")
         tmp_1 = tmp[1]
         tmp_2 = tmp_1.split('/')
         numbers = tmp_2[0]
         numbers_tmp = numbers.split('_')
         taskId = numbers_tmp[1]
         logging.info("--->>> taskId = " + str(taskId))         
         jobId = numbers_tmp[2]
         logging.info("--->>> jobId = " + str(jobId))        
 
         # prepare to retrieve the job report file.
         # NOTE: we assume that the report file has a relative unique name
         # NOTE: if that is not the case we need to add a unique identifier to it.
         slash = jobReportUrl.rfind('/')
         fileName = jobReportUrl[slash+1:]
         ### to test if correct !!
         urllib.urlretrieve(jobReportUrl, self.args['jobReportLocation']+'/'+fileName)
         logging.info(">CrabRunFailureHandler<:Retrieving job report from %s " % jobReportUrl)
        
          
         jobReport=readJobReport(self.args['jobReportLocation']+'/'+fileName)
         logging.info("--->>> " + self.args['jobReportLocation']+'/'+fileName)
         
         ### Retrieving wrapper and application exit code from fjr #### 
         input = self.args['jobReportLocation']+'/'+fileName
         logging.info("--->>> input = " + input)         
        
         listCode = []
         if os.path.exists(input):
              codeValue = self.parseFinalReport(input)
         else:
              msg = ">CrabRunFailureHandler<:Problems with "+str(input)+". File not available.\n"
              logging.info(msg)
         logging.info("--->>> wrapperReturnCode = " + str(codeValue["wrapperReturnCode"]))
         logging.info("--->>> applicationReturnCode = " + str(codeValue["applicationReturnCode"]))

         ### Updating the boss DB with wrapper and application exit code #### 
         task = self.bliteSession.load(taskId,jobId)
         
         if (codeValue["applicationReturnCode"] != '' ):
             task.jobs[0].runningJob['applicationReturnCode']=int(codeValue["applicationReturnCode"])
         if (codeValue["wrapperReturnCode"] != ''): 
             task.jobs[0].runningJob['wrapperReturnCode']=int(codeValue["wrapperReturnCode"])

         self.bliteSession.updateDB(task)
        
         #### Querying the boss db to obtain wrapper and application exit code ###
         task = self.bliteSession.load(taskId,jobId)
         wrapperReturnCode=task.jobs[0].runningJob['wrapperReturnCode']
         applicationReturnCode=task.jobs[0].runningJob['applicationReturnCode']

         logging.info("--->>> wrapperReturnCode = " + str(wrapperReturnCode))         
         logging.info("--->>> applicationReturnCode = " + str(applicationReturnCode))

         #### if both codes are empty and the job has been killed or aborted
         #### the job will be resubmit with delay
         
         #### add a control about the number of resubmission ####

         if ((wrapperReturnCode is None) and ( applicationReturnCode is None)):
             if (task.jobs[0].runningJob['status'] == 'K'):# or (task.jobs[0].runningJob['status'] == 'A')):
                 ### to check if correct 
                 ### dont_allow_resubmission()
                 #if ((task.jobs[0].runningJob['submission'] >= 2) and (task.jobs[0].runningJob['status_history'] == 'A')):
                 if (task.jobs[0].runningJob['submission'] >= 2):
                     try:
                         doNotAllowMoreSubmissions([jobId])
                     except ProdAgentException, ex:
                         msg = "Updating max racers fields failed for job %s\n" % jobId
                         logging.info(msg)
                         logging.error(msg)
                 
                 else:
                     ### or check the statusReason or postmortem to get the failure reason
                     ### wait 2 minutes before resubmitting
                     delay = 120
                     delay=convertSeconds(delay) 
                     logging.info(">CrabRunFailureHandler<: re-submitting with delay (h:m:s) "+ str(delay))
                     payload = str(taskId)+'::'+str(jobId) 
                     logging.info("--->>> payload = " + payload)         
                     self.publishEvent("ResubmitJob",payload,delay)
             elif (task.jobs[0].runningJob['status'] == 'A'):
                 try:
                     doNotAllowMoreSubmissions([jobId])
                 except ProdAgentException, ex:
                     msg = "Updating max racers fields failed for job %s\n" % jobId
                     logging.info(msg)
                     logging.error(msg)
             else:
                 logging.info("--->>> status = " + task.jobs[0].runningJob['status'])
                 pass
         #try:
         #except Exception, e :
         #    logging.info(str(e))
         #    logging.info(traceback.format_exc())

         #### if wrapper code is not null and different from 60303 (file already exists in the SE) and 
         #### from 70000 (output too big), the job will be resubmit banning the site where it previously run

         if ((wrapperReturnCode is not None) and (wrapperReturnCode != 50117) and (wrapperReturnCode != 60303) and (wrapperReturnCode != 70000) and (wrapperReturnCode != 0)):
             ce = str(task.jobs[0].runningJob['destination'])
             ce_temp = task.jobs[0].runningJob['destination'].split(':')

             # fix to avoid empty ce names # Fabio
             if len(ce_temp[0])>0:
                 ce_name = ce_temp[0] 
             else:
                 ce_name = "#fake_ce#"
             # 

             logging.info("--->>> ce = " + str(ce))
             logging.info("--->>> ce_name = " + str(ce_name))
             logging.info(">CrabRunFailureHandler<: re-submitting banning the ce "+ str(ce_name))
             payload = str(taskId) + '::' + str(jobId) + '::' + str(ce_name)  
             logging.info("--->>> payload = " + payload)         
             self.publishEvent("ResubmitJob",payload)
         #### to understand how to implement the notification #### 
         #else:
         #    self.publishEvent(notification)
         
         #### if wrapper code is 60303 (file already exists in the SE) or 70000 (output too big),
         #### the job will be not resubmitted

         if ((wrapperReturnCode == 50117) or (wrapperReturnCode == 60303) or (wrapperReturnCode == 70000)):
             try:
                 doNotAllowMoreSubmissions([jobId])
             except ProdAgentException, ex:
                 msg = "Updating max racers fields failed for job %s\n" % jobId
                 logging.info(msg)
                 logging.error(msg)
             
registerHandler(CrabRunFailureHandler(),"crabRunFailureHandler","ErrorHandler")
