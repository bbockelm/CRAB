#!/usr/bin/env python
"""
_FatWorker_

Implements thread logic used to perform the actual Crab task submissions.

"""

__revision__ = "$Id: FatWorker.py,v 1.12 2007/09/20 10:16:10 farinafa Exp $"
__version__ = "$Revision: 1.12 $"

import sys
import time
from threading import Thread

from MessageService.MessageService import MessageService

# BossLite import
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface 

# CRAB dependencies
from CrabServer.crab_util import *

class FatWorker(Thread):
    def __init__(self, logger, FWname, brokerName, taskDescriptor, cmdDescriptor,
                 taskUniqName, proxyFile, taskDir,
                 resubCount, dashBparam={}, dynamicBlackList=[]):

        Thread.__init__(self)
        # Worker Properties
        self.tInit = time.time()
        self.log = logger
        self.myName = FWname

        # BossLite related
        bossCfgDB = {}
        schedulerConfig = {}
        self.blDBsession = None
        self.blSchedSession = None

        # Cmd attributes
        self.resCount = resubCount
        self.cmdDesc = cmdDescriptor
        self.fullCmdXML = xml.dom.minidom.parseString(cmdDescriptor)
        self.cmdXML = self.fullCmdXML.getElementsByTagName("TaskAttributes")

        # Task attributes
        self.taskXML = taskDescriptor
        self.taskName = taskUniqName
        ## not needed -- TaskTracking duty self.dashParams = dashBparam
        self.taskDir = taskDir
        self.whiteL = []
        self.blackL = [] + dynamicBlackList # TODO: passed as params by the BossLite XML?

        # simple container to move _small_ portions of the cfg to server
        # TODO once the things become clear put these directly on the cmd XML format # Fabio
        self.cfg_params = {}
        self.cfg_params.update( eval(str(doc.getAttribute("CfgParamDict"))) )

        ## Prepare the config for the BossSession
        # TODO problably dont needed, but stored in the Task/RunningJob objects 
        schedName = self.cmdXML.getAttribute('Scheduler')
        schedulerConfig['user_proxy'] = proxyFile

        if schedName in ['glite', 'glitecoll']:
            schedulerConfig['name'] = 'SchedulerGLite' #'SchedulerGLiteAPI'
            schedulerConfig['config'] = 'glite.conf.CMS_' + brokerName
        elif schedName == 'edg':
            schedulerConfig['name'] = 'SchedulerEDG'
            schedulerConfig['config'] = 'edg_wl_ui_cmd_var.conf.CMS_' + + brokerName

        # Prepare filter lists for matching sites
        if 'glite' in schedName:
            if ('EDG.ce_white_list' in self.cfg_params) and (self.cfg_params['EDG.ce_white_list']):
                for ceW in self.cfg_params['EDG.ce_white_list'].strip().split(","):
                    if ceW:
                        self.whiteL.append(ceW.strip())
            if ('EDG.ce_black_list' in self.cfg_params) and (self.cfg_params['EDG.ce_black_list']):
                for ceB in self.cfg_params['EDG.ce_black_list'].strip().split(","):
                    if ceB:
                        self.blackL.append(ceB.strip())

        ## Initialize BossLite sessions
        # TODO this should be passed by the component at higher level
        bossCfgDB = {'dbName':'BossLiteDB', 'user':'ProdAgentUser', 'passwd':'ProdAgentPass', \
            'socketFileLocation':'$PRODAGENT_WORKDIR/mysqldata/mysql.sock'}

        self.blDBsession = BossLiteAPI('MySQL', bossCfgDB)
        # TODO take schedConfig from DBObjects ? i.e. posticipare the schedSession after the pre-checks # Fabio
        self.blSchedSession = BossLiteAPISched(self.blDBsession, schedulerConfig) 

        ## Init the session objects
        self.local_ms = MessageService()
        self.local_ms.registerAs(FWname)

        ## perform the submission
        self.start(taskDescriptor)
        pass
        
    def run(self, taskDescriptor):
        return self.submissionDriver(taskDescriptor)

####################################
    # Submission methods 
####################################

    def submissionDriver(self, taskDescr):
        taskObj = None
        newRange = None
        skipped = None

        # if taskDescr is not None then we are working on a new task
        if taskDescr is not None:
            ## create a new task object in the boss session and register its jobs to PA core
            self.local_ms.publish("CrabServerWorkerComponent:TaskArrival", self.taskName)
            self.local_ms.commit()

            taskObj = self.blDBsession.declare(self.taskXML)
            if self.registerTask(taskObj) != 0:
                self.sendResult(10, "Unable to register task %s. Causes: deserialization, saving, registration "%(self.taskName), \
                    "FatWorkerError %s. Error while registering jobs for task %s."%(self.myName, self.taskName) )
                # propagate failure message
                self.local_ms.publish("CrabServerWorkerComponent:CrabWorkFailed", self.taskName)
                self.local_ms.commit()
                return 
        else:
            ## retrieve the task from the boss session 
            self.local_ms.publish("CrabServerWorkerComponent:CommandArrival", self.taskName)
            self.local_ms.commit()

            taskObj = self.blDBsession.loadTaskByName(self.cmdXML.getAttribute('Task') )
            if taskObj is None:
                self.sendResult(11, "Unable to retrieve task %s. Causes: loadTaskByName"%(self.taskName), \
                    "FatWorkerError %s. Requested task %s does not exist."%(self.myName, self.taskName) )
                # propagate failure message
                self.local_ms.publish("CrabServerWorkerComponent:CrabWorkFailed", self.taskName)
                self.local_ms.commit()
                return 

        ## Go on with the submission
        newRange, skippedJobs = self.preSubmissionCheck(taskObj, self.cmdXML.getAttribute('Range') )
        if (newRange is not None) and (len(newRange) > 0):
            submissionMaps = {'[]':[]}
            submissionMaps.update( self.submissionListsCreation(taskObj, newRange) )

            # ----- call the submitter -----
            submittedJobs, unmatchedJobs, nonSubmittedJobs = self.submitTaskBlocks(taskObj, submissionMaps, newRange)
            self.evaluateSubmissionOutcome(taskObj, newRange, submittedJobs, unmatchedJobs, nonSubmittedJobs, skippedJobs)
            # ----- ----- ----- ----- ------
            return 

        ## Manage the empty range case due to incompatibilities
        if (newRange is not None):
            if taskDescr is not None:
                self.sendResult(20, "Empty submission range for task %s"%self.taskName, \
                    "FatWorker %s. Empty range submission for task %s"%(self.myName, self.taskName) )
            else:
                self.sendResult(21, "Command empty submission range for task"%self.taskName, \
                    "FatWorker %s. Empty range submission for task %s"%(self.myName, self.taskName) )
            # propagate the re-submission attempt
            self.resubCount -= 1
            payload = str(self.fullCmdXML.toxml())+"::"+self.taskName+"::"+str(self.resubCount)
            self.local_ms.publish("CRAB_Cmd_Mgr:NewCommand", payload)
            self.local_ms.commit()
            return 

        ## Finally, manage possible exceptions on the preSubmission checks
        if taskDescr is not None:
            self.sendResult(30, "Exception during task %s submission checks"%self.taskName, \
                 "FatWorker %s. Failure during pre-submission checks for task %s"%(self.myName, self.taskName) )
        else:
            self.sendResult(31, "Command failure during pre-submission checks for task %s"%self.taskName, \
                 "FatWorker %s. Failure during pre-submission checks for task %s"%(self.myName, self.taskName) )
        # propagate the re-submission attempt
        self.resubCount -= 1
        payload = str(self.fullCmdXML.toxml())+"::"+self.taskName+"::"+str(self.resubCount)
        self.local_ms.publish("CRAB_Cmd_Mgr:NewCommand", payload)
        self.local_ms.commit()
        return 

    def preSubmissionCheck(self, task, rng):
        newRange = [i-1 for i in parseRange2(rnd)]  # TODO a-la-CRAB range expansion and Boss ranges (0 starting)
        doNotSubmitStatusMask = ['R','S'] # ,'K','Y','D'] # to avoid resubmission of final state jobs
        tryToSubmitMask = ['C', 'A', 'RC', 'Z'] + ['K','Y','D']
        skippedSubmissions = []

        ## check if the input sandbox is already on the right SE  
        taskFileList = []
        # TODO populate the filelist with task/runningObject attributes # Fabio  
        sbi = SBinterface(params['protocol'], params['seUrl'])
        for f in taskFileList:
            if sbi.checkExists(f) == False:
                # A file is missing, reject all the task
                return [], newRange
  
        for j in task.jobs:
            if (j['jobId'] in newRange):
                # do not submit already running or scheduled jobs
                if j['status'] in doNotSubmitStatusMask:
                    self.log.info("FatWorker %s.\n Task %s job %s status %s. Won't be submitted"%(self.myName, \
                        self.taskName, j['name'], j['status']) )
                    newRange.remove(j['jobId'])
                    continue

                # trace unknown state jobs and skip them from submission
                if j['status'] not in tryToSubmitMask:
                    newRange.remove(j['jobId'])
                    skippedSubmissions.append(j['jobId'])

        return newRange, skippedSubmissions

    def submitTaskBlocks(self, taskObj, blockMap, rng):
        # skip unmatched jobs
        unmatchedJobs = []
        unmatchedJobs +=  blockMap['[]']
        del blockMap['[]']

        # loop and submit blocks
        for bulkRng in blockMap.values():
            self.blSchedSession.submit(taskObj, bulkRng)
            reqSubmJobs += bulkRng

        submittedJobs = [ j['jobId'] for j in self.blDBsession.loadSubmitted(taskObj) ]
        submittedJobs += [ j['jobId'] for j in self.blDBsession.loadEnded(taskObj) if j['jobId'] not in submittedJobs ]
        # get the really not submitted jobs
        nonSubmittedJobs = [ jId for jId in rng if j not in (submittedJobs + unmatchedJobs) ]        
        return submittedJobs, unmatchedJobs, nonSubmittedJobs

####################################
    # Results propagation methods
####################################

    def evaluateSubmissionOutcome(self, taskObj, submittableRange, submittedJobs, \
            unmatchedJobs, nonSubmittedJobs, skippedJobs):

        ## log the result of the submission
        self.log.info("FatWorker %s. Task %s (%d jobs): submitted %d unmatched %d notSubmitted %d skipped %d"%(self.myName, self.taskName, \
            len(submittableRange), len(submittedJobs), len(unmatchedJobs), len(nonSubmittedJobs), len(skippedJobs) ) \
            )
        self.log.debug("FatWorker %s. Task %s\n"%(self.myName, self.taskName) + \
            "jobs : %s \nsubmitted %s \nunmatched %s\nnotSubmitted %s\nskipped %s"%(str(submittableRange), \
            str(submittedJobs), str(unmatchedJobs), str(nonSubmittedJobs), str(skippedJobs) ) \
            )

        # TODO NO MORE needed ... self.dashBoardPublish(taskObj, submittedJobs)

        ## if all the jobs have been submitted send a success message
        if len(unmatchedJobs)==0 and len(nonSubmittedJobs)==0 and len(skippedJobs)==0:
            self.sendResult(0, "Full Success for %s"%self.taskName, \
                "FatWorker %s. Successful complete submission for task %s"%(self.myName, self.taskName) )

            self.local_ms.publish("CrabServerWorkerComponent:CrabWorkPerformed", taskObj['name'])
            self.local_ms.commit()
            return

        ## some jobs need to be resubmitted later
        else:
            # get the list of missing jobs # discriminate on the lists? in future maybe
            setMap = {}
            for j in unmatchedJobs + nonSubmittedJobs + skippedJobs:
                setMap[j] = 0
            resubmissionList = [] + setMap.keys()
            del setMap

            # prepare a new message for the missing jobs
            resubmissionList = [ i+1 for i in resubmissionList ]
            newRange = str(resubmissionList).replace('[','').replace(']','')
            self.cmdXML.setAttribute('Range', newRange)
            #### replaceChild(newChild, oldChild)? # Check if ok # Fabio

            if len(submittedJobs) == 0:
                self.sendResult(-1, "Any jobs submitted for task %s"%self.taskName, \
                    "FatWorker %s. Any job submitted for task %s, %d more attempts \
                     will be performed"%(self.myName, self.taskName, self.resubCount))
            else:
                self.sendResult(-2, "Partial Success for %s"%self.taskName, \
                    "FatWorker %s. Partial submission for task %s, %d more attempts \
                     will be performed"%(self.myName, self.taskName, self.resubCount))
                self.local_ms.publish("CrabServerWorkerComponent:CrabWorkPerformedPartial", taskObj['name'])
                self.local_ms.commit()

            # propagate the re-submission attempt
            self.resubCount -= 1
            payload = str(self.fullCmdXML.toxml())+"::"+self.taskName+"::"+str(self.resubCount)
            self.local_ms.publish("CRAB_Cmd_Mgr:NewCommand", payload)
            self.local_ms.commit()
        return

####################################
    # Auxiliary methods
####################################

    def sendResult(self, status, reason, logMsg):
        self.log.info(logMsg)
        msg = self.myName + "::" + self.taskName + "::"
        msg += str(status) + "::" + reason + "::" + str(time.time() - self.tInit)
        self.local_ms.publish("CrabServerWorkerComponent:FatWorkerResult", msg)
        self.local_ms.commit()
        return

    def registerTask(self, taskArg):
        from ProdAgent.WorkflowEntities import Job
        from ProdCommon.Database.SafeSession import SafeSession
        from ProdAgentDB.Config import defaultConfig as dbConfig
        from ProdCommon.Database.MysqlInstance import MysqlInstance

        th_db_session = SafeSession(dbInstance = MysqlInstance(dbConfig))
        
        for job in taskArg.jobs:
            jobName = self.taskName + "_" + job['name'] # self.taskName + "_" + str(jid)
            if Job.exists(jobName) == True:
                continue

            cacheArea = self.taskDir + "/res/job%s"%job['jobId']
            jobDetails = {'id':self.taskName,'job_type':'Processing','max_retries':4,'max_racers':1, 'owner':self.taskName}
            try:    
                Job.register(self.taskName, None, jobDetails)
                Job.setState(self.taskName,'create')
                Job.setCacheDir(jobName,cacheArea)
                Job.setState(jobName,'inProgress')
                Job.setState(jobName,'submit')
            except Exception, e:
                self.log.info("BOSS Registration error %s. DB insertion."%cacheArea + str(e))
                th_db_session.rollback()
                th_db_session.close()
                return 1

            # create cache area # WB: NEEDED FOR RESUBMISSION WITH CVS JobSubmitterComponent
            # WARNING: is still needed for the new arch ?? # Fabio
            '''
            from ProdCommon.MCPayloads.JobSpec import JobSpec
            try:
                os.mkdir(cacheArea)
                fakeJobSpec = JobSpec()
                fakeJobSpec.parameters['SubmissionCount'] = 0  
                fakeJobSpec.setJobName(jobName)
                fakeJobSpec.save("%s/%s-JobSpec.xml"%(cacheArea,jobName))
                idfile = open("%s/%sid"%(cacheArea,jobName),'w')
                idfile.write("JobId=%s"%job['jobId'])
                idfile.close()
            except Exception, e:
                logging.info("BOSS Registration error %s. Cache area creation."%cacheArea + str(e))
                session.rollback()
                return 1
            '''
        th_db_session.commit()
        th_db_session.close()        
        return 0

    def submissionListsCreation(self, taskObj, jobRng):
        # no more needed: different approach w.r.t. Crab SA  # from BlackWhiteListParser import BlackWhiteListParser
        #
        # ASSUMPTION: data discovery performed as usual on the client at creation
        # and the compatible sites have been collected in 'destination' field of the
        # running object structure. # Fabio
        # NOTE: no assumptions are done on the fileblocks, the only relevant information is
        # the compatible sites matching the jobs.
        bulkMap = {'[]':[]}
        
        ## create the structure that associates destinations with jobs Ids
        for job in taskObj:
            if job['jobId'] in jobRng:
                if type(job['destination']) is list:
                    prunedDestination = job['destination']
                else:
                    prunedDestination = str(job['destination']).replace('[','').replace(']','')
                    prunedDestination = prunedDestination.split(',')
                
                # clean the destinations from blackList
                prunedDestination = [ceURL for ceURL in prunedDestination if ceURL not in self.blackL]
                # force the whiteList if any
                if self.whiteL:
                    prunedDestination = [ceURL for ceURL in prunedDestination if ceURL in self.whiteL]
                    
                # compose the bulk submissions
                if prunedDestination in bulkMap:
                    bulkMap[prunedDestination].append(job['jobId'])
                else:
                    bulkMap[prunedDestination] = []
        return bulkMap

'''
    def dashBpublish(self, taskObj, submittedJobs):
        for j in taskObj.jobs:
            if j['jobId'] not in submittedJobs:
                continue

            j['status'] = 'S'
            j['statusHistory'].append('S') 
            jid = j['id']

            # taken from the old submitter # structures preparation
            jobId = str(j['jobId']) + '_' + jid
            if taskObj['scheduler'] == 'CONDOR_G':
                condor_hash = str(self.cfg_params['cfgFileNameCkSum'])
                jobId =  str(j['jobId'])+'_'+condor_hash+'_'+jid

                rb = 'OSG'
                if ':' in jid:
                    rb = str(jid.split(':')[1]).replace('//', '')

                T_SE = '%d_Selected_SE'%len(j['destination'])
                if len(j['destination']) <= 2 :
                    T_SE = string.join(j['destination'],",")

                # submit information to DashBoard 
                # TODO check with Daniele for the id codes. # ce n'e uno in meno rispetto al vecchio SA # Fabio
                params = {'jobId': j['jobId'], 'sid': jid, 'broker': rb, \
                    'bossId': j['jobId'], 'SubmissionType': 'Server', 'TargetSE': T_SE}
                self.dashParams.update(params)

                taskId = 'unknown'
                jobId = 'unknown'
                if 'taskId' in self.dashParams:
                    taskId = self.dashParams['taskId']
                if 'jobId' in self.dashParams:
                    jobId = self.dashParams['jobId']

                apmonSend(taskId, jobId, self.dashParams)
        # finalize the dashboard interactions
        apmonFree()
        return
'''

