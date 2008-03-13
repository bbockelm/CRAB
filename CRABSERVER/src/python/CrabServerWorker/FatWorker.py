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
from xml.dom import minidom
import traceback
import copy

# BossLite import
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

from ProdAgent.WorkflowEntities import Job as wfJob
from ProdAgent.WorkflowEntities import JobState

from ProdCommon.Database import Session
from ProdCommon.Database.MysqlInstance import MysqlInstance

# CRAB dependencies
from CrabServer.crab_util import *

class FatWorker(Thread):
    def __init__(self, logger, FWname, configs):

        Thread.__init__(self)

        ## Worker Properties
        self.tInit = time.time()
        self.log = logger
        self.myName = FWname

        try:
            self.proxy = configs['proxy']
            self.brokerName = configs['rb'] 
            self.taskName = configs['taskname']
            self.resubCount = int(configs['resubCount'])
            self.wdir = configs['wdir'] 
            self.firstSubm = configs['firstSubmit']
            self.SEproto = configs['SEproto']
            self.SEurl = configs['SEurl']
            self.SEport = configs['SEport']
            self.blackL = [] + configs['dynBList']
        except Exception, e:
            self.log.info('Missing parameters in the Worker configuration')
            self.log.info( traceback.format_exc() ) 
        
        # derived attributes
        self.local_ms = MessageService()
        self.local_ms.registerAs(self.myName)
        
        self.blDBsession = None
        self.blSchedSession = None
        self.schedulerConfig = {}

        self.cmdXML = None
        self.taskXML = None

        self.whiteL = []

        # Parse the XML files
        taskDir = self.wdir + '/' + self.taskName + '_spec/'
        try:
            cmdSpecFile = taskDir + 'cmd.xml'
            doc = minidom.parse(cmdSpecFile)
            self.cmdXML = doc.getElementsByTagName("TaskCommand")[0]

            #taskSpecFile = taskDir + 'task.xml'
            #f = open(taskSpecFile, 'r')
            #self.taskXML = f.readlines()
            #f.close()

            # TODO fix for BL deserialization blocking bug
            self.taskXML =  taskDir + 'task.xml'

        except Exception, e:
            status = 6
            reason = "Error while parsing command XML for task %s, it will not be processed"%self.taskName
            self.log.info( reason +'\n'+ traceback.format_exc() )
            self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
            self.local_ms.commit()
            return

        # simple container to move _small_ portions of the cfg to server
        self.cfg_params = eval( self.cmdXML.getAttribute("CfgParamDict") )
 
        ## Perform the submission
        self.log.info("Worker %s initialized"%self.myName)
        try:
            self.start()
        except Exception, e:
            self.log.info('"Worker %s exception : \n'+traceback.format_exc() )
        pass
        
    def run(self):
        ## Warm up phase
        self.blDBsession = BossLiteAPI('MySQL', dbConfig)
 
        self.schedulerConfig.update( {'user_proxy' : self.proxy} )
        schedName = str( self.cmdXML.getAttribute('Scheduler') )

        if schedName in ['glite', 'glitecoll']:
            self.schedulerConfig['name'] = 'SchedulerGLiteAPI' #'SchedulerGLite'
            self.schedulerConfig['config'] = self.wdir + '/glite.conf.CMS_' + self.brokerName
            self.schedulerConfig['service'] = 'https://wms006.cnaf.infn.it:7443/glite_wms_wmproxy_server'
        elif schedName == 'edg':
            self.schedulerConfig['name'] = 'SchedulerEDG'
            self.schedulerConfig['config'] = self.wdir + '/edg_wl_ui_cmd_var.conf.CMS_' + + self.brokerName
            self.schedulerConfig['service'] = ''
        elif schedName == 'condor_g':
            pass
        elif schedName == 'arc':
            pass
        elif schedName == 'lsf':
            pass

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

        # actual submission
        self.log.info("Worker %s warmed up and ready to submit"%self.myName)
        try:
            self.submissionDriver()
        except Exception, e:
            self.log.info( traceback.format_exc() )

        return

####################################
    # Submission methods 
####################################
    
    def submissionDriver(self):
        taskObj = None
        newRange = None
        skipped = None

        if self.firstSubm == True:
            ## create a new task object in the boss session and register its jobs to PA core
            self.local_ms.publish("CrabServerWorkerComponent:TaskArrival", self.taskName)
            self.local_ms.commit()
           
            taskObj = self.blDBsession.declare(self.taskXML, self.proxy)
            if self.registerTask(taskObj) != 0:
                self.sendResult(10, "Unable to register task %s. Causes: deserialization, saving, registration "%(self.taskName), \
                    "FatWorkerError %s. Error while registering jobs for task %s."%(self.myName, self.taskName) )
                # propagate failure message
                self.local_ms.publish("CrabServerWorkerComponent:CrabWorkFailed", self.taskName)
                self.local_ms.commit()
                return 
            self.log.info('Worker %s submitting a new task'%self.myName)

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
            self.log.info('Worker %s submitting a new command on a task'%self.myName)

        ## Go on with the submission
        newRange, skippedJobs = self.preSubmissionCheck(taskObj, str(self.cmdXML.getAttribute('Range')) )
        if (newRange is not None) and (len(newRange) > 0):
            submissionMaps = {'[]':[]}
            submissionMaps.update( self.submissionListsCreation(taskObj, newRange) )
            self.log.info('Worker %s passed pre-submission checks passed'%self.myName)

            # ----- call the submitter -----
            # self.schedulerConfig['service'] = taskObj['serverName'] # TODO check if correct, ask DS-GC #Fabio
            self.blSchedSession = BossLiteAPISched(self.blDBsession, self.schedulerConfig)
            self.log.info('Worker %s scheduler interaction set'%self.myName)

            submittedJobs, unmatchedJobs, nonSubmittedJobs = self.submitTaskBlocks(taskObj, submissionMaps, newRange)
            self.log.info('Worker %s submitted jobs. Ready to evaluate outcomes'%self.myName)
            self.evaluateSubmissionOutcome(taskObj, newRange, submittedJobs, unmatchedJobs, nonSubmittedJobs, skippedJobs)
            # ----- ----- ----- ----- ------
            return 

        ## Manage the empty range case due to incompatibilities
        if (newRange is not None):
            if self.firstSubm == True:
                self.sendResult(20, "Empty submission range for task %s"%self.taskName, \
                    "FatWorker %s. Empty range submission for task %s"%(self.myName, self.taskName) )
            else:
                self.sendResult(21, "Command empty submission range for task %s"%self.taskName, \
                    "FatWorker %s. Empty range submission for task %s"%(self.myName, self.taskName) )
            # propagate the re-submission attempt
            self.resubCount -= 1
            payload = self.taskName+"::"+str(self.resubCount)
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
        payload = self.taskName+"::"+str(self.resubCount)
        self.local_ms.publish("CRAB_Cmd_Mgr:NewCommand", payload)
        self.local_ms.commit()
        return 

    def preSubmissionCheck(self, task, rng):
        newRange = eval(rng)  # TODO a-la-CRAB range expansion and Boss ranges (0 starting)
        doNotSubmitStatusMask = ['R','S'] # ,'K','Y','D'] # to avoid resubmission of final state jobs
        tryToSubmitMask = ['C', 'A', 'RC', 'Z'] + ['K','Y','D']
        skippedSubmissions = []

        ## check if the input sandbox is already on the right SE  
        taskFileList = list( task['globalSandbox'] )

        # TODO fix the problem of abs paths on client-side # Fabio
        return newRange, skippedSubmissions
        ##
        #############################
         
        # TODO fix the problem of abs paths on client-side and add proxy path infos # Fabio
        ###
        try:
            seEl = SElement(self.seCfg['url'], self.seCfg['proto']) 
            sbi = SBinterface( seEl )
            for f in taskFileList:
                if sbi.checkExists(f, self.proxy) == False:
                    return [], newRange

        except Exception, e:
            self.log.info( traceback.format_exc() )
            return [], newRange
        
        # TODO how to get information about the current running objects of the task?
        ### 
        for j in task.jobs:
            if (j['jobId'] in newRange):
                try:
                    rj = j.runningJob
                except Exception, e:
                    self.log.info("FatWorker %s.\n Task %s job %s not a RunningJob object. Won't be submitted"%(self.myName, \
                             self.taskName, j['name']) )
                    newRange.remove(j['jobId'])
                    continue 

                # do not submit already running or scheduled jobs
                if rj['status'] in doNotSubmitStatusMask:
                    self.log.info("FatWorker %s.\n Task %s job %s status %s. Won't be submitted"%(self.myName, \
                        self.taskName, j['name'], j['status']) )
                    newRange.remove(j['jobId'])
                    continue

                # trace unknown state jobs and skip them from submission
                if rj['status'] not in tryToSubmitMask:
                    newRange.remove(j['jobId'])
                    skippedSubmissions.append(j['jobId'])
        ## 
        return newRange, skippedSubmissions

    def submitTaskBlocks(self, taskObj, blockMap, rng):
        # skip unmatched jobs
        unmatchedJobs = [] + blockMap['[]']
        del blockMap['[]']

        # loop and submit blocks
        for bulkRng in blockMap.values():
            if len(bulkRng) > 0:
                self.blSchedSession.submit(taskObj, bulkRng)
                # reqSubmJobs += bulkRng

        # TODO bad counting of submitted jobs # Fabio
        submittedJobs = [ j['jobId'] for j in self.blDBsession.loadSubmitted(taskObj) ]
        submittedJobs += [ j['jobId'] for j in self.blDBsession.loadEnded(taskObj) if j['jobId'] not in submittedJobs ]
        # get the really not submitted jobs
        nonSubmittedJobs = [ jId for jId in rng if jId not in (submittedJobs + unmatchedJobs) ]        
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
                self.local_ms.publish("CrabServerWorkerComponent:CrabWorkPerformedPartial", self.taskName)
                self.sendResult(-2, "Partial Success for %s"%self.taskName, \
                    "FatWorker %s. Partial submission for task %s, %d more attempts \
                     will be performed"%(self.myName, self.taskName, self.resubCount))

            # propagate the re-submission attempt
            self.resubCount -= 1
            payload = self.taskName+"::"+str(self.resubCount)
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
        # TODO DB Problems here, bypassed for now # Fabio
        #return 0

        try:
            dbCfg = copy.deepcopy(dbConfig)
            dbCfg['dbType'] = 'mysql'

            Session.set_database(dbCfg)
            Session.connect(self.taskName)
            Session.start_transaction(self.taskName)

            for job in taskArg.jobs:
                jobName = self.taskName + '_' + job['name'] 
                cacheArea = self.wdir + '/' + self.taskName + '_spec/%s'%job['name']
                jobDetails = {'id':jobName, 'job_type':'Processing', 'max_retries':4, 'max_racers':1, 'owner':self.taskName}

                wfJob.register(jobName, None, jobDetails)
                wfJob.setState(jobName, 'create')
                wfJob.setCacheDir(jobName, cacheArea)
                wfJob.setState(jobName, 'inProgress')
                wfJob.setState(jobName, 'submit')

            Session.commit(self.taskName)
            Session.close(self.taskName)
            return 0

        except Exception, e:
            self.log.info('Error while registering job for JT: %s'%self.taskName)
            self.log.info( traceback.format_exc() )
            Session.rollback(self.taskName)
            Session.close(self.taskName)
            return 1
 
        return 1    
        '''
            # create cache area # WB: NEEDED FOR RESUBMISSION WITH CVS JobSubmitterComponent
            # WARNING: is still needed for the new arch ?? # Fabio

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
        return 1

    def submissionListsCreation(self, taskObj, jobRng):
        bulkMap = {'[]':[]}
        
        ### define here the list of distinct destinations sites list
        distinct_dests = self.queryDistJob_Attr(taskObj['id'], 'dlsDestination', 'jobId' ,jobRng)
 
        ### fill the per-site maps
        for dest in distinct_dests:
            # prune blacklist
            if dest in self.blackL:
                bulkMap['[]'].append( self.queryAttrJob({'dlsDestination':dest},'jobId') )
                continue

            # prune not whitelist
            if len(self.whiteL) > 0 and dest not in self.whiteL:
                bulkMap['[]'].append( self.queryAttrJob({'dlsDestination':dest},'jobId') )
                continue

            # submission range 
            bulkMap[str(dest)] = []
            for jid in self.queryAttrJob({'dlsDestination':dest},'jobId'):
                if jid in jobRng:
                     bulkMap[str(dest)].append(jid)

        ### prune lists according listmatches
        for dest in bulkMap:
            # already unmatching jobs
            if dest == '[]':
                continue

            list_match_jobs = [] + bulkMap[dest] 
            for id_job in list_match_jobs:
                # TODO extract scheduler info from running job instead of XML message 
                schedName = str( self.cmdXML.getAttribute('Scheduler') ).upper()

                if schedName != "CONDOR_G" :
                    ## TODO correct here 
                    match = "1" #match = common.scheduler.listMatch(id_job)
                else :
                    match = "1"

                if not match:
                    bulkMap[dest].remove(id_job)
                    bulkMap['[]'].append(id_job)
        ### all done
        return bulkMap

# Taken from the CRAB-SA submitter
    def queryDistJob_Attr(self, taskId, attr_1, attr_2, list):
        '''
        Returns the list of distinct value for a given job attributes
        '''
        distAttr=[]
        task = self.blDBsession.loadJobDistAttr( taskId, attr_1, attr_2, list )
        for i in task: 
            distAttr.append(i[attr_1])
        return  distAttr

    def queryAttrJob(self, attr, field):
        '''
        Returns the list of jobs matching the given attribute
        '''
        matched=[]
        task = self.blDBsession.loadJobsByAttr(attr)
        for i in task:
            matched.append(i[field])
        return  matched




