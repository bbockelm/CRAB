#!/usr/bin/env python
"""
_FatWorker_

Implements thread logic used to perform the actual Crab task submissions.

"""

__revision__ = "$Id: FatWorker.py,v 1.82 2008/06/17 09:05:40 farinafa Exp $"
__version__ = "$Revision: 1.82 $"
import string
import sys, os
import time
from threading import Thread
from MessageService.MessageService import MessageService
from xml.dom import minidom
import traceback
import copy
import re

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
from CrabServer.ApmonIf import ApmonIf

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
            self.submissionKind = configs['submitKind']
            self.SEproto = configs['SEproto']
            self.SEurl = configs['SEurl']
            self.SEport = configs['SEport']

            self.wmsEndpoint = configs['wmsEndpoint']
            self.se_blackL = [] + configs['se_dynBList']
            self.ce_blackL = [] + configs['ce_dynBList']

            self.maxRetries = configs['maxRetries']

        except Exception, e:
            self.log.info('Missing parameters in the Worker configuration')
            self.log.info( traceback.format_exc() ) 
        

        # Mandatory parameters for local usage (e.g. caf) #Ds
        if configs['allow_anonymous'] !=0 : 
            try:
                self.proxy == None
                self.cpCmd = configs['cpCmd']
                self.rfioServer = configs['rfioServer']
            except Exception, e:
                self.log.info('Missing parameters in the Worker configuration')
                self.log.info( traceback.format_exc() ) 

        # derived attributes
        self.local_ms = MessageService()
        self.local_ms.registerAs( '_'.join(self.myName.split('_')[:2]) )
        
        self.blDBsession = BossLiteAPI('MySQL', dbConfig)
        self.blSchedSession = None
        self.schedulerConfig = {}

        self.cmdXML = None
        self.taskXML = None

        self.se_whiteL = []
        self.ce_whiteL = []

        self.TURLpreamble = ""

        self.log.info("Worker %s initialized"%self.myName)
        self.apmon = ApmonIf()

        # fast management for ErrorHandler triggered resubmissions
        if self.submissionKind == 'errHdlTriggered':
            self.taskId = configs['taskId']
            self.jobId = configs['jobId']
            self.resubmissionDriver()
            self.apmon.free()
            return

        # Parse the XML files (cmd CfgParamDict needed by ErrHand resub too) 
        taskDir = os.path.join(self.wdir, self.taskName + '_spec/' )
        try:
            cmdSpecFile = taskDir + 'cmd.xml'
            doc = minidom.parse(cmdSpecFile)
            self.cmdXML = doc.getElementsByTagName("TaskCommand")[0]
            self.taskXML =  taskDir + 'task.xml'
        except Exception, e:
            status = 6
            reason = "Error while parsing command XML for task %s, it will not be processed"%self.taskName

            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
            self.local_ms.commit()

            return

        # simple container to move _small_ portions of the cfg to server
        self.cfg_params = eval( self.cmdXML.getAttribute("CfgParamDict") )
        self.EDG_retry_count = int(self.cfg_params['EDG_retry_count'])
        self.EDG_shallow_retry_count = int(self.cfg_params['EDG_shallow_retry_count'])

        try:
            self.start()
        except Exception, e:
            self.log.info('"Worker %s exception : \n'+traceback.format_exc() )

        self.apmon.free()
        pass
        
    def run(self):
        ## Warm up phase
        self.schedulerConfig.update( {'user_proxy' : self.proxy} )
        self.schedName = str( self.cmdXML.getAttribute('Scheduler') )
        
        if self.schedName.lower() in ['glite', 'glitecoll']:
            self.schedulerConfig['name'] = 'SchedulerGLiteAPI' 
            self.schedulerConfig['config'] = self.wdir + '/glite.conf.CMS_' + self.brokerName
            if self.wmsEndpoint:
                self.schedulerConfig['service'] = self.wmsEndpoint

        elif self.schedName.lower() == 'condor_g':
            self.schedulerConfig['name'] = 'SchedulerCondorGAPI' 
            self.schedulerConfig['config'] = self.wdir + '/glite.conf.CMS_' + self.brokerName
            if self.wmsEndpoint:
                self.schedulerConfig['service'] = self.wmsEndpoint

        elif self.schedName == 'arc':
            pass

        elif self.schedName.lower() in ['lsf','caf']:
            self.schedulerConfig['name']    = 'SchedulerLsf' 
            self.schedulerConfig['cpCmd']   = self.cpCmd 
            self.schedulerConfig['rfioSer'] = self.rfioServer 

        # Prepare filter lists for matching sites
        if 'glite' in self.schedName:
            # ce related
            if ('EDG.ce_white_list' in self.cfg_params) and (self.cfg_params['EDG.ce_white_list']):
                for ceW in self.cfg_params['EDG.ce_white_list'].strip().split(","):
                    if ceW:
                        self.ce_whiteL.append(ceW.strip())

            if ('EDG.ce_black_list' in self.cfg_params) and (self.cfg_params['EDG.ce_black_list']):
                for ceB in self.cfg_params['EDG.ce_black_list'].strip().split(","):
                    if ceB:
                        self.ce_blackL.append(ceB.strip())

            # se related
            if ('EDG.se_white_list' in self.cfg_params) and (self.cfg_params['EDG.se_white_list']):
                for seW in self.cfg_params['EDG.se_white_list'].strip().split(","):
                    if seW:
                        self.se_whiteL.append( re.compile(seW.strip().lower()) )

            if ('EDG.se_black_list' in self.cfg_params) and (self.cfg_params['EDG.se_black_list']):
                for seB in self.cfg_params['EDG.se_black_list'].strip().split(","):
                    if seB:
                         self.se_blackL.append( re.compile(seB.strip().lower()) )

        # actual submission
        try:
            self.submissionDriver()
        except Exception, e:
            self.log.info( traceback.format_exc() )
            self.sendResult(66, "Worker exception. Free-resource message", \
                    "WorkerError %s. Task %s."%(self.myName, self.taskName) )
        return

####################################
    # Submission methods 
####################################
    
    def submissionDriver(self):
        taskObj = None
        newRange = None
        skipped = None

        ## not the proper submission handler
        if self.submissionKind not in ['first', 'subsequent']:
            self.sendResult(10, "Bad submission manager for %s. This kind of submission should not be handled here."%(self.taskName), \
                    "WorkerError %s. Wrong submission manager for %s."%(self.myName, self.taskName) )
            self.local_ms.publish("CrabServerWorkerComponent:CrabWorkFailed", self.taskName)
            self.local_ms.commit()
            return

        if self.submissionKind == 'first':
            ## create a new task object in the boss session and register its jobs to PA core
            self.local_ms.publish("CrabServerWorkerComponent:TaskArrival", self.taskName)
            self.local_ms.commit()
          
            try: 
                taskObj = self.blDBsession.declare(self.taskXML, self.proxy)
                self.log.info('Worker %s submitting a new task'%self.myName)
            except Exception, e:
                self.log.info('Worker %s failed to declare task. Checking if already registered'%self.myName)
                self.log.info( traceback.format_exc() )

                # force the other computation branch
                self.submissionKind = 'subsequent'
                taskObj = None 
                pass

        if taskObj == None and self.submissionKind == 'subsequent':
            ## retrieve the task from the boss session 
            self.local_ms.publish("CrabServerWorkerComponent:CommandArrival", self.taskName)
            self.local_ms.commit()

            try:  
                taskObj = self.blDBsession.loadTaskByName(self.cmdXML.getAttribute('Task'))
            except Exception, e:
                self.log.info("Error loading %s"%str(self.cmdXML.getAttribute('Task')) )
                taskObj = None
                pass

            self.log.info('Worker %s submitting a new command on a task'%self.myName)

            # resubmission of retrieved jobs
            meaningfulRng = eval(str(self.cmdXML.getAttribute('Range')), {}, {}) 
            if taskObj is not None:
                needUpd = False
                for j in taskObj.jobs:
                    if j.runningJob['closed'] == 'Y' and j['jobId'] in meaningfulRng:  
                        needUpd = True  
                        self.blDBsession.getNewRunningInstance(j)
                        j.runningJob['status'] = 'C'
                        j.runningJob['statusScheduler'] = 'Created'
                if needUpd:
                    self.blDBsession.updateDB(taskObj)  

        ## failed to load
        if taskObj is None:
            self.sendResult(11, "Unable to retrieve task %s. Causes: loadTaskByName"%(self.taskName), \
                "WorkerError %s. Requested task %s does not exist."%(self.myName, self.taskName) )
            self.local_ms.publish("CrabServerWorkerComponent:CrabWorkFailed", self.taskName)
            self.local_ms.commit()
            return

        ## Go on with the submission
        self.blSchedSession = BossLiteAPISched(self.blDBsession, self.schedulerConfig)
        newRange, skippedJobs = self.preSubmissionCheck(taskObj, str(self.cmdXML.getAttribute('Range')) )

        submittedJobs = []
        nonSubmittedJobs = [] + newRange
        matched = []
        unmatched = []
 
        if len(newRange) > 0:
            sub_jobs, reqs_jobs, matched, unmatched = self.submissionListCreation(taskObj, newRange) 
            if len(matched) > 0:
                submittedJobs, nonSubmittedJobs = self.submitTaskBlocks(taskObj, sub_jobs, reqs_jobs, matched)
            else:
                self.log.info('Worker %s unable to submit jobs. No sites matched '%self.myName)
        else:
            self.log.info('Worker %s unable to submit jobs. File missing on SE'%self.myName)

        self.evaluateSubmissionOutcome(taskObj, newRange, submittedJobs, unmatched, nonSubmittedJobs, skippedJobs)
        return 

    def preSubmissionCheck(self, task, rng):
        newRange = eval(rng)  # a-la-CRAB range expansion and Boss ranges (1 starting)
        doNotSubmitStatusMask = ['R','S'] # ,'K','Y','D'] # to avoid resubmission of final state jobs
        tryToSubmitMask = ['C', 'A', 'RC', 'Z'] + ['K','Y','D','E']
        skippedSubmissions = []

        ## check if the input sandbox is already on the right SE 
        taskFileList = task['globalSandbox'].split(',')
        try:
            for f in taskFileList:
                remoteFile = os.path.join( str(self.cfg_params['CRAB.se_remote_dir']), f)
                checkCount = 3

                fileFound = False 
                while (checkCount > 0):
                    seEl = SElement(self.SEurl, self.SEproto, self.SEport)
                    sbi = SBinterface( seEl )
                    fileFound = sbi.checkExists(remoteFile, self.proxy)
                    if fileFound == True:
                        break
                    checkCount -= 1
                    self.log.info("Worker %s. Checking file %s"%(self.myName, remoteFile))
                    time.sleep(15) 
                    pass

                if fileFound == False: 
                    self.log.info("Worker %s. Missing file %s"%(self.myName, remoteFile)) 
                    return [], newRange

            # get TURL for WMS bypass
            if len(taskFileList) > 0: 
                self.TURLpreamble = sbi.getTurl( taskFileList[0], self.proxy )
                self.TURLpreamble = self.TURLpreamble.split(taskFileList[0])[0]
                if self.TURLpreamble:
                    if self.TURLpreamble[-1] != '/':
                        self.TURLpreamble += '/' 
           
        except Exception, e:
            self.log.info( traceback.format_exc() )
            return [], newRange
        
        for j in task.jobs:
            if (j['jobId'] in newRange):
                try:
                    # do not submit already running or scheduled jobs
                    if j.runningJob['status'] in doNotSubmitStatusMask:
                        newRange.remove(j['jobId'])
                        continue

                    # trace unknown state jobs and skip them from submission
                    if j.runningJob['status'] not in tryToSubmitMask:
                        newRange.remove(j['jobId'])
                        skippedSubmissions.append(j['jobId'])
                except Exception, e:
                    self.log.info("Worker %s. Problem inspecting task %s job %s. Won't be submitted"%(self.myName, \
                                self.taskName, j['name']) )
                    self.log.debug( traceback.format_exc() )
                    newRange.remove(j['jobId'])
                    skippedSubmissions.append(j['jobId'])

        return newRange, skippedSubmissions

    def submitTaskBlocks(self, task, sub_jobs, reqs_jobs, matched):
        unsubmitted = [] # list of all the jId hadled. Used to check the number of successful jubmissions
        submitted = [] 

        # modify sandbox and other paths for WMS bypass
        self.log.debug("Worker %s. Reference TURL: %s"%(self.myName,self.TURLpreamble) )
        if self.TURLpreamble and (self.TURLpreamble not in task['outputDirectory']):
            task['startDirectory'] = self.TURLpreamble
            destDir = task['outputDirectory']
            task['outputDirectory'] = self.TURLpreamble + destDir
            task['scriptName'] = self.TURLpreamble + task['scriptName']
            task['cfgName'] = self.TURLpreamble + task['cfgName']

        if not self.submissionKind == 'first': 
            # backup for job output (tgz files only, less load)
            fullSubJob = []
            for sub in sub_jobs:  
                fullSubJob += sub
  
            for jid in task.jobs:
                if jid not in fullSubJob:
                    continue
                seEl = SElement(self.SEurl, self.SEproto, self.SEport)
                sbi = SBinterface( seEl )
                outcomes = [ str(task['outputDirectory']+'/'+f).replace(self.TURLpreamble, '/') for f in job['outputFiles'] if 'tgz' in f ]
                bk_sbi = SBinterface( seEl, copy.deepcopy(seEl) )

                for orig in outcomes:
                    try:
                        if sbi.checkExists(orig, task['user_proxy']) == True:
                            # create a backup copy
                            replica = orig+'.'+str(job['submissionNumber'])
                            bk_sbi.copy( source=orig, dest=replica, proxy=task['user_proxy'])
                    except Exception, ex:
                        self.log.info("Problem while creating back-up copy for %s: %s"%(self.myName, orig))
                        self.log.info( traceback.format_exc() )
                        continue
            pass

        # file lists correction 
        for jid in xrange(len(task.jobs)):
            gsiOSB = [ os.path.basename(of) for of in  task.jobs[jid]['outputFiles']  ]
            task.jobs[jid]['outputFiles'] = gsiOSB 

            if 'crab_fjr_%d.xml'%(jid+1) not in task.jobs[jid]['outputFiles']:
                task.jobs[jid]['outputFiles'].append( 'crab_fjr_%d.xml'%(jid+1) ) #'file://' + destDir +'_spec/crab_fjr_%d.xml'%(jid+1) )

            if '.BrokerInfo' not in task.jobs[jid]['outputFiles']:
                task.jobs[jid]['outputFiles'].append( '.BrokerInfo' )

        self.blDBsession.updateDB(task) 

        ##send here pre submission info to ML DS
        self.SendMLpre(task)

        # loop and submit blocks
        for ii in matched:
            unsubmitted += sub_jobs[ii]
            ##############  SplitCollection if too big DS
            sub_bulk = []
            bulk_window = 200
            if len(sub_jobs[ii]) > bulk_window:
                n_sub_bulk = int( int(len(sub_jobs[ii]) ) / bulk_window ) 
                for n in xrange(n_sub_bulk):
                    first = n*bulk_window
                    last = (n+1)*bulk_window
                    if last > len(sub_jobs[ii]):
                        last = len(sub_jobs[ii])
  
                    sub_bulk.append(sub_jobs[ii][first:last])

                if len(sub_jobs[ii][last:-1]) < bulk_window/8:
                    for pp in sub_jobs[ii][last:-1]:
                        sub_bulk[n_sub_bulk-1].append(pp)

                self.log.info("Worker check: the collection is too Big..splitted in %s sub_collection"%(n_sub_bulk))
            ###############
            try:
                if len(sub_bulk)>0:
                    count = 1
                    for sub_list in sub_bulk: 
                        self.blSchedSession.submit(task['id'], sub_list,reqs_jobs[ii])
                        self.log.info("Worker submitted sub collection # %s "%(count))
                        count += 1
                    task =  self.blDBsession.load( task['id'], sub_jobs[ii] )[0]
                else:

                    task = self.blSchedSession.submit(task['id'], sub_jobs[ii], reqs_jobs[ii])
            except Exception, e:
                self.log.info("Worker %s. Problem submitting task %s jobs %s. %s"%(self.myName, self.taskName, str(sub_jobs[ii]), str(e)))
                continue

            # check if submitted
            parentIds = {}
            for j in task.jobs: 
                self.blDBsession.getRunningInstance(j)
                if j.runningJob['schedulerId']:
                    parentIds[str(j.runningJob['schedulerParentId'])] = ''
                    submitted.append(j['jobId'])
                    if j['jobId'] in unsubmitted:
                        unsubmitted.remove(j['jobId'])

                    j.runningJob['status'] = 'S'
                    j.runningJob['statusScheduler'] = 'Submitted'
 
           
            parentIds = ','.join(parentIds.keys()) 
            self.log.info("Parent IDs for task %s: %s"%(self.taskName, parentIds) )
            self.blDBsession.updateDB( task )
            ## send here post submission info to ML DS
            self.SendMLpost( task, sub_jobs[ii] )
            # note: this must be done after update, otherwise jobIds will be None
        return submitted, unsubmitted 

####################################
    # Resubmission driver
####################################
    def resubmissionDriver(self):
        # load the task
        try:
            task = self.blDBsession.load(self.taskId, self.jobId)[0]
        except Exception, e:
            task = None
            pass
        
        if not task or len(task.jobs)==0:
            status = 6
            reason = "Error loading task for %s, the attempts will be stopped"%self.myName
            self.sendResult(status, reason, reason)
            self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
            self.local_ms.commit()
            return
 
        job = task.jobs[0]
        self.blDBsession.getRunningInstance(job)

        # no more attempts  
        dbCfg = copy.deepcopy(dbConfig)
        dbCfg['dbType'] = 'mysql'
        Session.set_database(dbCfg)

        jobInfo = {}
        try:
            Session.connect(self.taskName)
            jobInfo.update( wfJob.get(job['name']) )
            Session.close(self.taskName)
        except Exception, e:
            self.log.info("Error while getting WF-Entities for job %s"%job['name'])
            # force the following condition to be true
            jobInfo['retries'] = 1
            jobInfo['max_retries'] = 0
            pass  
 
        if int(jobInfo['retries']) >= int(jobInfo['max_retries']):
            status = 6
            reason = "No more attempts for resubmission for %s, the attempts will be stopped"%self.myName
            self.sendResult(status, reason, reason)
            self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
            self.local_ms.commit()
            return

        # TODO to be cleaned
        # get the scheduler name used by listCreation
        self.schedName = 'glite'
        if 'condor' in str(job.runningJob['scheduler']).lower():
           self.schedName = 'condor_g'
        elif 'lsf' in str(job.runningJob['scheduler']).lower():
           self.schedName = 'lsf'
        elif 'arc' in str(job.runningJob['scheduler']).lower():
           self.schedName = 'arc'
        else:
           self.schedName = 'glite'

        schedulerConfig = {'name' : job.runningJob['scheduler'], 'user_proxy' : task['user_proxy'] }
        try:
            self.blSchedSession = BossLiteAPISched( self.blDBsession, schedulerConfig )
        except Exception, e:
            status = 6
            reason = "Error allocating SchedSession for %s, resubmission attempt will not be processed"%self.myName
            self.sendResult(status, reason, reason)
            self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
            self.local_ms.commit()
            return

        self.blDBsession.getNewRunningInstance(job)

        # recreate auxiliary infos from old dictionary
        self.cfg_params = {}
        self.cmdXML = None
        taskDir = os.path.join( self.wdir, (task['name'] + '_spec/') )
        try:
            cmdSpecFile = taskDir + 'cmd.xml'
            doc = minidom.parse(cmdSpecFile)
            self.cmdXML = doc.getElementsByTagName("TaskCommand")[0]
            self.cfg_params.update( eval( self.cmdXML.getAttribute("CfgParamDict") ) )

            # Prepare filter lists for matching sites
            if 'glite' in self.schedName:
                # ce related
                if ('EDG.ce_white_list' in self.cfg_params) and (self.cfg_params['EDG.ce_white_list']):
                    for ceW in self.cfg_params['EDG.ce_white_list'].strip().split(","):
                        if ceW:
                            self.ce_whiteL.append(ceW.strip())

                if ('EDG.ce_black_list' in self.cfg_params) and (self.cfg_params['EDG.ce_black_list']):
                    for ceB in self.cfg_params['EDG.ce_black_list'].strip().split(","):
                        if ceB:
                            self.ce_blackL.append(ceB.strip())

                # se related
                if ('EDG.se_white_list' in self.cfg_params) and (self.cfg_params['EDG.se_white_list']):
                    for seW in self.cfg_params['EDG.se_white_list'].strip().split(","):
                        if seW:
                            self.se_whiteL.append( re.compile(seW.strip().lower()) )

                if ('EDG.se_black_list' in self.cfg_params) and (self.cfg_params['EDG.se_black_list']):
                    for seB in self.cfg_params['EDG.se_black_list'].strip().split(","):
                        if seB:
                            self.se_blackL.append( re.compile(seB.strip().lower()) )

        except Exception, e:
            status = 6
            reason = "Error parsing command XML for %s, resubmission attempt will not be processed"%self.myName
            self.sendResult(status, reason, reason)
            self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
            self.local_ms.commit()
            return

        # simple container to move _small_ portions of the cfg to server
        self.EDG_retry_count = int(self.cfg_params['EDG_retry_count'])
        self.EDG_shallow_retry_count = int(self.cfg_params['EDG_shallow_retry_count'])
 
        # get the turl
        seEl = SElement(self.SEurl, self.SEproto, self.SEport)
        sbi = SBinterface( seEl )
        taskFileList = task['globalSandbox'].split(',')
        self.TURLpreamble = sbi.getTurl( taskFileList[0], self.proxy )
        self.TURLpreamble = self.TURLpreamble.split(taskFileList[0])[0]
        if self.TURLpreamble:
            if self.TURLpreamble[-1] != '/':
                self.TURLpreamble += '/'

        # submit once again 
        sub_jobs, reqs_jobs, matched, unmatched = self.submissionListCreation(task, [int(self.jobId)])
        self.log.info('Worker %s listmatched jobs, now submitting'%self.myName)

        submittedJobs = []
        nonSubmittedJobs = [int(self.jobId)]
        skippedJobs = []

        if len(matched) > 0:
            submittedJobs, nonSubmittedJobs = self.submitTaskBlocks(task, sub_jobs, reqs_jobs, matched)
        else:
            self.log.info('Worker %s unable to submit jobs. No sites matched'%self.myName)

        self.evaluateSubmissionOutcome(task, [int(self.jobId)], submittedJobs, unmatched, nonSubmittedJobs, skippedJobs)

        # increase the resubmit counter
        dbCfg = copy.deepcopy(dbConfig)
        dbCfg['dbType'] = 'mysql'

        Session.set_database(dbCfg)
        Session.connect(self.taskName)
        Session.start_transaction(self.taskName)

        # TODO avoid SQL interactions 
        sqlStr="UPDATE we_Job SET retries=retries+1 WHERE id='%s' "%job['name']
        Session.execute(sqlStr)

        Session.commit(self.taskName)
        Session.close(self.taskName)

        return   

####################################
    # Results propagation methods
####################################

    def evaluateSubmissionOutcome(self, taskObj, submittableRange, submittedJobs, \
            unmatchedJobs, nonSubmittedJobs, skippedJobs):

        resubmissionList = list( set(submittableRange).difference(set(submittedJobs)) )

        self.log.info("Worker. Task %s (%d jobs): submitted %d unmatched %d notSubmitted %d skipped %d"%(self.taskName, \
            len(submittableRange), len(submittedJobs), len(unmatchedJobs), len(nonSubmittedJobs), len(skippedJobs) )    )
        self.log.debug("Task %s\n"%self.myName + "jobs : %s \nsubmitted %s \nunmatched %s\nnotSubmitted %s\nskipped %s"%(str(submittableRange), \
            str(submittedJobs), str(unmatchedJobs), str(nonSubmittedJobs), str(skippedJobs) )   )

        ## if all the jobs have been submitted send a success message
        if len(resubmissionList) == 0 and len(unmatchedJobs + nonSubmittedJobs + skippedJobs) == 0:
            # update state in WE for succesfull jobs OR insert a new entry if first submission 
            if self.registerTask(taskObj) != 0:
                self.sendResult(10, "Unable to register task %s. Causes: deserialization, saving, registration "%(self.taskName), \
                    "WorkerError %s. Error while registering jobs for task %s."%(self.myName, self.taskName) )
                self.local_ms.publish("CrabServerWorkerComponent:CrabWorkFailed", self.taskName)
                self.local_ms.commit()
                return

            # diffuse success communication
            self.sendResult(0, "Full Success for %s"%self.taskName, "Worker. Successful complete submission for task %s"%self.taskName )
            self.local_ms.publish("CrabServerWorkerComponent:CrabWorkPerformed", taskObj['name'])
            self.local_ms.commit()

            dbCfg = copy.deepcopy(dbConfig)
            dbCfg['dbType'] = 'mysql'
            Session.set_database(dbCfg)
            Session.connect(self.taskName)
            Session.start_transaction(self.taskName)

            for j in taskObj.jobs:
                if j['jobId'] in submittedJobs:
                    try:
                        if wfJob.exists(j['name']) and not (wfJob.get(j['name'])['status'] == 'inProgress'):
                            #wfJob.setState(j['name'], 'submit')
                            wfJob.setState(j['name'], 'inProgress')
                    except Exception, e:
                        continue  

            Session.commit(self.taskName)
            Session.close(self.taskName)
            return

        elif self.submissionKind == 'errHdlTriggered':
            self.sendResult(55, "Unable to resubmit single job as requested by ErrorHandler for task %s"%self.taskName, \
                    "Worker %s. Unable to resubmit task %s as requested by ErrorHandler"%(self.myName, self.taskName))
            return

        else:
            ## some jobs need to be resubmitted later
            if len(submittedJobs) == 0:
                self.sendResult(-1, "Any jobs submitted for task %s"%self.taskName, \
                    "Worker %s. Any job submitted: %d more attempts \
                    will be performed"%(self.myName, self.resubCount))
            else:
                self.local_ms.publish("CrabServerWorkerComponent:CrabWorkPerformedPartial", self.taskName)
                self.sendResult(-2, "Partial Success for %s"%self.taskName, \
                    "Worker %s. Partial submission: %d more attempts \
                     will be performed"%(self.myName, self.resubCount))

            # propagate the re-submission attempt
            self.cmdXML.setAttribute('Range', ','.join(map(str, resubmissionList)) )
            if self.resubCount <= 0:
                # determine the failure reason
                reason = "simplyFailed"
                if len(unmatchedJobs)>0:
                    reason = "unmatched"  
                elif len(nonSubmittedJobs)>0:
                    reason = "wmsNotSubmitted"
                elif len(skippedJobs)>0:
                    reason = "ioSkipped"

                # SubmissionFailed message for ErrHandler
                self.local_ms.publish("SubmissionFailed", "%s::%s::%s"%(self.taskName, reason, str(resubmissionList)) )
                self.local_ms.commit()

                try:
                    self.registerTask(taskObj)

                    jobSpecId = []
                    toMarkAsFailed = list(set(resubmissionList+unmatchedJobs + nonSubmittedJobs + skippedJobs))
                    for j in taskObj.jobs:
                        if j['jobId'] in toMarkAsFailed:
                            jobSpecId.append(j['name'])

                    dbCfg = copy.deepcopy(dbConfig)
                    dbCfg['dbType'] = 'mysql'
                    Session.set_database(dbCfg)
                    Session.connect(self.taskName)
                    Session.start_transaction(self.taskName)

                    JobState.doNotAllowMoreSubmissions(jobSpecId)
                    for jId in jobSpecId:
                            wfJob.setState(jId, 'reallyFinished') #'failed')

                    Session.commit(self.taskName)
                    Session.close(self.taskName)
                except Exception,e:
                    self.log.info("Unable to mark failed jobs in WorkFlow Entities ")
                    self.log.info( traceback.format_exc() )

                # Give up message
                status = 10
                reason = "Command for task %s has no more attempts. Give up."%self.taskName
                self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
                self.local_ms.commit()
                self.log.info("Worker %s has no more attempts: give up with task %s"%(self.myName, self.taskName) )
                return 

            self.resubCount -= 1
            payload = self.taskName+"::"+str(self.resubCount)
            self.local_ms.publish("CRAB_Cmd_Mgr:NewCommand", payload, "00:00:10")
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

        # register in workflow  
        try:
            dbCfg = copy.deepcopy(dbConfig)
            dbCfg['dbType'] = 'mysql'

            Session.set_database(dbCfg)
            Session.connect(self.taskName)
            Session.start_transaction(self.taskName)

            for job in taskArg.jobs:
                jobName = job['name']
                cacheArea = self.wdir + '/' + self.taskName + '_spec/%s'%jobName
                #jobDetails = {'id':jobName, 'job_type':'Processing', 'max_retries':self.maxRetries, 'max_racers':(self.maxRetries+1), 'owner':self.taskName}
                jobDetails = {'id':jobName, 'job_type':'Processing', 'max_retries':self.maxRetries, 'max_racers':1, 'owner':self.taskName}

                try:
                    if not wfJob.exists(jobName):
                        wfJob.register(jobName, None, jobDetails)
                        wfJob.setState(jobName, 'register')
                        wfJob.setState(jobName, 'create')
                        wfJob.setCacheDir(jobName, cacheArea)
                        # wfJob.setState(jobName, 'inProgress')
                except Exception, e:
                    self.log.info('Error checking if job is already registered in WF-Entities.')
                    continue
 
            Session.commit(self.taskName)
            Session.close(self.taskName)
        except Exception, e:
            self.log.info('Error while registering job for JT: %s'%self.taskName)
            self.log.info( traceback.format_exc() )
            Session.rollback(self.taskName)
            Session.close(self.taskName)
            return 1
        return 0    

    def submissionListCreation(self, taskObj, jobRng):
        '''
           Matchmaking process. Inherited from CRAB-SA
        '''

        sub_jobs = []      # list of jobs Id list to submit
        requirements = []  # list of requirements for the submitting jobs

        # group jobs by destination
        # distinct_dests = self.queryDistJob_Attr(taskObj['id'], 'dlsDestination', 'jobId', jobRng)
        distinct_dests = []
        for j in taskObj.jobs:
           if not isinstance(j['dlsDestination'], list):
              j['dlsDestination'] = eval(j['dlsDestination'])
           if  j['dlsDestination'] not in distinct_dests:
               distinct_dests.append( j['dlsDestination'] )
        ##

        jobs_to_match = []
        all_jobs = []
        count = 0

        for distDest in distinct_dests:
             # get job_ids for a specific destination 
             jobs_per_dest = []
             for j in taskObj.jobs:
                 if str(distDest) == str( j['dlsDestination'] ):
                     jobs_per_dest.append(j['jobId'])
             all_jobs.append( [] + jobs_per_dest )
             
             # prune by range 
             sub_jobs_temp = []
             for i in jobRng:
                 if i in all_jobs[count]: 
                     sub_jobs_temp.append(i)

             # select match candidate
             if len(sub_jobs_temp)>0:
                 sub_jobs.append(sub_jobs_temp)
                 jobs_to_match.append(sub_jobs[count][0])
                 count += 1
             pass

        # ListMatch 
        sel = 0
        matched = []
        unmatched = []

        for id_job in jobs_to_match:
            # Get JDL requirements
            if "GLITE" in self.schedName.upper(): 
                tags_tmp = str(taskObj['jobType']).split('"')
                tags = [str(tags_tmp[1]),str(tags_tmp[3])]
                requirements.append( self.sched_parameter_Glite(id_job, taskObj) )
            elif self.schedName.upper()== "CONDOR_G":
                requirements.append( self.sched_parameter_Condor() )
            elif self.schedName.upper() in [ "LSF", "CAF"]:
                requirements.append( self.sched_parameter_Lsf(id_job, taskObj) )
                tags = ''
            else:
                continue
            
            # Perform listMatching
            if self.schedName.upper() != "CONDOR_G" or self.submissionKind != 'errHdlTriggered':
                cleanedList = None
                if len(distinct_dests[sel])!=0:
                    cleanedList = self.checkWhiteList(self.checkBlackList(distinct_dests[sel],''),'')

                sites = self.blSchedSession.lcgInfo(tags, seList=cleanedList, blacklist=self.ce_blackL, whitelist=self.ce_whiteL) 
                match = len( sites )
            else:
                match = "1"
            if match:
               matched.append(sel)
            else:
               unmatched.append(sel)
            sel += 1

        # all done and matched, go on with the submission 
        return sub_jobs, requirements, matched, unmatched

#########################################################
### Matching auxiliary methods
#########################################################

    def checkWhiteList(self, Sites, fileblocks):
        """
        select sites that are defined by the user (via SE white list)
        """
        if len(self.se_whiteL)==0: return Sites
        goodSites = []
        for aSite in Sites:
            good=0
            for re in self.se_whiteL:
                if re.search(string.lower(aSite)):
                    good=1
                pass
            if good: goodSites.append(aSite)

        return goodSites

    def checkBlackList(self, Sites, fileblocks):
        """
        select sites that are not excluded by the user (via SE black list)
        """
        goodSites = []
        for aSite in Sites:
            good=1
            for re in self.se_blackL:
                if re.search(string.lower(aSite)):
                    good=0
                pass
            if good: goodSites.append(aSite)
        return goodSites

    def SendMLpre(self, task):
        """
        Send Pre info to ML
        """
        params = self.collect_MLInfo(task)
        params['jobId'] = 'TaskMeta'
        self.apmon.sendToML(params)
        return

    def collect_MLInfo(self, taskObj):
        """
        Preapre DashBoard information
        """
        taskId=str("_".join(str(taskObj['name']).split('_')[:-1]))

        gridName = self.cmdXML.getAttribute('Subject')
        # rebuild flat gridName string (pruned from SSL print and delegation adds)
        gridName = '/'+"/".join(gridName.split('/')[1:-1])
        taskType = 'analysis'
        # version
        datasetPath = self.cfg_params['CMSSW.datasetpath']
        if datasetPath.lower() == 'none':
            datasetPath = None
        VO = self.cfg_params['VO']
        executable = self.cfg_params.get('CMSSW.executable','cmsRun')

        params = {'tool': 'crab',\
                  'JSToolVersion': os.environ['CRAB_SERVER_VERSION'], \
                  'tool_ui': os.environ['HOSTNAME'], \
                  'scheduler': self.schedName, \
                  'GridName': str(gridName), \
                  'taskType': taskType, \
                  'vo': VO, \
                  'user': self.taskName.split('_')[0], \
                  'taskId': taskId, \
                  'datasetFull': datasetPath, \
                  'application': os.environ['CRAB_SERVER_VERSION'], \
                  'exe': executable }

        return params

    def SendMLpost(self, taskFull, allList):
        """
        Send post-submission info to ML
        """

        task = self.blDBsession.load(taskFull, allList)[0]
        params = {}
        for k,v in self.collect_MLInfo(task).iteritems():
            params[k] = v

        taskId = str("_".join(str(task['name']).split('_')[:-1]))
        Sub_Type = 'Server'

        for job in task.jobs:
            jj = job['jobId']
            jobId = ''
            localId = ''
            jid = str(job.runningJob['schedulerId'])

            if self.schedName.upper() == 'CONDOR_G':
                hash = self.cfg_params['cfgFileNameCkSum'] #makeCksum(common.work_space.cfgFileName())
                rb = 'OSG'
                jobId = str(jj) + '_' + hash + '_' + jid
            elif self.schedName in ['lsf', 'caf']:
                jobId = "https://"+self.schedName+":/" + jid + "-" + taskId.replace("_", "-")
                rb = self.schedName
                localId = jid
            else:
                jobId = str(jj) + '_' + str(jid)
                rb = str(job.runningJob['service'])

            dlsDest = job['dlsDestination'] # this is surely a list, see before
            if len(dlsDest) <= 2 :
                T_SE = ','.join(dlsDest)
            else :
                T_SE = str(len(dlsDest))+'_Selected_SE'

            infos = { 'jobId': jobId, \
                      'sid': jid, \
                      'broker': rb, \
                      'bossId': jj, \
                      'SubmissionType': Sub_Type, \
                      'TargetSE': T_SE, \
                      'localId' : localId}

            for k,v in infos.iteritems():
                params[k] = v

            self.apmon.sendToML(params)
        return


#########################################################
### Specific Scheduler requirements parameters
#########################################################
    def sched_parameter_Condor(self):
        return

    def sched_parameter_Lsf(self, i, task):

        sched_param= ''
        resDir= "/".join((task['globalSandbox'].split(',')[0]).split('/')[:-1])  
        queue =  'cmscaf' ### ToBeAddede in cfg.xml file
        res = 'cmscaf'
        if (queue):
            sched_param += '-q '+queue +' '
            if (res): sched_param += ' -R '+res +' '
        pass

       # sched_param+='-cwd '+resDir + ' '
        return sched_param

        return

    def sched_parameter_Glite(self, i, task):
        if self.submissionKind != 'errHdlTriggered':
            dest = task.jobs[i-1]['dlsDestination'] # shift due to BL ranges 
        else:
            dest = task.jobs[0]['dlsDestination']

        sched_param = 'Requirements = ' + task['jobType'] 

        req=''

        if self.cfg_params['EDG.max_wall_time']:
            req += 'other.GlueCEPolicyMaxWallClockTime>=' + self.cfg_params['EDG.max_wall_time']
        if self.cfg_params['EDG.max_cpu_time']:
            if (not req == ' '): req = req + ' && '
            req += ' other.GlueCEPolicyMaxCPUTime>=' + self.cfg_params['EDG.max_cpu_time']

        sched_param += req + self.se_list(i, dest) + self.ce_list() +';\n' ## BL--DS
        #if self.EDG_addJdlParam: 
        #    sched_param+=self.jdlParam() ## BL--DS
        sched_param+='MyProxyServer = "' + self.cfg_params['proxyServer'] + '";\n'
        sched_param+='VirtualOrganisation = "' + self.cfg_params['VO'] + '";\n'
        sched_param+='RetryCount = '+str(self.EDG_retry_count)+';\n'
        sched_param+='ShallowRetryCount = '+str(self.EDG_shallow_retry_count)+';\n'
        return sched_param

#########################################################
    def findSites_(self, n, sites):
        itr4 =[]
        if len(sites)>0 and sites[0]=="":
            return itr4

        if sites != [""]:
            ##Addedd Daniele
            replicas = self.checkBlackList(sites,n)
            if len(replicas)!=0:
                replicas = self.checkWhiteList(replicas,n)

            itr4 = replicas
            #####
        return itr4

    def se_list(self, id, dest):
        """
        Returns string with requirement SE related
        """
        hostList = self.findSites_(id, dest)
        req=''
        reqtmp=[]
        concString = '||'

        for arg in hostList:
            reqtmp.append(' Member("'+arg+'" , other.GlueCESEBindGroupSEUniqueID) ')

        if len(reqtmp): req += " && (" + concString.join(reqtmp) + ") "
        return req

    def ce_list(self):
        """
        Returns string with requirement CE related
        """
        req = ''
        if self.ce_whiteL:
            tmpCe=[]
            concString = '&&'
            for ce in self.ce_whiteL:
                tmpCe.append('RegExp("' + str(ce).strip() + '", other.GlueCEUniqueId)')
            if len(tmpCe) == 1:
                req +=  " && (" + concString.join(tmpCe) + ") "
            elif len(tmpCe) > 1:
                firstCE = 0
                for reqTemp in tmpCe:
                    if firstCE == 0:
                        req += " && ( (" + reqTemp + ") "
                        firstCE = 1
                    elif firstCE > 0:
                        req += " || (" + reqTemp + ") "
                if firstCE > 0:
                    req += ") "

        if self.ce_blackL:
            tmpCe=[]
            concString = '&&'
            for ce in self.ce_blackL:
                tmpCe.append('(!RegExp("' + str(ce).strip() + '", other.GlueCEUniqueId))')
            if len(tmpCe): req += " && (" + concString.join(tmpCe) + ") "

        # requirement added to skip gliteCE
        req += '&& (!RegExp("blah", other.GlueCEUniqueId))'
        return req



