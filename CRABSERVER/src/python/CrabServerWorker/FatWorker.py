#!/usr/bin/env python
"""
_FatWorker_

Implements thread logic used to perform the actual Crab task submissions.

"""

__revision__ = "$Id: FatWorker.py,v 1.126.2.1 2008/10/30 20:14:32 ewv Exp $"
__version__ = "$Revision: 1.126.2.1 $"
import string
import sys, os
import time
from threading import Thread
from MessageService.MessageService import MessageService
from xml.dom import minidom
import traceback
import copy
import re

# CW DB API 
from CrabWorkerAPI import CrabWorkerAPI

# BossLite import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched

from ProdCommon.BossLite.Common.BossLiteLogger import BossLiteLogger
from ProdCommon.BossLite.Common.Exceptions import BossLiteError

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser
from WMCore.SiteScreening.BlackWhiteListParser import CEBlackWhiteListParser

# CRAB dependencies
from CrabServer.ApmonIf import ApmonIf

class FatWorker(Thread):
    def __init__(self, logger, FWname, threadAttributes):

        Thread.__init__(self)

        ## Worker Properties
        self.tInit = time.time()
        self.log = logger
        self.myName = FWname
        self.configs = threadAttributes

        self.taskName = self.configs['taskname']
        self.wdir = self.configs['wdir']
        self.resubCount = int(self.configs['retries'])
        self.cmdRng = [] + self.configs['submissionRange']

        self.se_blackL = [] + self.configs['se_dynBList']
        self.se_whiteL = []
        self.ce_blackL = [] + self.configs['ce_dynBList']
        self.ce_whiteL = []
        self.wmsEndpoint = self.configs['wmsEndpoint']
        self.local_queue = self.configs['messageQueue']

        ##Initialization to allow lsf@caf
        self.cpCmd = self.configs['cpCmd']
        self.rfioServer = self.configs['rfioServer']

        self.seEl = SElement(self.configs['SEurl'], self.configs['SEproto'], self.configs['SEport'])
        self.blDBsession = BossLiteAPI('MySQL', dbConfig, pool=self.configs['blSessionPool'])
        self.blSchedSession = None
        self.apmon = ApmonIf()

        ## CW DB init
        self.cwdb = CrabWorkerAPI( self.blDBsession.bossLiteDB )

        try:
            self.start()
        except Exception, e:
            self.log.info('FatWorker exception : %s'%self.myName)
            self.log.info( traceback.format_exc() )
        self.apmon.free()

        return

    def run(self):
        self.log.info("FatWorker %s initialized: task %s"%(self.myName, self.taskName) )
        taskObj = None
        self.local_queue.put((self.myName, "CrabServerWorkerComponent:CommandArrival", self.taskName))
        if not self.parseCommandXML() == 0:
            return

        self.log.info("FatWorker %s loading task"%self.myName)
        try:
            taskObj = self.blDBsession.loadTaskByName(self.taskName)
        except Exception, e:
            self.sendResult(11, "Unable to retrieve task %s. Causes: loadTaskByName"%(self.taskName), \
                "WorkerError %s. Requested task %s does not exist."%(self.myName, self.taskName) )
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:CrabWorkFailed", self.taskName))
            return

        self.log.info("FatWorker %s allocating submission system session"%self.myName)
        if not self.allocateBossLiteSchedulerSession(taskObj) == 0:
            return

        self.log.info('FatWorker %s preparing submission'%self.myName)
        errStatus, errMsg = (66, "Worker exception. Free-resource message")
        try:
            newRange, skippedJobs = self.preSubmissionCheck(taskObj)
            if len(newRange) == 0 :
                raise Exception('Empty range submission temptative')
        except Exception, e:
            self.log.debug( traceback.format_exc() )
            self.sendResult(errStatus, errMsg, "WorkerError %s. Task %s. preSubmissionCheck"%(self.myName, self.taskName) )
            return
                
        try:
            sub_jobs, reqs_jobs, matched, unmatched = self.submissionListCreation(taskObj, newRange)
        except Exception, e:
            self.log.debug( traceback.format_exc() )
            self.sendResult(errStatus, errMsg, "WorkerError %s. Task %s. listMatch"%(self.myName, self.taskName) )
            return

        self.log.info("FatWorker %s performing submission"%self.myName)
        try:
            submittedJobs, nonSubmittedJobs, errorTrace = self.submitTaskBlocks(taskObj, sub_jobs, reqs_jobs, matched)
        except Exception, e:
            self.log.debug( traceback.format_exc() )
            self.sendResult(errStatus, errMsg, "WorkerError %s. Task %s."%(self.myName, self.taskName) )
            return

        self.log.info("FatWorker %s evaluating submission outcomes"%self.myName)
        try:
            ## added blite safe connection to the DB
            self.evaluateSubmissionOutcome(taskObj, newRange, submittedJobs, unmatched, nonSubmittedJobs, skippedJobs)
        except Exception, e:
            self.log.debug( traceback.format_exc() )
            self.sendResult(errStatus, errMsg, "WorkerError %s. Task %s. postSubmission"%(self.myName, self.taskName) )
            return
        self.log.info("FatWorker %s finished %s"%(self.myName, self.taskName) )
        return

    def sendResult(self, status, reason, logMsg):
        self.log.info(logMsg)
        msg = self.myName + "::" + self.taskName + "::"
        msg += str(status) + "::" + reason + "::" + str(time.time() - self.tInit)
        self.local_queue.put((self.myName, "CrabServerWorkerComponent:FatWorkerResult", msg))
        return

    def parseCommandXML(self):
        status = 0
        cmdSpecFile = os.path.join(self.wdir, self.taskName + '_spec/cmd.xml' )
        try:
            doc = minidom.parse(cmdSpecFile)
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]
            self.schedName = str( cmdXML.getAttribute('Scheduler') ).upper()
            ## already set in the message
            # self.cmdRng =  eval( cmdXML.getAttribute('Range'), {}, {} )
            ##
            self.proxySubject = str( cmdXML.getAttribute('Subject') )
            self.cfg_params = eval( cmdXML.getAttribute("CfgParamDict"), {}, {} )

            # se related
            if 'EDG.se_white_list' in self.cfg_params:
                for seW in str(self.cfg_params['EDG.se_white_list']).split(","):
                    if seW: 
                        self.se_whiteL.append(seW.strip()) 
            if 'EDG.se_black_list' in self.cfg_params:
                for seB in self.cfg_params['EDG.se_black_list'].split(","):
                    if seB: 
                        self.se_blackL.append(seB.strip())

            # ce related
            if 'EDG.ce_white_list' in self.cfg_params:
                for ceW in str(self.cfg_params['EDG.ce_white_list']).split(","):
                    if seW: 
                        self.ce_whiteL.append(ceW.strip()) 
            if 'EDG.ce_black_list' in self.cfg_params:
                for ceB in self.cfg_params['EDG.ce_black_list'].split(","):
                    if ceB: 
                        self.ce_blackL.append(ceB.strip())

        except Exception, e:
            status = 6
            reason = "Error while parsing command XML for task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            pload = self.taskName + "::" + str(status) + "::" + reason
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:SubmitNotSucceeded", pload))
        return status

    def allocateBossLiteSchedulerSession(self, taskObj):
        """
        Set scheduler specific parameters and allocate the Scheduler Session
        """

        self.bossSchedName = {'GLITE':'SchedulerGLiteAPI',
                              'GLITECOLL':'SchedulerGLiteAPI',
                              'CONDOR_G':'SchedulerCondorG',
                              'ARC':'arc',
                              'LSF':'SchedulerLsf',
                              'CAF':'SchedulerLsf'}[self.schedName]
        schedulerConfig = {'name': self.bossSchedName, 'user_proxy':taskObj['user_proxy']}

        if schedulerConfig['name'] in ['SchedulerGLiteAPI']:
            schedulerConfig['config'] = self.wdir + '/glite_wms_%s.conf' % self.configs['rb']
            schedulerConfig['skipWMSAuth'] = 1 
            if self.wmsEndpoint:
                schedulerConfig['service'] = self.wmsEndpoint
        elif schedulerConfig['name'] in ['SchedulerGlidein', 'SchedulerCondorG']:
            # FIXME: Get rid of "condorTemp" directory
            condorTemp = os.path.join(self.wdir, self.taskName+'_spec')
            self.log.info('Condor will use %s for temporary files' % condorTemp)
            schedulerConfig['tmpDir'] = condorTemp
            schedulerConfig['useGlexec'] = True
        elif schedulerConfig['name'] == 'arc':
            pass
        elif schedulerConfig['name'] in ['SchedulerLsf']:
            schedulerConfig['cpCmd']   = self.cpCmd
            schedulerConfig['rfioSer'] = self.rfioServer

        try:
            self.blSchedSession = BossLiteAPISched(self.blDBsession, schedulerConfig)
        except Exception, e:
            status = 6
            reason = "Unable to create a BossLite Session because of the following error: %s"%str(e)
            self.sendResult(status, reason, reason)
            return 1
        return 0

####################################
    # Submission methods
####################################
    def preSubmissionCheck(self, task):
        newRange = self.cmdRng
        doNotSubmitStatusMask = ['R','S'] # ,'K','Y','D'] # to avoid resubmission of final state jobs
        tryToSubmitMask = ['C', 'A', 'RC', 'Z'] + ['K','Y','D','E', 'SD']
        skippedSubmissions = []

        # closed running jobs regeneration and osb manipulation
        needUpd = False
        backupFiles = []
        for j in task.jobs:
            if j['jobId'] in self.cmdRng:
                try:
                    if j.runningJob['closed'] == 'Y':
                        # backup for job output (tgz files only, less load)
                        sbi = SBinterface( self.seEl )
                        bk_sbi = SBinterface( self.seEl, copy.deepcopy(self.seEl) )
                        for orig in [ task['outputDirectory']+'/'+f for f in j['outputFiles'] if 'tgz' in f ]:
                            try:
                                bk_sbi.move( source=orig, dest=orig+'.'+str(j['submissionNumber']), proxy=task['user_proxy'])
                            except Exception, ex:
                                continue
                            # track succesfully replicated files
                            backupFiles.append( os.path.basename(orig) )

                        # reproduce closed runningJob instances
                        self.blDBsession.getNewRunningInstance(j)
                        j.runningJob['status'] = 'C'
                        j.runningJob['statusScheduler'] = 'Created'
                        needUpd = True
                except Exception, e:
                    self.log.info("Worker %s. Problem regenerating RunningJob %s.%s. Skipped"%(self.myName, \
                            self.taskName, j['name']) )
                    self.log.debug( traceback.format_exc() )
                    newRange.remove(j['jobId'])
                    skippedSubmissions.append(j['jobId'])
                    continue

        if len(backupFiles) > 0:
            self.log.info("Backup copy created for %s: %s"%(self.myName, str(backupFiles) ))

        if needUpd == True:
            try:
                self.blDBsession.updateDB(task)
            except Exception, e:
                self.log.info("Worker %s. Problem saving regenerated RunningJobs for %s"%(self.myName, self.taskName) )
                return [], self.cmdRng

        # consider only those jobs that are in a submittable status
        for j in task.jobs:
            if j['jobId'] in newRange:
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
                    continue
        return newRange, skippedSubmissions

    def submitTaskBlocks(self, task, sub_jobs, reqs_jobs, matched):
        submitted, fullSubJob, errorTrace = ([], [], '')
        for sub in sub_jobs: fullSubJob.extend(sub)
        unsubmitted = fullSubJob
        if len(matched)==0:
            self.log.info('Worker %s unable to submit jobs. No sites matched'%self.myName)
            return submitted, unsubmitted, errorTrace

        self.SendMLpre(task)
        for ii in matched:
            # SplitCollection if too big DS # fix Fabio
            sub_bulk = []
            bulk_window = 200
            if len(sub_jobs[ii]) > bulk_window:
                sub_bulk = [ sub_jobs[ii][i:i+bulk_window] for i in range(0, len(sub_jobs[ii]), bulk_window)]
                self.log.info("Collection too big: split in %s sub_collection"%len(sub_bulk) )

            # submit now
            errorTrace = ''
            try:
                if len(sub_bulk)>0:
                    count = 1
                    for sub_list in sub_bulk:
                        self.blSchedSession.submit(task['id'], sub_list, reqs_jobs[ii])
                        self.log.info("Worker submitted sub collection # %s "%count)
                        count += 1
                    task = self.blDBsession.load( task['id'], sub_jobs[ii] )
                else:
                    task = self.blSchedSession.submit(task['id'], sub_jobs[ii], reqs_jobs[ii])
            except BossLiteError, e:
                self.log.info("Worker %s. Problem submitting task %s jobs. %s"%(self.myName, self.taskName, str(e.description()) ))
                #self.log.info( str(e) ) # temp message
                errorTrace = str( BossLiteLogger( task, e ) )
                self.log.debug(errorTrace)
                pass

            # check if submitted
            if len(errorTrace) == 0:
                parentIds = []
                for j in task.jobs:
                    self.blDBsession.getRunningInstance(j)
                    if j.runningJob['schedulerId']:
                        submitted.append(j['jobId'])
                        if j['jobId'] in unsubmitted: unsubmitted.remove(j['jobId'])
                        j.runningJob['status'] = 'S'
                        j.runningJob['statusScheduler'] = 'Submitted'
                        parentIds.append( j.runningJob['schedulerParentId'] )
                self.log.info("Parent IDs for task %s: %s"%(self.taskName, str(set(parentIds)) ) )
                self.blDBsession.updateDB( task )
                self.SendMLpost( task, sub_jobs[ii] )
        return submitted, unsubmitted, errorTrace


    def evaluateSubmissionOutcome(self, taskObj, submittableRange, submittedJobs, \
            unmatchedJobs, nonSubmittedJobs, skippedJobs):

        resubmissionList = list( set(submittableRange).difference(set(submittedJobs)) )
        self.log.info("Worker. Task %s (%d jobs): submitted %d unmatched %d notSubmitted %d skipped %d"%(self.taskName, \
            len(submittableRange), len(submittedJobs), len(unmatchedJobs), len(nonSubmittedJobs), len(skippedJobs) )    )
        self.log.debug("Task %s\n"%self.myName + "jobs : %s \nsubmitted %s \nunmatched %s\nnotSubmitted %s\nskipped %s"%(str(submittableRange), \
            str(submittedJobs), str(unmatchedJobs), str(nonSubmittedJobs), str(skippedJobs) )   )

        ## if all the jobs have been submitted send a success message
        if len(resubmissionList) == 0 and len(unmatchedJobs + nonSubmittedJobs + skippedJobs) == 0:
            ## added db Session to passed parameters
            if self.registerTask(taskObj) != 0:
                self.sendResult(10, "Unable to register task %s. Causes: deserialization, saving, registration "%(self.taskName), \
                    "WorkerError %s. Error while registering jobs for task %s."%(self.myName, self.taskName) )
                self.local_queue.put((self.myName, "CrabServerWorkerComponent:CrabWorkFailed", self.taskName))
                return

            self.sendResult(0, "Full Success for %s"%self.taskName, "Worker. Successful complete submission for task %s"%self.taskName )
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:CrabWorkPerformed", self.taskName))

            onDemandRegDone = False
            self.log.info("Submitted jobs: " + str(submittedJobs))
            for j in taskObj.jobs:
                if j['jobId'] in submittedJobs:
                    state_we_job = ""
                    try: 
                        ## get internal server job status (not blite status)
                        state_we_job = self.cwdb.getWEStatus( j['name'] )
                    except Exception, ex:
                        self.log.info("Job Status: " +(str(ex)))
                        ##TODO: Need to differenciate between different problems!
                        # in case the job shouldn't be registered 
                        if onDemandRegDone == False:
                            ## added db Session to passed parameters
                            self.registerTask(taskObj)
                            onDemandRegDone = True
                        else:
                            ## debug problems
                            pass
                    try:
                        # update the job status properly
                        if state_we_job == 'create':
                            self.cwdb.updateWEStatus( j['name'], 'inProgress' )
                    except Exception, ex:
                        self.log.info("Problem changing status to "+str(j['name']))
                        self.log.info(str(ex))
                        continue
            self.log.info("FatWorker %s registered jobs entities "%self.myName)
            return
        else:
            ## some jobs need to be resubmitted later
            if len(submittedJobs) == 0:
                self.sendResult(-1, "Any jobs submitted for task %s"%self.taskName, \
                    "Worker %s. Any job submitted: %d more attempts \
                    will be performed"%(self.myName, self.resubCount))
            else:
                self.local_queue.put((self.myName, "CrabServerWorkerComponent:CrabWorkPerformedPartial", self.taskName))
                self.sendResult(-2, "Partial Success for %s"%self.taskName, \
                    "Worker %s. Partial submission: %d more attempts \
                     will be performed"%(self.myName, self.resubCount))

            # propagate the re-submission attempt
            self.cmdRng = ','.join(map(str, resubmissionList))
            self.resubCount -= 1
            if self.resubCount > 0:
                payload = self.taskName+"::"+str(self.resubCount)+"::"+str(resubmissionList)
                self.local_queue.put((self.myName, "CrabServerWorkerComponent:Submission", payload))
                return

            payload = self.taskName+"::"+str(resubmissionList)
            self.local_queue.put((self.myName, "SubmissionFailed", payload))
            try:
                self.registerTask(taskObj)
                jobSpecId = []
                toMarkAsFailed = list(set(resubmissionList+unmatchedJobs + nonSubmittedJobs + skippedJobs))
                for j in taskObj.jobs:
                    if j['jobId'] in toMarkAsFailed:
                        jobSpecId.append(j['name'])

                self.cwdb.stopResubmission(jobSpecId)
                for jId in jobSpecId:
                    try:
                        self.cwdb.updateWEStatus(jId, 'reallyFinished')
                    except Exception, e:
                        continue
            except Exception,e:
                self.log.info("Unable to mark failed jobs in WorkFlow Entities ")
                self.log.info( traceback.format_exc() )

            # Give up message
            self.log.info("Worker %s has no more attempts: give up with task %s"%(self.myName, self.taskName) )
            status, reason = ("10", "Command for task %s has no more attempts. Give up."%self.taskName)
            payload = "%s::%s::%s"%(self.taskName, status, reason)
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:SubmitNotSucceeded", payload))
        return

####################################
    # Auxiliary methods
####################################
    def registerTask(self, taskArg):
        for job in taskArg.jobs:
            jobName = job['name']
            cacheArea = os.path.join( self.wdir, str(self.taskName + '_spec'), jobName )
            jobDetails = { \
                          'id':jobName, 'job_type':'Processing', 'cache':cacheArea, \
                          'owner':self.taskName, 'status': 'create', \
                          'max_retries':self.configs['maxRetries'], 'max_racers':1 \
                         }
            jobAlreadyRegistered = False
            try:
                jobAlreadyRegistered = self.cwdb.existsWEJob(jobName)
            except Exception, e:
                ##TODO: need to differnciate when more then 1 entry per job (limit case) 
                self.log.debug('Error while checking job registration: assuming %s as not registered'%jobName)
                jobAlreadyRegistered = False

            if jobAlreadyRegistered == True:
                continue

            self.log.debug('Registering %s'%jobName)
            try:
                self.cwdb.registerWEJob(jobDetails)
            except Exception, e:
                self.log.info('Error while registering job for JT: %s'%jobName)
                self.log.info(str(e))
                return 1
            self.log.debug('Registration for %s performed'%jobName)
        return 0

    def submissionListCreation(self, taskObj, jobRng):
        '''
           Matchmaking process. Inherited from CRAB-SA
        '''
        sub_jobs = []      # list of jobs Id list to submit
        requirements = []  # list of requirements for the submitting jobs

        # group jobs by destination
        distinct_dests = []
        for j in taskObj.jobs:
            if not isinstance(j['dlsDestination'], list):
                j['dlsDestination'] = eval(j['dlsDestination'])
            if  j['dlsDestination'] not in distinct_dests:
                distinct_dests.append( j['dlsDestination'] )

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
            tags = ''
            if self.bossSchedName == 'SchedulerCondorG':
                requirements.append( self.sched_parameter_CondorG(id_job, taskObj) )
            elif self.bossSchedName == 'SchedulerLsf':
                requirements.append( self.sched_parameter_Lsf(id_job, taskObj) )
            elif self.bossSchedName == 'SchedulerGLiteAPI':
                tags_tmp = str(taskObj['jobType']).split('"')
                tags = [str(tags_tmp[1]), str(tags_tmp[3])]
                requirements.append( self.sched_parameter_Glite(id_job, taskObj) )
            else:
                continue

            # Perform listMatching
            if self.bossSchedName in ['SchedulerCondorG', 'SchedulerLsf']:
                matched.append(sel)
            else:
                cleanedList = None
                if len(distinct_dests[sel]) > 0:
                    cleanedList = self.checkWhiteList(self.checkBlackList(distinct_dests[sel],''),'')
                sites = self.blSchedSession.lcgInfo(tags, seList=cleanedList, blacklist=self.ce_blackL, whitelist=self.ce_whiteL)

                if len(sites) > 0: matched.append(sel)
                else: unmatched.append(sel)
            sel += 1

        # all done and matched, go on with the submission
        return sub_jobs, requirements, matched, unmatched

#########################################################
### Matching auxiliary methods
#########################################################

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
        Prepare DashBoard information
        """
        taskId=str("_".join(str(taskObj['name']).split('_')[:-1]))
        # rebuild flat gridName string (pruned from SSL print and delegation adds)
        gridName = self.proxySubject
        gridName = '/'+"/".join(gridName.split('/')[1:-1])
        VO = self.cfg_params['VO']
        taskType = 'analysis'
        datasetPath = self.cfg_params['CMSSW.datasetpath']
        if datasetPath.lower() == 'none': datasetPath = None
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
        task = self.blDBsession.load(taskFull, allList)
        params = {}
        for k,v in self.collect_MLInfo(task).iteritems():
            params[k] = v

       # taskId = params['taskId']
        taskId = task['name']

        for job in task.jobs:
            jj, jobId, localId , jid = (job['jobId'], '', '', str(job.runningJob['schedulerId']) )
            if self.bossSchedName == 'SchedulerCondorG':
                hash = self.cfg_params['cfgFileNameCkSum'] #makeCksum(common.work_space.cfgFileName())
                rb = 'OSG'
                jobId = str(jj) + '_' + hash + '_' + jid
            elif self.bossSchedName == 'SchedulerLsf':
                jobId = "https://"+self.schedName+":/" + jid + "-" + taskId.replace("_", "-")
                rb = self.schedName
                localId = jid
            else:
                jobId = str(jj) + '_' + str(jid)
                rb = str(job.runningJob['service'])

            if len(job['dlsDestination']) == 1:
                T_SE=str(job['dlsDestination'][0])
            elif len(job['dlsDestination']) == 2:
                T_SE='%s,%s'%(job['dlsDestination'][0], job['dlsDestination'][1])
            else:
                T_SE = str(len(job['dlsDestination']))+'_Selected_SE'

            infos = { 'jobId': jobId, \
                      'sid': jid, \
                      'broker': rb, \
                      'bossId': jj, \
                      'SubmissionType': 'Server', \
                      'TargetSE': T_SE, \
                      'localId' : localId}
            params.update(infos)
            self.apmon.sendToML(params)
        return

#########################################################
### Specific Scheduler requirements parameters
#########################################################

    def sched_parameter_CondorG(self, i, task):
        """
        Parameters specific to CondorG scheduler
        """
        from ProdCommon.BDII.Bdii import getJobManagerList, listAllCEs
        
        # shift due to BL ranges
        i = i-1
        if i<0: 
            i = 0
            
        # Unpack CMSSW version and architecture from gLite style-string
        [verFrag, archFrag] = task['jobType'].split(',')[0:2]
        version = verFrag.split('-')[-1]
        arch = archFrag.split('-')[-1]
        version = version.replace('"','')
        arch = arch.replace('"','')

        # Get list of SEs and clean according to white/black list
        seList = task.jobs[i]['dlsDestination']
        seParser = SEBlackWhiteListParser(self.se_whiteL, self.se_blackL, self.log)
        seDest   = seParser.cleanForBlackWhiteList(seList, 'list')

        # Convert to list of CEs and clean according to white/black list
        onlyOSG = True # change for Glidein
        availCEs = getJobManagerList(seDest, version, arch, onlyOSG=onlyOSG)
        ceParser = CEBlackWhiteListParser(self.ce_whiteL, self.ce_blackL, self.log)
        ceDest   = ceParser.cleanForBlackWhiteList(availCEs, 'list')
        self.log.info('CE parser output: WL=%s, BL=%s' % (ceParser.whiteList(),ceParser.blackList()))
        self.log.info('SE parser output: WL=%s, BL=%s' % (seParser.whiteList(),seParser.blackList()))
        self.log.info('SE destination = %s' % seDest)
        self.log.info('CE destination = %s' % ceDest)

        schedParam = "schedulerList = " + ','.join(ceDest) + "; "

        if self.cfg_params['EDG.max_wall_time']:
            schedParam += 'globusrsl = (maxWalltime=%s); ' % self.cfg_params['EDG.max_wall_time']

        return schedParam


    def sched_parameter_Lsf(self, i, task):
        sched_param= ''
        resDir= "/".join((task['globalSandbox'].split(',')[0]).split('/')[:-1])
        queue = 'cmscaf' ### ToBeAddede in cfg.xml file
        res = 'cmscaf'
        if (queue):
            sched_param += '-q '+queue +' '
            if (res): sched_param += ' -R '+res +' '
       # sched_param+='-cwd '+resDir + ' '
        return sched_param

    def sched_parameter_Glite(self, i, task):
        # shift due to BL ranges
        i = i-1
        if i<0: i = 0

        seList = task.jobs[i]['dlsDestination']
        seParser = SEBlackWhiteListParser(self.se_whiteL, self.se_blackL, self.log)
        seDest   = seParser.cleanForBlackWhiteList(seList, 'list')
        ceParser = CEBlackWhiteListParser(self.ce_whiteL, self.ce_blackL, self.log)

        self.log.info('CE parser output: WL=%s, BL=%s' % (ceParser.whiteList(),ceParser.blackList()))
        self.log.info('SE parser output: WL=%s, BL=%s' % (seParser.whiteList(),seParser.blackList()))
        self.log.info('SE destination = %s' % seDest)

        sched_param = 'Requirements = ' + task['jobType']
        req=''

        if self.cfg_params['EDG.max_wall_time']:
            req += 'other.GlueCEPolicyMaxWallClockTime>=' + self.cfg_params['EDG.max_wall_time']
        if self.cfg_params['EDG.max_cpu_time']:
            if (not req == ' '): req = req + ' && '
            req += ' other.GlueCEPolicyMaxCPUTime>=' + self.cfg_params['EDG.max_cpu_time']

        sched_param += req + self.se_list(i, task.jobs[i]['dlsDestination']) + self.ce_list() +';\n'
        #if self.EDG_addJdlParam: sched_param+=self.jdlParam() ## BL--DS
        sched_param+='MyProxyServer = "' + self.cfg_params['proxyServer'] + '";\n'
        sched_param+='VirtualOrganisation = "' + self.cfg_params['VO'] + '";\n'
        sched_param+='RetryCount = '+str(self.cfg_params['EDG_retry_count'])+';\n'
        sched_param+='ShallowRetryCount = '+str(self.cfg_params['EDG_shallow_retry_count'])+';\n'
        return sched_param

#########################################################
    def checkWhiteList(self, Sites, fileblocks):
        """
        select sites that are defined by the user (via SE white list)
        """
        goodSites = []
        if len(self.se_whiteL)==0: return Sites
        for aSite in Sites:
            good=0
            for re in self.se_whiteL:
                if re.search(string.lower(aSite)):good=1
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
                if re.search(aSite.lower()): good=0
                pass
            if good: goodSites.append(aSite)
        return goodSites

    def se_list(self, id, dest):
        """
        Returns string with requirement SE related
        """
        # consolidation of findSites_ method and se_list as in Crab-SA
        # hostList = self.findSites_(id, dest)
        hostList = []
        if len(dest) > 0 and dest[0] != "":
            replicas = self.checkBlackList(dest, id)
            if len(replicas) > 0:
                replicas = self.checkWhiteList(replicas, id)
            hostList = replicas

        req = ''
        if len(hostList) > 0:
            reqtmp = [ ' Member("'+arg+'" , other.GlueCESEBindGroupSEUniqueID) ' for arg in hostList]
            req += " && (" + '||'.join(reqtmp) + ") "
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
                ce = str(ce).strip()
                if len(ce)==0: continue
                tmpCe.append('RegExp("' + ce + '", other.GlueCEUniqueId)')
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
                ce = str(ce).strip()
                if len(ce)==0: continue
                tmpCe.append('(!RegExp("' + str(ce).strip() + '", other.GlueCEUniqueId))')
            if len(tmpCe): req += " && (" + concString.join(tmpCe) + ") "

        # requirement added to skip gliteCE
        req += '&& (!RegExp("blah", other.GlueCEUniqueId))'
        return req
