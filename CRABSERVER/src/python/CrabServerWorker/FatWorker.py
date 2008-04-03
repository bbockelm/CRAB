#!/usr/bin/env python
"""
_FatWorker_

Implements thread logic used to perform the actual Crab task submissions.

"""

__revision__ = "$Id: FatWorker.py,v 1.12 2007/09/20 10:16:10 farinafa Exp $"
__version__ = "$Revision: 1.12 $"

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
#from CrabServer.ApmonIf import ApmonIf

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

            self.wmsEndpoint = configs['wmsEndpoint']
            self.se_blackL = [] + configs['se_dynBList']
            self.ce_blackL = [] + configs['ce_dynBList']

            self.EDG_retry_count = int(configs['EDG_retry_count'])
            self.EDG_shallow_retry_count = int(configs['EDG_shallow_retry_count'])
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

        self.se_whiteL = []
        self.ce_whiteL = []

        self.dashParams = {}
        #self.apmon = ApmonIf()

        # Parse the XML files
        taskDir = self.wdir + '/' + self.taskName + '_spec/'
        try:
            cmdSpecFile = taskDir + 'cmd.xml'
            doc = minidom.parse(cmdSpecFile)
            self.cmdXML = doc.getElementsByTagName("TaskCommand")[0]
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
        self.schedName = str( self.cmdXML.getAttribute('Scheduler') )

        if self.schedName in ['glite', 'glitecoll']:
            self.schedulerConfig['name'] = 'SchedulerGLiteAPI' 
            self.schedulerConfig['config'] = self.wdir + '/glite.conf.CMS_' + self.brokerName
            self.schedulerConfig['service'] = "https://wms102.cern.ch:7443/glite_wms_wmproxy_server" # self.wmsEndpoint
        elif self.schedName == 'condor_g':
            self.schedulerConfig['name'] = 'SchedulerCondorGAPI' 
            self.schedulerConfig['config'] = self.wdir + '/glite.conf.CMS_' + self.brokerName
            self.schedulerConfig['service'] = "https://wms102.cern.ch:7443/glite_wms_wmproxy_server"
        elif self.schedName == 'arc':
            pass
        elif self.schedName == 'lsf':
            pass

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
        self.log.info("Worker %s warmed up and ready to submit"%self.myName)
        try:
            self.submissionDriver()
        except Exception, e:
            self.log.info( traceback.format_exc() )
            self.sendResult(66, "Worker exception. Free-resource message", \
                    "FatWorkerError %s. Task %s."%(self.myName, self.taskName) )
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
        self.blSchedSession = BossLiteAPISched(self.blDBsession, self.schedulerConfig)
        newRange, skippedJobs = self.preSubmissionCheck(taskObj, str(self.cmdXML.getAttribute('Range')) )
        self.log.info('Worker %s pre-submission checks passed'%self.myName)

        if (newRange is not None) and (len(newRange) > 0):
            submittedJobs = []
            nonSubmittedJobs = [] + newRange

            sub_jobs, reqs_jobs, matched, unmatched = self.submissionListCreation(taskObj, newRange) 
            self.log.info('Worker %s listmatched jobs, now submitting'%self.myName)

            if len(matched) > 0:
                submittedJobs, nonSubmittedJobs = self.submitTaskBlocks(taskObj, sub_jobs, reqs_jobs, matched)
            else:
                self.log.info('Worker %s unable to submit jobs. No sites matched'%self.myName)

            self.evaluateSubmissionOutcome(taskObj, newRange, submittedJobs, unmatched, nonSubmittedJobs, skippedJobs)
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
        newRange = eval(rng)  # a-la-CRAB range expansion and Boss ranges (1 starting)
        doNotSubmitStatusMask = ['R','S'] # ,'K','Y','D'] # to avoid resubmission of final state jobs
        tryToSubmitMask = ['C', 'A', 'RC', 'Z'] + ['K','Y','D']
        skippedSubmissions = []

        ## check if the input sandbox is already on the right SE  
        taskFileList = task['globalSandbox'].split(',')
        remotePath = str(self.cfg_params['CRAB.se_remote_dir'])
        
        try:
            seEl = SElement(self.SEurl, self.SEproto, self.SEport) 
            sbi = SBinterface( seEl )
            for f in taskFileList:
                if sbi.checkExists(f, self.proxy) == False:
                    self.log.info("FatWorker %s. Missing file %s"%(self.myName, f)) 
                    return [], newRange
        except Exception, e:
            self.log.info( traceback.format_exc() )
            return [], newRange
        
        for j in task.jobs:
            if (j['jobId'] in newRange):
                try:
                    # do not submit already running or scheduled jobs
                    if j.runningJob['status'] in doNotSubmitStatusMask:
                        self.log.info("FatWorker %s.Task %s job %s status %s. Won't be submitted"%(self.myName, \
                            self.taskName, j['name'], j['status']) )
                        newRange.remove(j['jobId'])
                        continue

                    # trace unknown state jobs and skip them from submission
                    if j.runningJob['status'] not in tryToSubmitMask:
                        newRange.remove(j['jobId'])
                        skippedSubmissions.append(j['jobId'])
                except Exception, e:
                    self.log.info("FatWorker %s.\n problem inspecting task %s job %s. Won't be submitted"%(self.myName, \
                                self.taskName, j['name']) )
                    newRange.remove(j['jobId'])
                    skippedSubmissions.append(j['jobId'])

        return newRange, skippedSubmissions

    def submitTaskBlocks(self, task, sub_jobs, reqs_jobs, matched):
        unsubmitted = [] # list of all the jId hadled. Used to check the number of successful jubmissions
        submitted = [] 

        # modify sandbox and other paths for WMS bypass
        turlpreamble = 'gsiftp://%s:%s'%(self.SEurl, self.SEport)
        task['startDirectory'] = turlpreamble
        #remoteSBlist = [turlpreamble + f for f in str(task['globalSandbox']).split(',') ]
        #task['globalSandbox'] = ','.join(remoteSBlist)
        task['scriptName'] = turlpreamble + task['scriptName']
        task['cfgName'] = turlpreamble + task['cfgName']
        self.blDBsession.updateDB(task) 

        # loop and submit blocks
        for ii in matched:
            # extract task for the range and submit
            # task = self.blDBsession.load(taskObj['id'], sub_jobs[ii])[0]
            unsubmitted += sub_jobs[ii]
            try:
                task = self.blSchedSession.submit(task, sub_jobs[ii], reqs_jobs[ii])
            except Exception, e:
                self.log.info("FatWorker %s. Problem submitting task %s jobs %s. %s"%(self.myName, \
                                self.taskName, str(sub_jobs[ii]), str(e) )
                continue

            # check if submitted
            #task = self.blDBsession.load(taskObj['id'], sub_jobs[ii])[0]
            for j in task.jobs: 
                if j.runningJob['schedulerId']:
                    self.log.debug(j.runningJob['schedulerId'])
                    submitted.append(j['jobId'])
                    unsubmitted.remove(j['jobId'])
                    self.blDBsession.getRunningInstance(j)
                    j.runningJob['status'] = 'S'
            self.blDBsession.updateDB( task )

        return submitted, unsubmitted 

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
        if len( set(submittableRange).difference(set(submittedJobs)) )==0:
            self.sendResult(0, "Full Success for %s"%self.taskName, \
                "FatWorker %s. Successful complete submission for task %s"%(self.myName, self.taskName) )

            self.local_ms.publish("CrabServerWorkerComponent:CrabWorkPerformed", taskObj['name'])
            self.local_ms.commit()
            return

        ## some jobs need to be resubmitted later
        else:
            # get the list of missing jobs # discriminate on the lists? in future maybe
            resubmissionList = list( set(submittableRange).difference(set(submittedJobs)) )

            # prepare a new message for the missing jobs
            newRange = ','.join(map(str, resubmissionList))
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
        # register in workflow  
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
        except Exception, e:
            self.log.info('Error while registering job for JT: %s'%self.taskName)
            self.log.info( traceback.format_exc() )
            Session.rollback(self.taskName)
            Session.close(self.taskName)
            return 1

        # register DashBoard information
        try:
            gridName = str(self.cmdXML.getAttribute('Subject')).split('subject=')[1].strip()
            gridName = gridName[:gridName.rindex('/CN')]
 
            # TODO fix JSTool ver
            self.dashParams = {'jobId':'TaskMeta', \
                      'tool': 'crab',\
                      'JSToolVersion': '', \
                      'tool_ui': os.environ['HOSTNAME'], \
                      'scheduler': self.schedName, \
                      'GridName': gridName, \
                      'taskType': 'analysis', \
                      'vo': self.cfg_params['VO'], \
                      'user': os.environ['USER'],  \
                      'exe': 'cmsRun', \
                      'SubmissionType': 'Server'} 

            #self.apmon.sendToML(self.dashParams)
        except Exception, e:
            self.log.info('Error while registering job for Dashboard: %s'%self.taskName)
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
                 if str(distDest) == str(j['dlsDestination']):
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
        tags_tmp = str(taskObj['jobType']).split('"')
        tags = [str(tags_tmp[1]),str(tags_tmp[3])]

        for id_job in jobs_to_match:
            # Get JDL requirements
            if "GLITE" in self.schedName.upper(): 
                requirements.append( self.sched_parameter_Glite(id_job, taskObj) )
            elif self.schedName.upper()== "CONDOR_G":
                requirements.append( self.sched_parameter_Condor() )
            elif self.schedName.upper()== "LSF":
                requirements.append( self.sched_parameter_Lsf() )
            else:
                continue
            
            # Perform listMatching
            if self.schedName.upper() != "CONDOR_G" :
                cleanedList = None
                if len(distinct_dests[sel])!=0:
                    cleanedList = self.checkWhiteList(self.checkBlackList(distinct_dests[sel],''),'')

                sites = self.blSchedSession.lcgInfo(tags, cleanedList, self.ce_whiteL, self.ce_blackL )
                match = len( sites )
            else :
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

#########################################################
### Specific Scheduler requirements parameters
#########################################################
    def sched_parameter_Condor(self):
        return

    def sched_parameter_Lsf(self):
        return

    def sched_parameter_Glite(self, i, task):
        dest = task.jobs[i]['dlsDestination']
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

