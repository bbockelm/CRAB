#!/usr/bin/env python
"""
_FatWorker_

Implements thread logic used to perform the actual Crab task submissions.

"""

__revision__ = "$Id: FatWorker.py,v 1.227 2012/07/12 15:51:40 fanzago Exp $"
__version__ = "$Revision: 1.227 $"

import string
import sys, os
from socket import getfqdn
import time
from threading import Thread
from MessageService.MessageService import MessageService
from xml.dom import minidom
import traceback
import copy
import re
import logging

# CW DB API
from CrabWorkerAPI import CrabWorkerAPI

# BossLite import
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched

from ProdCommon.BossLite.Common.System import executeCommand
from ProdCommon.BossLite.Common.BossLiteLogger import BossLiteLogger
from ProdCommon.BossLite.Common.Exceptions import BossLiteError, SchedulerError, TimeOut

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
        self.glexec = self.configs['glexec']
        self.glexecWrapper = self.configs['glexecWrapper']
        self.renewProxy = self.configs['renewProxy']
        self.condorQCacheDir = self.configs['CondorQCacheDir']

        self.resubCount = int(self.configs['retries'])
        self.cmdRng = [] + self.configs['submissionRange']
        self.submissionSource = self.configs['submissionSource']

        self.se_blackL = [] + self.configs['se_dynBList']
        self.se_whiteL = []
        self.ce_blackL = [] + self.configs['ce_dynBList']
        self.ce_whiteL = []
        self.wmsEndpoint = self.configs['wmsEndpoint']
        self.local_queue = self.configs['messageQueue']
        self.role = ''
        self.group = ''
       
        self.serviceconfig={}
        if self.configs['certPath'] and self.configs['keyPath']:
            self.serviceconfig['cert']=self.configs['certPath']
            self.serviceconfig['key']=self.configs['keyPath']
        

        ##Initialization to allow lsf@caf
        self.cpCmd = self.configs['cpCmd']
        self.rfioServer = self.configs['rfioServer']

        self.schedName = self.configs['defaultScheduler'].upper()
        self.supportedScheds = [] + self.configs['supportedSchedulers']

        self.seEl = SElement(self.configs['SEurl'], self.configs['SEproto'], self.configs['SEport'])
        self.blDBsession = BossLiteAPI('MySQL', dbConfig, pool=self.configs['blSessionPool'])
        self.blSchedSession = None
        self.apmon = ApmonIf()

        ## CW DB init
        self.cwdb = CrabWorkerAPI( self.blDBsession.bossLiteDB )

        ## extending field for info to log
        self.infotolog = {
                            "ev":       "Submission",       \
                            "reason":   None,               \
                            "error":    None,               \
                            "time":     None,               \
                            "range":    str(self.cmdRng),   \
                            "exc":      None,               \
                            "SE-Black": None,               \
                            "SE-White": None,               \
                            "CE-Black": None,               \
                            "CE-White": None,               \
                            "submittedJobs": None,          \
                            "unmatchedJobs": None,          \
                            "notSubmittedJobs": None,       \
                            "skippedJobs": None
                         }

        try:
            self.start()
        except Exception, e:
            logMsg = 'FW %s exception : \n'%self.myName
            logMsg +=  traceback.format_exc()
            self.log.info(logMsg)
        self.apmon.free()

        return

    def run(self):
        self.log.info("FW %s initialized: task %s"%(self.myName, self.taskName) )
        taskObj = None
        self.local_queue.put((self.myName, "CrabServerWorkerComponent:CommandArrival", self.taskName))
        if not self.parseCommandXML() == 0:
            logMsg = "FW %s Error. XML for the requested task %s does not exist."%(self.myName, self.taskName)
            self.preLog(mess = "Pre-submission failure", err = logMsg, exc = exc)
            self.sendResult(11, "Unable to parse XML for task %s. Causes: parseCommandXML"%(self.taskName), logMsg, e)
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:CrabWorkFailed", self.taskName))
            return

        self.log.info("FW %s loading task"%self.myName)
        try:
            taskObj = self.blDBsession.loadTaskByName(self.taskName)
        except Exception, e:
            exc = str( traceback.format_exc() )
            self.log.debug( exc )
            logMsg = "FW %s Error. Requested task %s does not exist."%(self.myName, self.taskName)
            self.preLog(mess = "Pre-submission failure", err = logMsg, exc = exc)
            self.sendResult(11, "Unable to retrieve task %s. Causes: loadTaskByName"%(self.taskName), logMsg, e)
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:CrabWorkFailed", self.taskName))
            return

        self.log.info("FW %s allocating submission system session"%self.myName)
        if not self.allocateBossLiteSchedulerSession(taskObj) == 0:
            self.log.info("Scheduler allocation failed")
            self.markJobsAsFailed(taskObj, self.cmdRng)
            return

        self.log.info('FW %s preparing submission'%self.myName)
        errStatus, errMsg = (66, "Worker exception. Free-resource message")
        newRange, skippedJobs = None, None
        try:
            newRange, skippedJobs = self.preSubmissionCheck(taskObj)
            if newRange is None or len(newRange) == 0 :
                raise Exception('Empty range [%s] submission temptative, skipped jobs [%s], asked jobs [%s].'%(str(newRange),str(skippedJobs),str(self.cmdRng)))
        except Exception, e:
            self.markJobsAsFailed(taskObj, self.cmdRng)
            exc = str( traceback.format_exc() )
            self.log.debug( exc )
            logMsg = "FW %s Error. Task %s. preSubmissionCheck."%(self.myName, self.taskName)
            self.preLog(mess = "Failure in pre-submission check", err = logMsg, exc = exc)
            self.sendResult(errStatus, errMsg, logMsg, e)
            return

        self.log.info('FW %s performing list-match operation'%self.myName)
        sub_jobs, reqs_jobs, matched, unmatched = None, None, None, None
        try:
            sub_jobs, reqs_jobs, matched, unmatched, errorLogs = self.submissionListCreation(taskObj, newRange)
            if len(matched)==0:
                logMsg = "Unable to submit jobs %s: no sites matched!"%str(sub_jobs)
                logMsg += '\n%s'%str(errorLogs)
                self.log.info(logMsg)
                errMsg = "NOMATCH: no site matched !!!" 
                raise Exception(errMsg)
        except Exception, e:
            self.markJobsAsFailed(taskObj, self.cmdRng)
            exc = str( traceback.format_exc() )
            self.log.debug( exc )
            logMsg = "FW %s Error. Task %s. listMatch."%(self.myName, self.taskName)
            self.preLog(mess = "Failure in pre-submission init", err = logMsg, exc = exc)
            self.sendResult(errStatus, errMsg, logMsg, e)
            return

        self.log.info("FW %s performing submission"%self.myName)
        try:
            submittedJobs, nonSubmittedJobs, errorTrace = self.submitTaskBlocks(taskObj, sub_jobs, reqs_jobs, matched)

        except Exception, e:
            self.markJobsAsFailed(taskObj, self.cmdRng)
            exc = str( traceback.format_exc() )
            self.log.info( exc )
            logMsg = "FW %s Error. Task %s."%(self.myName, self.taskName)
            self.preLog(mess = "Failure during jobs submission", err = logMsg, exc = exc)
            self.sendResult(errStatus, errMsg, logMsg, e)
            return

        self.log.info("FW %s evaluating submission outcomes"%self.myName)
        try:
            ## added blite safe connection to the DB
            self.evaluateSubmissionOutcome(taskObj, newRange, submittedJobs, unmatched, nonSubmittedJobs, skippedJobs)
        except Exception, e:
            self.markJobsAsFailed(taskObj, self.cmdRng)
            exc = str( traceback.format_exc() )
            self.log.debug( exc )
            logMsg = "FW %s Error. Task %s. postSubmission."%(self.myName, self.taskName)
            self.preLog(mess = "Post-submission check failure", err = logMsg, exc = exc)
            self.sendResult(errStatus, errMsg, logMsg, e)
            return
        self.log.info("FW %s finished %s"%(self.myName, self.taskName) )
        return

    def sendResult(self, status, reason, logMsg, error = ''):
        self.log.info(logMsg)
        self.log.info(str(error))
        timespent = time.time() - self.tInit
        msg = self.myName + "::" + self.taskName + "::"
        msg += str(status) + "::" + reason + "::" + str(timespent)
        self.local_queue.put((self.myName, "CrabServerWorkerComponent:FatWorkerResult", msg))

    def parseCommandXML(self):
        status = 0
        cmdSpecFile = os.path.join(self.wdir, self.taskName + '_spec/cmd.xml' )
        self.log.info("FW FEDE file %s"%cmdSpecFile)
        try:
            doc = minidom.parse(cmdSpecFile)
        except Exception, e:
            status = 6
            reason = "Error while parsing command XML for task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            pload = self.taskName + "::" + str(status) + "::" + reason + "::-1"
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:SubmitNotSucceeded", pload))

        if status == 0:
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]

            # Make the scheduler configurable from user with fallback
            requestedScheduler = str(cmdXML.getAttribute('Scheduler')).upper()
            if requestedScheduler in self.supportedScheds:
                self.schedName = requestedScheduler
     
            ## already set in the message
            # self.cmdRng =  eval( cmdXML.getAttribute('Range'), {}, {} )
            ##
            self.splittingAlgorithm  = str( cmdXML.getAttribute('splitting_algorithm') )
            self.type = str( cmdXML.getAttribute('Type') )
            self.owner = str( cmdXML.getAttribute('Subject') )
            self.cfg_params = eval( cmdXML.getAttribute("CfgParamDict"), {}, {} )
            self.log.info('FW FEDE %s self.cfg_params'%self.cfg_params)
          
            # FEDE for savannah 75255
            self.client_version = str( cmdXML.getAttribute('ClientVersion') )
            self.log.info('FW %s self.client_version'%self.client_version)
            ############


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
                    if ceW:
                        self.ce_whiteL.append(ceW.strip())
            if 'EDG.ce_black_list' in self.cfg_params:
                for ceB in self.cfg_params['EDG.ce_black_list'].split(","):
                    if ceB:
                        self.ce_blackL.append(ceB.strip())
            if 'EDG.role' in self.cfg_params:
                self.role = self.cfg_params['EDG.role']
            if 'EDG.group' in self.cfg_params:
                self.group = self.cfg_params['EDG.group']
        return status

    def SchedulerGlitePlugin(self):
        scheduler = None
        out, ret =  executeCommand('glite-version')
        if ret != 0 :
            self.log.info('Error checking gLite version:\n'+out)
        else:
            if out.strip().startswith('3.1'):
                scheduler = 'SchedulerGLiteAPI'
            else:
                scheduler = 'SchedulerGLite'

        return scheduler

    def allocateBossLiteSchedulerSession(self, taskObj):
        """
        Set scheduler specific parameters and allocate the Scheduler Session
        """
        status = 0

        self.bossSchedName = {'GLITE': self.SchedulerGlitePlugin(),
                              'CONDOR_G':'SchedulerCondorG',
                              'GLIDEIN' :'SchedulerGlidein',
                              'ARC':'SchedulerARC',
                              'LSF':'SchedulerLsf',
                              'CAF':'SchedulerLsf'}[self.schedName]
        schedulerConfig = {'name': self.bossSchedName, 
                           'user_proxy':taskObj['user_proxy'],
                           'timeout':180}

        self.log.info('FW %s Allocating %s scheduler' % (self.myName,schedulerConfig['name']) )
        if schedulerConfig['name'] in ['SchedulerGLiteAPI','SchedulerGLite']:
            schedulerConfig['config'] = self.configs['serviceFile']
            schedulerConfig['skipWMSAuth'] = 1
            if self.wmsEndpoint:
                schedulerConfig['service'] = self.wmsEndpoint
            if schedulerConfig['name']=='SchedulerGLite':schedulerConfig['timeout'] = 600
        elif schedulerConfig['name'] in ['SchedulerGlidein', 'SchedulerCondorG']:
            condorTemp = os.path.join(self.wdir, self.taskName+'_spec')
            self.log.info('Condor will use %s for temporary files' % condorTemp)
            self.log.info('Path to glexec: %s' % self.glexec)
            self.log.info('Renew proxy script: %s' % self.renewProxy)
            schedulerConfig['tmpDir']    = condorTemp
            schedulerConfig['useGlexec'] = True
            schedulerConfig['glexecWrapper']    = self.glexecWrapper
            schedulerConfig['glexec']    = self.glexec
            schedulerConfig['renewProxy']    = self.renewProxy
            schedulerConfig['CondorQCacheDir']    = self.condorQCacheDir
        elif schedulerConfig['name'] == 'arc':
            pass
        elif schedulerConfig['name'] in ['SchedulerLsf']:
            schedulerConfig['cpCmd']   = self.cpCmd
            schedulerConfig['rfioSer'] = self.rfioServer
        try:
            self.blSchedSession = BossLiteAPISched(self.blDBsession, schedulerConfig)
        except Exception, e:
            exc = traceback.format_exc()
            status = 6
            reason = "Unable to create a BossLite Session because of the following error: \n "
            self.log.info(exc)
            self.sendResult(status, reason, reason, e, exc, True)
        return status


####################################
    # Submission methods
####################################
    def preSubmissionCheck(self, task):
        newRange = self.cmdRng
        # With the fix about https://savannah.cern.ch/bugs/?68702 these have just role of sanity check
        # I prefer to not remove them for the moment 
        doNotSubmitStatusMask = ['R','S'] 
        tryToSubmitMask = ['C', 'A', 'RC', 'Z'] + ['K','Y','D','E', 'SD']

        skippedSubmissions = []

        # closed running jobs regeneration and osb manipulation
        backupFiles = []
        try:
            bk_sbi = SBinterface( self.seEl, copy.deepcopy(self.seEl) )
        except Exception, ex:
            logMsg = "FW %s. Problem creating SE Api interface %s.\n"%self.myName
            logMsg += str(ex)
            raise Exception(logMsg)

        credential = task['user_proxy']
        if self.schedName.upper() in ['LSF','CAF']:
            username = task['name'].split('_')[0]
            credential = '%s::%s'%(username,credential)

        basePath = task['outputDirectory']
        if task['startDirectory'] != '':
            try : 
                basePath = task['outputDirectory'].split(task['startDirectory'])[1]
            except Exception, ex:
                logMsg = "FW %s. Error while correcting base path for task %s."%(self.myName, self.taskName)
                logMsg += "outputDirectory used as base path. Submission continues.\n"
                logMsg += str(ex)
                self.log.info( logMsg )
                raise Exception(logMsg)

        for j in task.jobs:
            if j['jobId'] not in self.cmdRng:
                continue  

            # fix candidate for empty submission range issue # Fabio
            # if the submission request is related to a "Done(failed)" job, then close and regenerate it
            # see https://savannah.cern.ch/bugs/?68702
            if j.runningJob['status'] != 'C' or j.runningJob['state'] != 'Created': 
                j.runningJob['closed'] = 'Y'
            ###

            if j.runningJob['closed'] == 'Y':
                # backup for job output (tgz files only, less load)
                for orig in [ basePath+'/'+f for f in j['outputFiles'] if 'tgz' in f ]:
                    try:
                       if self.bossSchedName in ['SchedulerGlidein']:
                          self.log.info("FatWorker : No need to back up osb for glideins")
                          check=0
                       else:
                          check=bk_sbi.checkExists(source=orig, proxy=task['user_proxy'])
                    except Exception, ex:
                        logMsg = "FW %s. Problem backupping OSB for job %s of task %s.\n"%(self.myName, \
                        j['name'], self.taskName)
                        logMsg += str(ex)
                        self.log.info( logMsg )
                        continue
                    if check:
                        try:
                            bk_sbi.move( source=orig, dest=orig+'.'+str(j['submissionNumber']), proxy=task['user_proxy'])
                        except Exception, ex:
                            logMsg = "FW %s. Problem backupping OSB for job %s of task %s.\n"%(self.myName, \
                            j['name'], self.taskName)
                            logMsg += str(ex)
                            self.log.info( logMsg )
                            continue
                        # track succesfully replicated files
                        backupFiles.append( os.path.basename(orig) )
                    else:
                        self.log.debug("FW %s No need to back up osb"%self.myName)

                # reproduce closed runningJob instances
                try:
                    self.blDBsession.updateDB(j)
                    self.blDBsession.getNewRunningInstance(j)
                except Exception, e:
                    logMsg = ("FW %s. Problem regenerating RunningJob %s.%s. Skipped"%(self.myName, \
                            self.taskName, j['name']) )
                    logMsg += str(e)
                    self.log.info( logMsg )
                    self.log.debug( traceback.format_exc() )
                    newRange.remove(j['jobId'])
                    skippedSubmissions.append(j['jobId'])
                    continue

                j.runningJob['status'] = 'C'
                j.runningJob['statusScheduler'] = 'Created'

                try:
                    self.blDBsession.updateDB(j)
                except Exception, e:
                    logMsg = "FW %s. Error saving regenerated RunningJobs for %s"%(self.myName, self.taskName)
                    logMsg += str(e)
                    self.log.info( logMsg )
                    newRange.remove(j['jobId'])
                    skippedSubmissions.append(j['jobId'])
                    continue

            # request for submission
            j.runningJob['state'] = 'SubRequested'
            try:
                self.blDBsession.updateDB(task)
            except Exception, e:
                logMsg = "FW %s. Problem saving regenerated RunningJobs for %s"%(self.myName, self.taskName)
                logMsg += str(e)
                self.log.info( logMsg )
                newRange.remove(j['jobId'])
                skippedSubmissions.append(j['jobId'])
                continue

        # consider only those jobs that are in a submittable status
        for j in task.jobs:
            if j['jobId'] in newRange:
                # do not submit already running or scheduled jobs
                if j.runningJob['status'] in doNotSubmitStatusMask:
                    newRange.remove(j['jobId'])
                    continue
                # trace unknown state jobs and skip them from submission
                if j.runningJob['status'] not in tryToSubmitMask:
                    newRange.remove(j['jobId'])
                    skippedSubmissions.append(j['jobId'])

        # summary printout for the jobs requested for submission
        self.log.info("FW %s Submission requested for %s"%(self.myName,str(newRange)))
        if len(skippedSubmissions) > 0:
            self.log.info("FW %s Skipped submission requests: %s"%(self.myName,str(skippedSubmissions)))
        if len(backupFiles) > 0:
            self.log.info("FW %s Backup copy created for: %s"%(self.myName, str(backupFiles) ))

        return newRange, skippedSubmissions

    def submitTaskBlocks(self, task, sub_jobs, reqs_jobs, matched):
        submitted, fullSubJob, errorTrace = ([], [], '')
        for sub in sub_jobs: fullSubJob.extend(sub)
        unsubmitted = fullSubJob
        if len(matched)==0:
            self.log.info('FW %s unable to submit jobs %s. No sites matched'%(self.myName,str(unsubmitted)))
            return submitted, unsubmitted, errorTrace

        self.SendMLpre(task)
        self.log.info('FW %s Delegating Proxy for task %s.'%(self.myName,self.taskName))
        try:
            self.blSchedSession.getSchedulerInterface().delegateProxy()
        except SchedulerError, e:
            logMsg = "FW %s. Problem Delegating Proxy for task %s. "%(self.myName, self.taskName)
            logMsg += str(e.description())
            self.log.info( logMsg )
        except TimeOut, e:
            logMsg  = "FW %s. Problem Delegating Proxy for task %s. \n"%(self.myName, self.taskName) 
            logMsg += str(e)+'\n'
            logMsg += "PARTIAL SUBPROCESS MESSAGE : \n%s"%e.commandOutput() 
            self.log.info( logMsg )
        except Exception, ee:
            logMsg = "FW %s. Unexpected exception Delegating Proxy for task %s. "%(self.myName, self.taskName)
            logMsg +=  traceback.format_exc()
            self.log.info( logMsg )
        for ii in matched:
            sub_bulk = []
            bulk_window = 200
            if len(sub_jobs[ii]) > bulk_window:
                sub_bulk = [ sub_jobs[ii][i:i+bulk_window] for i in range(0, len(sub_jobs[ii]), bulk_window)]
                self.log.info("FW %s Collection too big: split in %s sub_collection"%(self.myName, len(sub_bulk)) )

            # submit now
            errorTrace = ''
            if len(sub_bulk)>0:
                count = 1
                for sub_list in sub_bulk:
                    # update arguments for unique output naming

                    # Only for fullySpec tasks or automated tasks using another algo than RunBased 
                    if self.type == "fullySpecified" or ( self.type == "partiallySpecified" and self.splittingAlgorithm != "RunBased" ) :
                        for j in task.jobs:
                            if j['jobId'] in sub_list:
                                self.blDBsession.getRunningInstance(j)                            
                             
                                try:
                                    resubNum = int(str(j['arguments']).split(' ')[1]) + 1 
                                except Exception, e:
                                    resubNum = j.runningJob['submission'] 
                                newArgs = "%d %d"%(j.runningJob['jobId'], resubNum)
                                j['arguments'] = newArgs

                        self.blDBsession.updateDB( task )

                    try:
                        self.blSchedSession.submit(task['id'], sub_list, reqs_jobs[ii])
                        self.log.info("FW %s submitted sub collection # %s "%(self.myName,count))
                        count += 1
                    except SchedulerError, e:
                        logMsg = "FW %s. Problem submitting task %s collection %s. "%(self.myName, self.taskName, count)
                        logMsg += str(e.description())
                        self.log.info( logMsg )
                        errorTrace = str( BossLiteLogger( task, e ) )
                        try:
                            self.preLog(logMsg, str(e.description()), 0, errorTrace)
                        except Exception, ee:
                            self.log.error("FW %s Problem logging information: [%s]"%(self.myName,str(ee)))
                    except TimeOut, e:
                        logMsg = "FW %s. Problem submitting task %s collection %s. "%(self.myName, self.taskName, count)
                        logMsg += str(e)+'\n'
                        logMsg += "PARTIAL SUBPROCESS MESSAGE : \n%s"%e.commandOutput() 
                        self.log.info( logMsg )
                        errorTrace = str( BossLiteLogger( task, e ) )
                        try:
                            self.preLog(logMsg, str(e.description()), 0, errorTrace)
                        except Exception, ee:
                            self.log.error("FW %s Problem logging information: [%s]"%(slef.myName,str(ee)))
                    except Exception, ee:
                        logMsg = "FW %s. Unexpected exception submitting task %s collection %s. "%(self.myName, self.taskName, count)
                        logMsg +=  traceback.format_exc()
                        self.log.info( logMsg )

                    ## could be loaded just the sub_list jobs...
                    task = self.blDBsession.load( task['id'], sub_jobs[ii] )
                    # check if submitted
                    self.log.info("FW %s. Errors: [%s]" %(self.myName, str(errorTrace)) )
                    if len(errorTrace) == 0:
                        self.log.info("FW %s. Setting sub success for %s " %(self.myName, str(len(sub_list))) )
                        parentIds = []
                        for j in task.jobs:
                            self.blDBsession.getRunningInstance(j)
                            if j.runningJob['schedulerId'] and j['jobId'] in sub_list:
                                submitted.append(j['jobId'])
                                if j['jobId'] in unsubmitted: unsubmitted.remove(j['jobId'])
                                j.runningJob['status'] = 'S'
                                j.runningJob['statusScheduler'] = 'Submitted'
                                j.runningJob['state'] = 'SubSuccess'
                                parentIds.append( j.runningJob['schedulerParentId'] )
                        self.log.info("FW %s Parent IDs for task %s: %s"%(self.myName, self.taskName, str(set(parentIds)) ) )
                        self.SendMLpost( task, sub_jobs[ii] )
                        self.blDBsession.updateDB( task )
                    errorTrace = ''
            else:
                # update arguments for unique output naming

                # Only for fullySpec tasks or automated tasks using another algo than RunBased
                if self.type == "fullySpecified" or ( self.type == "partiallySpecified" and self.splittingAlgorithm != "RunBased" ) :

                    for j in task.jobs:
                        if j['jobId'] in sub_jobs[ii]:
                            self.blDBsession.getRunningInstance(j)
                            try:
                                resubNum = int(str(j['arguments']).split(' ')[1]) + 1 
                            except Exception, e:
                                resubNum = j.runningJob['submission'] 
                            newArgs = "%d %d"%(j.runningJob['jobId'], resubNum)
                            j['arguments'] = newArgs

                    self.blDBsession.updateDB( task )

                try:
                    task = self.blSchedSession.submit(task['id'], sub_jobs[ii], reqs_jobs[ii])
                except SchedulerError, e:
                    logMsg = "FW %s. Problem submitting task %s. "%(self.myName, self.taskName)
                    logMsg += str(e.description())
                    self.log.info( logMsg )
                    errorTrace = str( BossLiteLogger( task, e ) )
                    try:
                        self.preLog(logMsg, str(e.description()), 0, errorTrace)
                    except Exception, ee:
                        self.log.error("FW %s Problem logging information: [%s]"%(self.myName,str(ee)))
                except TimeOut, e:
                    logMsg = "FW %s. Problem submitting task %s. "%(self.myName, self.taskName)
                    logMsg += str(e)+'\n'
                    logMsg += "PARTIAL SUBPROCESS MESSAGE : \n%s"%e.commandOutput() 
                    self.log.info( logMsg )
                    errorTrace = str( BossLiteLogger( task, e ) )
                    try:
                        self.preLog(logMsg, str(e.description()), 0, errorTrace)
                    except Exception, ee:
                        self.log.error("FW %s Problem logging information: [%s]"%(self.myName,str(ee)))
                except Exception, ee:
                    logMsg = "FW %s. Unexpected exception submitting task %s. "%(self.myName, self.taskName)
                    logMsg +=  traceback.format_exc()
                    self.log.info( logMsg )

                # check if submitted
                self.log.info("FW %s. Errors: [%s]" %(self.myName, str(errorTrace)) )
                if len(errorTrace) == 0:
                    self.log.info("FW %s. Setting sub success for %s " %(self.myName, str(len(task.jobs))) )
                    parentIds = []
                    for j in task.jobs:
                        self.blDBsession.getRunningInstance(j)
                        if j.runningJob['schedulerId']:
                            submitted.append(j['jobId'])
                            if j['jobId'] in unsubmitted: unsubmitted.remove(j['jobId'])
                            j.runningJob['status'] = 'S'
                            j.runningJob['statusScheduler'] = 'Submitted'
                            j.runningJob['state'] = 'SubSuccess'
                            parentIds.append( j.runningJob['schedulerParentId'] )
                    self.log.info("FW %s Parent IDs for task %s: %s"%(self.myName, self.taskName, str(set(parentIds)) ) )
                    self.SendMLpost( task, sub_jobs[ii] )
                    self.blDBsession.updateDB( task )

        return submitted, unsubmitted, errorTrace


    def evaluateSubmissionOutcome(self, taskObj, submittableRange, submittedJobs, \
            unmatchedJobs, nonSubmittedJobs, skippedJobs):

        resubmissionList = list( set(submittableRange).difference(set(submittedJobs)) )
        logMsg = "FW %s. Task %s summary: \n "%(self.myName, self.taskName)
        logMsg +="\t\t  (%d jobs), submitted %d unmatched %d notSubmitted %d skipped %d"%(
            len(submittableRange), len(submittedJobs), len(unmatchedJobs), len(nonSubmittedJobs), len(skippedJobs) )
        self.log.info( logMsg )
        self.log.debug("FW %s\n"%self.myName + "jobs : %s \nsubmitted %s \nunmatched %s\nnotSubmitted %s\nskipped %s"%(str(submittableRange), \
            str(submittedJobs), str(unmatchedJobs), str(nonSubmittedJobs), str(skippedJobs) )   )

        ## if all the jobs have been submitted send a success message
        if len(resubmissionList) == 0 and len(unmatchedJobs + nonSubmittedJobs + skippedJobs) == 0:
            messagelog = "FW %s Successful complete submission for task %s"%(self.myName,self.taskName)
            self.sendResult(0, "Full Success for %s"%self.taskName, "Worker. %s"%messagelog )
            self.detailLog( submittableRange, submittedJobs, unmatchedJobs, skippedJobs )
            self.preLog(mess = messagelog)
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:CrabWorkPerformed", self.taskName+"::"+messagelog))

            self.log.info("FW %s Submitted jobs: "%self.myName + str(submittedJobs))
            for j in taskObj.jobs:
                if j['jobId'] in submittedJobs:
                    try:
                        self.cwdb.updateWEStatus( j['name'], 'inProgress' )
                    except Exception, ex:
                        logMsg = "FW %s Problem changing status for job "%slef.myName + str(j['name'])+"\n"
                        logMsg +=  traceback.format_exc()
                        self.log.info(logMsg)
                        continue
            self.log.info("FW %s registered jobs entities "%self.myName)
            return
        else:
            ## some jobs need to be resubmitted later
            if len(submittedJobs) == 0:
                self.sendResult(-1, "No jobs submitted for task %s"%self.taskName, \
                    "FW %s. No job submitted: %d more attempts will be performed"%(self.myName, self.resubCount))
            else:
                messagelog = "Partial submission: %d more attempts will be performed"%self.resubCount
                self.local_queue.put((self.myName, "CrabServerWorkerComponent:CrabWorkPerformedPartial", self.taskName+"::"+messagelog))
                self.sendResult(-2, "Partial Success for %s"%self.taskName, "FW %s. %s"%(self.myName, messagelog))

            # propagate the re-submission attempt
            self.resubCount -= 1
            if self.resubCount > 0:
                submissionSource = 'auto'
                payload = self.taskName+"::"+str(self.resubCount)+"::"+str(resubmissionList)+"::"+submissionSource
                self.local_queue.put((self.myName, "CrabServerWorkerComponent:Submission", payload))
                return

            payload = self.taskName+"::"+str(resubmissionList)
            self.local_queue.put((self.myName, "SubmissionFailed", payload))
            toMarkAsFailed = list(set(resubmissionList+unmatchedJobs + nonSubmittedJobs + skippedJobs))
            self.markJobsAsFailed(taskObj, toMarkAsFailed)
            
            # Give up message
            reason = "FW %s has no more attempts: give up with task %s"%(self.myName, self.taskName)
            self.log.info( reason )
            status = "10"
            payload = "%s::%s::%s::%s"%(self.taskName, status, reason, "-1")
            self.detailLog( submittableRange, submittedJobs, unmatchedJobs, skippedJobs)
            self.preLog(mess = reason)
            self.local_queue.put((self.myName, "CrabServerWorkerComponent:SubmitNotSucceeded", payload))
        return

####################################
    # Auxiliary methods
####################################

    def markJobsAsFailed(self, taskObj, toMarkAsFailed):
        for j in taskObj.jobs:
            if j['jobId'] in toMarkAsFailed:
                try:
                    self.blDBsession.getRunningInstance(j)
                except Exception, exc: ## TODO handle proper exception
                    self.log.info("FW %s Problem extracting running job for %s: '%s'"%(self.myName,str(j),str(exc)))
                    continue
                
                try:
                    j.runningJob['state'] = "SubFailed"
                    j.runningJob['closed'] = "Y"
                    self.blDBsession.updateDB( j )
                except Exception, e:
                    logMsg = "FW %s Problem updating BossLite status. Cannot mark task %s job %s as failed\n"%(self.myName, self.taskName, str( j['jobId']))
                    logMsg +=  traceback.format_exc()
                    self.log.info (logMsg)

                try:
                    self.cwdb.updateWEStatus(j['name'], 'reallyFinished')
                except Exception, e:
                    logMsg = "FW %s Problem updating WE status.Cannot mark task %s job %s as failed\n"%(self.myName, self.taskName, str( j['jobId']))
                    logMsg +=  traceback.format_exc()
                    self.log.info (logMsg)
        return

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
                jobs_to_match.append(sub_jobs_temp[0])
            count += 1
            pass

        # ListMatch
        sel = 0
        matched = []
        unmatched = []
        schedParam = ''
        sites = []
        logMsg_dic = {} 

        for id_job in jobs_to_match:
            tags = ''
            if self.bossSchedName in ['SchedulerCondorG']:
                requirements.append( self.sched_parameter_CondorG(id_job, taskObj) )
            elif self.bossSchedName in ['SchedulerGlidein']:
                schedParam, sites = self.sched_parameter_Glidein(id_job, taskObj)
                requirements.append(schedParam)
            elif self.bossSchedName == 'SchedulerLsf':
                requirements.append( self.sched_parameter_Lsf(id_job, taskObj) )
            elif self.bossSchedName in ['SchedulerGLiteAPI', 'SchedulerGLite'] :
                tags_tmp = str(taskObj['jobType']).split('"')
                tags = [str(tags_tmp[1]), str(tags_tmp[3])]
                requirements.append( self.sched_parameter_Glite(id_job, taskObj) )
            elif self.bossSchedName == 'SchedulerARC':
                requirements.append( self.sched_parameter_Arc(id_job, taskObj) )
            else:
                continue

            # Perform listMatching
            if self.bossSchedName in ['SchedulerCondorG',
                                      'SchedulerLsf']:
                matched.append(sel)
            elif self.bossSchedName in ['SchedulerGlidein']:
                if len(sites) > 0:
                    matched.append(sel)
                else:
                    unmatched.append(sel)
            elif self.bossSchedName in ['SchedulerGLite']:
                try: 
                    sites = self.blSchedSession.getSchedulerInterface().matchResources(taskObj, requirements[sel])
                except SchedulerError, e:
                    logMsg = "FW %s. Problem performing List Match for task %s. "%(self.myName, self.taskName)
                    logMsg += str(e.description())+'\n'
                    logMsg += "Found sites: %s"%str(sites)
                    logMsg_dic[id_job] = ""+logMsg
                except TimeOut, e:
                    logMsg = "FW %s. Problem performing List Match for task %s. "%(self.myName, self.taskName)
                    logMsg += str(e)+'\n'
                    logMsg += "Matchmaking timed out. Partial output: \n%s"%e.commandOutput() 
                    logMsg_dic[id_job] = ""+logMsg
                except Exception, ee:
                    logMsg = "FW %s. Unexpected exception performing List Match for task %s. "%(self.myName, self.taskName)
                    logMsg +=  traceback.format_exc()
                    logMsg_dic[id_job] = ""+logMsg

                if len(sites) > 0: 
                    matched.append(sel)
                else: 
                    unmatched.append(sel)
            else :
                cleanedList = None
                if len(distinct_dests[sel]) > 0:
                    seList = distinct_dests[sel]
                    seParser = SEBlackWhiteListParser(self.se_whiteL, self.se_blackL, self.log, self.serviceconfig)
                    cleanedList = seParser.cleanForBlackWhiteList(seList, 'list')
                    if '' in cleanedList :cleanedList.remove('')
                voTags=['cms']
                if self.role != '': voTags.append('VOMS:/cms/Role=%s'%self.role)
                sites = self.blSchedSession.getSchedulerInterface().lcgInfo(tags, voTags, seList=cleanedList, blacklist=self.ce_blackL, whitelist=self.ce_whiteL)
                if len(sites) > 0: matched.append(sel)
                else: unmatched.append(sel)
                
            sel += 1

        # all done and matched, go on with the submission
        return sub_jobs, requirements, matched, unmatched, logMsg_dic

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
        
        VO = self.cfg_params['VO']
        taskType = self.cfg_params.get('USER.tasktype','analysis')
        datasetPath = self.cfg_params['CMSSW.datasetpath']
        if datasetPath.lower() == 'none': datasetPath = None
        executable = self.cfg_params.get('CMSSW.executable','cmsRun')
        appVersion = self.cfg_params.get('CMSSW_version',None)

        params = {'tool': 'crab',\
                  'SubmissionType':'server',\
                  #'JSToolVersion': os.environ['CRAB_SERVER_VERSION'], \
                  # FEDE for savannah 75255
                  'JSToolVersion':self.client_version, \
                  'tool_ui': os.environ['HOSTNAME'], \
                  'scheduler': self.schedName, \
                  'GridName': str(self.owner), \
                  'taskType': taskType, \
                  'vo': VO, \
                  'user': self.taskName.split('_')[0], \
                  'taskId': self.taskName, \
                  'datasetFull': datasetPath, \
                  'ApplicationVersion':appVersion , \
                  'resubmitter': self.submissionSource, \
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
            if self.bossSchedName in ['SchedulerCondorG']:
                hash = self.cfg_params['cfgFileNameCkSum'] #makeCksum(common.work_space.cfgFileName())
                rb = 'OSG'
                jobId = str(jj) + '_' + hash + '_' + jid
            elif self.bossSchedName == 'SchedulerGlidein':
                rb = self.schedName
                jobId = str(jj) + '_https://' + str(jid)
            elif self.bossSchedName == 'SchedulerLsf':
                jobId = str(jj) +"_https://"+self.schedName+":/" + jid + "-" + taskId.replace("_", "-")
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
       #     self.log.info (params)

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
        i = i - 1
        if i < 0:
            i = 0

        # Unpack CMSSW version and architecture from gLite style-string
        [verFrag, archFrag] = task['jobType'].split(',')[0:2]
        version = verFrag.split('-')[-1]
        arch = archFrag.split('-')[-1]
        version = version.replace('"','')
        arch = arch.replace('"','')

        # Get list of SEs and clean according to white/black list
        seList = task.jobs[i]['dlsDestination']
        seParser = SEBlackWhiteListParser(self.se_whiteL, self.se_blackL,
                                          self.log, self.serviceconfig)
        seDest   = seParser.cleanForBlackWhiteList(seList, 'list')

        # Convert to list of CEs and clean according to white/black list
        onlyOSG = True # change for Glidein
        availCEs = getJobManagerList(seDest, version, arch, onlyOSG=onlyOSG)
        ceParser = CEBlackWhiteListParser(self.ce_whiteL, self.ce_blackL,
                                          self.log, self.serviceconfig)
        ceDest   = ceParser.cleanForBlackWhiteList(availCEs, 'list')

        schedParam = "schedulerList = " + ','.join(ceDest) + "; "

        if self.cfg_params['EDG.max_wall_time']:
            schedParam += 'globusrsl = (maxWalltime=%s); ' % \
                          self.cfg_params['EDG.max_wall_time']

        return schedParam


    def sched_parameter_Glidein(self, i, task):
        """
        Parameters specific to CondorG scheduler
        """
        from ProdCommon.BDII.Bdii import getJobManagerList, listAllCEs

        # shift due to BL ranges
        i = i - 1
        if i < 0:
            i = 0

        # Unpack CMSSW version and architecture from gLite style-string
        [verFrag, archFrag] = task['jobType'].split(',')[0:2]
        version = verFrag.split('-')[-1]
        arch = archFrag.split('-')[-1]
        version = version.replace('"','')
        arch = arch.replace('"','')

        # Get list of SEs and clean according to white/black list
        seList = task.jobs[i]['dlsDestination']
        seParser = SEBlackWhiteListParser(self.se_whiteL, self.se_blackL,
                                          self.log, self.serviceconfig)
        seDest   = seParser.cleanForBlackWhiteList(seList, 'list')

        # Convert to list of CEs and clean according to white/black list
        onlyOSG = False
        if (seList == ['']) or (not seList):
            availCEs = listAllCEs(version, arch, onlyOSG=onlyOSG)
        else:
            availCEs = getJobManagerList(seDest, version, arch, onlyOSG=onlyOSG)
        ceParser = CEBlackWhiteListParser(self.ce_whiteL, self.ce_blackL,
                                          self.log, self.serviceconfig)
        ceDest   = ceParser.cleanForBlackWhiteList(availCEs, 'list')
        ceString = ','.join(ceDest)
        seString = ','.join(seDest)
        cmsver=re.split('_', version)
        mcmsver = "%s%.2d%.2d" %(cmsver[1], int(cmsver[2]), int(cmsver[3]))


        myschedName = getfqdn()
        schedParam  = '+DESIRED_Gatekeepers = "' + ceString + '"; '
        schedParam += '+DESIRED_Archs = "INTEL,X86_64"; '
        schedParam += '+DESIRED_SEs = "' + seString + '"; '
        schedParam += '+DESIRED_CMSVersion = "' + version + '"; '
        schedParam += '+DESIRED_CMSVersionNr = ' + mcmsver + '; '
#        schedParam += "Requirements = stringListMember(GLIDEIN_Gatekeeper,DESIRED_Gatekeepers) &&  stringListMember(Arch,DESIRED_Archs); "
        schedParam += '+Glidein_MonitorID = "https://'+ myschedName + '//$(Cluster).$(Process)"; '

        if self.cfg_params['EDG.max_wall_time']:
            schedParam += '+MaxWallTimeMins = %s; ' % \
                          self.cfg_params['EDG.max_wall_time']
        else:
            schedParam += '+MaxWallTimeMins = 1300; '
            schedParam += '+NormMaxWallTimeMins = 600; '

        return schedParam, ceDest


    def sched_parameter_Lsf(self, i, task):
        sched_param= ''
        resDir= "/".join((task['globalSandbox'].split(',')[0]).split('/')[:-1])
        queue =  self.cfg_params.get('CAF.queue','cmscaf1nw')
        res =  self.cfg_params.get('CAF.resource', '"type==SLC5_64 || type==SLC4_64"')
        if (queue):
            sched_param += '-q '+queue +' '
        if (res): sched_param += ' -R '+res +' '
       # sched_param+='-cwd '+resDir + ' '
        return sched_param

    def sched_parameter_Glite(self, i, task):
        """
        Parameters specific to gLite scheduler
        """
        # shift due to BL ranges
        i = i - 1
        if i < 0:
            i = 0

        sched_param = 'Requirements = ' + task['jobType']
        
        self.log.info("FW FEDE sched_param: %s"%sched_param)
         
        req = ''

        if self.cfg_params['EDG.max_wall_time']:
            if (not req == ' '):
                req = req + ' && '
            req += 'other.GlueCEPolicyMaxWallClockTime>=' + self.cfg_params['EDG.max_wall_time']
        
        if self.cfg_params['EDG.max_cpu_time']:
            if (not req == ' '):
                req = req + ' && '
            req += ' other.GlueCEPolicyMaxCPUTime>=' + self.cfg_params['EDG.max_cpu_time']
        
        self.log.info("FW FEDE req: %s"%req)
         
        seReq = self.se_list(i, task.jobs[i]['dlsDestination'])
        ceReq = self.ce_list()
        sched_param += req + seReq + ceReq + ';\n'
        self.log.info("FW FEDE sched_param_fin: %s"%sched_param)

        sched_param += 'MyProxyServer = "' + self.cfg_params['proxyServer'] + '";\n'
        sched_param += 'VirtualOrganisation = "' + self.cfg_params['VO'] + '";\n'
        sched_param += 'RetryCount = '+str(self.cfg_params['EDG_retry_count'])+';\n'
        sched_param += 'DefaultNodeRetryCount = '+str(self.cfg_params['EDG_retry_count'])+';\n'
        sched_param += 'ShallowRetryCount = '+str(self.cfg_params['EDG_shallow_retry_count'])+';\n'
        sched_param += 'DefaultNodeShallowRetryCount = '+str(self.cfg_params['EDG_shallow_retry_count'])+';\n'
        return sched_param


    def sched_parameter_Arc(self,i,task):
        #
        # RTE:s
        #
        xrsl = ""
        for s in task['jobType'].split('&&'):
            if re.search('Member\(".*", .*RunTimeEnvironment', s):
                # Found an RTE; extract its name
                rte = re.sub(" *Member\(\"", "", s)
                rte = re.sub("\", .*", "", rte)
                if re.search('VO-cms-CMSSW_', rte):
                    # If it's a CMSSW RTE, convert it's name from
                    # VO-cms-CMSSW_x_y_z to APPS/HEP/CMSSW-x.y.z
                    rte = re.sub("VO-cms-", "APPS/HEP/", rte)
                    rte = re.sub("_", "-", rte, 1)
                    rte = re.sub("_", ".", rte)
                    rte = rte.upper()
                xrsl += "(runTimeEnvironment=%s)" % rte

        #
        # User supplied parameters
        #
        if self.cfg_params.has_key("GRID.max_cpu_time"):
            s = self.cfg_params["GRID.max_cpu_time"]
            if s.strip()[0] not in ['"', "'"] and s.strip()[-1] not in ['"', "'"]:
                s = '"' + s.strip() + '"'
            xrsl += '(cpuTime=%s)' % s

        if self.cfg_params.has_key('GRID.max_wall_clock_time'):
            s = self.cfg_params["GRID.max_wall_clock_time"]
            if s.strip()[0] not in ['"', "'"] and s.strip()[-1] not in ['"', "'"]:
                s = '"' + s.strip() + '"'
            xrsl += '(wallTime=%s)' % s

        if self.cfg_params.has_key("GRID.additional_xrsl_parameters"):
            self.log.warning("additional_xrsl_parameters is deprecated; use 'additional_jdl_parameters' instead!")
            xrsl += self.cfg_params["GRID.additional_xrsl_parameters"]

        if self.cfg_params.has_key("GRID.additional_jdl_parameters"):
            xrsl += self.cfg_params["GRID.additional_jdl_parameters"]

        return xrsl



    def se_list(self, id, dest):
        """
        Returns string with requirement SE related
        """
        seParser = SEBlackWhiteListParser(self.se_whiteL, self.se_blackL,
                                          self.log, self.serviceconfig)
        seDest   = seParser.cleanForBlackWhiteList(dest, 'list')

        req = ''

        if '' in seDest : seDest.remove('')

        if len(seDest) > 0:
            reqtmp = [ ' Member("'+arg+'" , other.GlueCESEBindGroupSEUniqueID) ' for arg in seDest]
            req += " && (" + '||'.join(reqtmp) + ") "
        return req

    def ce_list(self):
        """
        Returns string with requirement CE related
        """
        ceParser = CEBlackWhiteListParser(self.ce_whiteL, self.ce_blackL,
                                          self.log, self.serviceconfig)

        self.ce_whiteL = ceParser.whiteList()
        self.ce_blackL = ceParser.blackList()
        req = ''
        if self.ce_whiteL:
            tmpCe = []
            concString = '&&'
            for ce in self.ce_whiteL:
                ce = str(ce).strip()
                if len(ce)==0:
                    continue
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
            tmpCe = []
            concString = '&&'
            for ce in self.ce_blackL:
                ce = str(ce).strip()
                if len(ce)==0:
                    continue
                tmpCe.append('(!RegExp("' + str(ce).strip() + '", other.GlueCEUniqueId))')
            if len(tmpCe):
                req += " && (" + concString.join(tmpCe) + ") "

        # requirement added to skip gliteCE
        #req += '&& (!RegExp("blah", other.GlueCEUniqueId))'
        return req

    def detailLog( self,jtodo,jdone,jnotm,jskip ):
        """
        _detailLog_
        """
        if len(jtodo) > 0:
            self.infotolog['submittableRange'] = str(jtodo)
        if jtodo > 0:
            self.infotolog['submittedJobs'] = str(jdone)
        if jtodo > 0:
            self.infotolog['nonSubmittedJobs'] = str(jnotm)
        if jtodo > 0:
            self.infotolog['skippedJobs'] = str(jskip)

    def preLog(self, mess = None, err = None, time = None, exc = None):
        """
        _preLog_
        """
        self.infotolog['reason']    = mess
        self.infotolog['error']     = err
        self.infotolog['time']      = time
        self.infotolog['exc']       = exc
        if len(self.se_blackL) > 0:
            self.infotolog["SE-Black"] = str(self.se_blackL)
        if len(self.se_whiteL) > 0:
            self.infotolog["SE-White"] = str(self.se_whiteL)
        if len(self.ce_blackL) > 0:
            self.infotolog["CE-Black"] = str(self.ce_blackL)
        if len(self.ce_whiteL) > 0:
            self.infotolog["CE-White"] = str(self.ce_whiteL)

        self.infoLogger(self.taskName, self.infotolog)

    def infoLogger(self, taskname, diction, jobid = -1):
        """
        _infoLogger_
        Send the default message to log the information in the task/job log info
        """
        from IMProv.IMProvAddEntry import Event
        eve = Event( )
        eve.initialize( diction )
        import time
        unifilename = os.path.join(self.wdir, taskname+"_spec", str(time.time())+".pkl")
        eve.dump( unifilename )
        message = "TTXmlLogging"
        payload  = taskname + "::" +str(jobid) + "::" + unifilename
        self.log.info("FW %s Sending %s."%(self.myName,message))
        self.local_queue.put((self.myName, message, payload))
        self.log.info("FW %s Registering information:\n%s"%(self.myName,str(diction)))

