#!/usr/bin/env python
"""
_RegisterWorker_

Implements thread logic used to perform Crab task reconstruction on server-side.

"""

__revision__ = "$Id: RegisterWorker.py,v 1.0 2008/06/16 18:00:00 farinafa Exp $"
__version__ = "$Revision: 1.00 $"

import string
import sys, os
import time
import traceback
import commands
from threading import Thread
from xml.dom import minidom

from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI


class RegisterWorker(Thread):
    def __init__(self, logger, FWname, threadAttributes):

        Thread.__init__(self)

        ## Worker Properties
        self.tInit = time.time()
        self.log = logger
        self.myName = FWname
        self.configs = threadAttributes

        self.taskName = self.configs['taskname']
        self.wdir = self.configs['wdir']       
         
        # derived attributes
        self.blDBsession = BossLiteAPI('MySQL', dbConfig)
        self.seEl = SElement(self.configs['SEurl'], self.configs['SEproto'], self.configs['SEport'])
        self.local_ms = self.configs['messageService']
        
        self.cfg_params = {}
        self.proxy = "anonymous"
        self.cmdRng = "[]"
        
        # run the worker
        try:
            self.start()
        except Exception, e:
            self.log.info('RegisterWorker exception : %s'%self.myName)
            self.log.info( traceback.format_exc() )
        return
        
    def run(self):
        self.log.info("RegisterWorker %s initialized"%self.myName)
        self.local_ms.publish("CrabServerWorkerComponent:TaskArrival", self.taskName)
        self.local_ms.commit()

        # reconstruct command structures
        if not self.parseCommandXML() == 0:
            return

        # pair proxy to task
        if not self.associateProxyToTask() == 0 or len(self.proxy)==0:
            return
        
        # declare and customize the task object on the server
        reconstructedTask = self.declareAndLocalizeTask() 
        if reconstructedTask is None:
            return
        
        # check if the ISB are where they should
        if self.inputFileCheck(reconstructedTask) == False:
            return            

        # notify that the task is ready for submission
        payload = self.taskName +"::"+ str(self.configs['retries']) +"::"+ self.cmdRng 
        # add then other usefull infos for scheduling
        self.local_ms.publish("CrabServerWorkerComponent:NewTaskRegistered", payload)
        self.local_ms.commit()

        self.log.info("RegisterWorker %s finished"%self.myName)
        return
        
    def sendResult(self, status, reason, logMsg):
        self.log.info(logMsg)
        msg = self.myName + "::" + self.taskName + "::"
        msg += str(status) + "::" + reason + "::" + str(time.time() - self.tInit)
        self.local_ms.publish("CrabServerWorkerComponent:FatWorkerResult", msg)
        self.local_ms.commit()
        return

####################################
    # RegisterWorker methods 
####################################
    
    def parseCommandXML(self):
        status = 0
        cmdSpecFile = os.path.join(self.wdir, self.taskName + '_spec/cmd.xml' )
        try:
            doc = minidom.parse(cmdSpecFile)
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]
            
            self.cfg_params = eval( cmdXML.getAttribute("CfgParamDict"), {}, {} )
            self.cmdRng =  str( cmdXML.getAttribute('Range') )
            self.proxySubject = str( cmdXML.getAttribute('Subject') ) 
        except Exception, e:
            status = 6
            reason = "Error while parsing command XML for task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
            self.local_ms.commit()
        return status

    def declareAndLocalizeTask(self):
        taskObj = None
        ## create a new task object in the boss session and register its jobs to PA core
        
        ## TODO: this part could be moved to CommandManager, but not now. 
        ## Need to understand better how long declare call takes
        #
        self.log.info('Worker %s declaring new task'%self.myName)
        taskSpecFile = os.path.join(self.wdir, self.taskName + '_spec/task.xml' )
        try:
            tmpTask = None
            try:
                tmpTask = self.blDBsession.loadTaskByName(self.taskName)
            except Exception, ee:
                tmpTask = None 

            if tmpTask is not None:
                self.log.info("Task already registered: %s"%self.taskName)
                return tmpTask
 
            taskObj = self.blDBsession.declare(taskSpecFile, self.proxy)
            taskObj['user_proxy'] = self.proxy
        except Exception, e:
            status = 6
            reason = "Error while declaring task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
            self.local_ms.commit()
            return None
        
        # get TURL for WMS bypass and manage paths
        if taskObj is not None:
            remoteSBlist = [ os.path.basename(f) for f in taskObj['globalSandbox'].split(',') ]
                
            if len(remoteSBlist) > 0:
                # get the TURL for the first sandbox file
                turlFileCandidate = os.path.join(self.cfg_params['CRAB.se_remote_dir'], remoteSBlist[0])
                self.TURLpreamble = SBinterface(self.seEl).getTurl( turlFileCandidate, self.proxy )
    
                # stores only the path without the base filename and correct the last char
                self.TURLpreamble = self.TURLpreamble.split(remoteSBlist[0])[0]
                if self.TURLpreamble:
                    if self.TURLpreamble[-1] != '/':
                        self.TURLpreamble += '/'
    
                # correct the task attributes w.r.t. the TURL
                self.log.debug("Worker %s. Reference TURL: %s"%(self.myName, self.TURLpreamble) )
                taskObj['globalSandbox'] = ','.join(remoteSBlist)
                taskObj['startDirectory'] = self.TURLpreamble
                taskObj['outputDirectory'] = self.TURLpreamble 
                taskObj['scriptName'] = self.TURLpreamble + os.path.basename(taskObj['scriptName']) 
                taskObj['cfgName'] = self.TURLpreamble + os.path.basename(taskObj['cfgName']) 
                for j in taskObj.jobs:
                    j['executable'] = os.path.basename(j['executable'])

                    j['outputFiles'] = [ os.path.basename(of) for of in j['outputFiles']  ]
                    jid = taskObj.jobs.index(j)
                    if 'crab_fjr_%d.xml'%(jid+1) not in j['outputFiles']:
                        j['outputFiles'].append('crab_fjr_%d.xml'%(jid+1)) 
                        #'file://' + destDir +'_spec/crab_fjr_%d.xml'%(jid+1) )
                    if '.BrokerInfo' not in j['outputFiles']:
                        j['outputFiles'].append('.BrokerInfo')

                # save changes
                try: 
                    self.blDBsession.updateDB(taskObj)
                except Exception, e:
                    status = 6
                    reason = "Error while updating task %s, it will not be processed"%self.taskName
                    self.sendResult(status, reason, reason)
                    self.log.info( traceback.format_exc() )
                    self.local_ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", self.taskName + "::" + str(status) + "::" + reason)
                    self.local_ms.commit()
                    return None
        # all done
        return taskObj 

    def inputFileCheck(self, task):
        ## check if the input sandbox is already on the right SE  
        try:
            for f in task['globalSandbox'].split(','):
                remoteFile = os.path.join( str(self.cfg_params['CRAB.se_remote_dir']), f)
                checkCount = 3

                fileFound = False 
                while (checkCount > 0):
                    sbi = SBinterface( self.seEl )
                    fileFound = sbi.checkExists(remoteFile, self.proxy)
                    if fileFound == True:
                        return True
                    checkCount -= 1
                    self.log.info("Worker %s. Checking file %s"%(self.myName, remoteFile))
                    time.sleep(15) 
                    pass
                
                if fileFound == False:
                    status = 20
                    reason = "Worker %s. Missing file %s"%(self.myName, remoteFile)
                    self.sendResult(status, reason, reason)
                    return False
        except Exception, e:
            status = 20
            reason = "Worker %s. Missing file %s"%(self.myName, remoteFile)
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            return False
        
####
## TODO INtegrare queste
#####

    def associateProxyToTask(self):
        """
        Check whether there are macroscopic conditions that prevent the task to be submitted.
        At the same time performs the proxy <--> task association.
        """
        self.proxy = ""

        if int(self.configs['allow_anonymous']) != 0: # and (subj=='anonymous'):
            self.proxy = 'anonymous'
            return 0

        proxyLink = os.path.join(self.wdir, (self.taskName + '_spec/userProxy'))
        if os.path.exists(proxyLink):
            self.log.info("Project -> Task already associated: %s"%self.taskName )
            self.proxy = str(proxyLink)
            return 0 

        assocFile = self.getProxyFile() 
        if assocFile: 
            self.log.info("Project -> Task association: %s -> %s"%(self.taskName, assocFile) )
            try:
                cmd = 'ln -s %s %s'%(assocFile, proxyLink)
                cmd = cmd + ' && chmod 600 %s'%assocFile
                if os.system(cmd) == 0:
                    self.proxy = str(proxyLink)
                else:
                    self.proxy = str(assocFile)
            except Exception, e:
                reason = "Warning: error while linking the proxy file for task %s."%self.taskName 
                self.log.info(reason)
                self.log.info( traceback.format_exc() )
                self.proxy = str(assocFile)
            return 0
            
        status = 20
        reason = "Unable to locate a proper proxy for the task %s"%(self.taskName)
        self.sendResult(status, reason, reason)
        return 2

    def getProxyFile(self):
        proxySubject = str(self.proxySubject)
        proxyDir = self.configs['ProxiesDir']
        pfList = [ os.path.join(proxyDir, q)  for q in os.listdir(proxyDir) if q[0]!="." ]

        for pf in pfList:
            exitCode = -1
            cmd = 'openssl x509 -in '+pf+' -subject -noout'
            try:
                exitCode, ps = commands.getstatusoutput(cmd)
            except Exception, e:
                self.log.info("Error while summoning %s: %s"%(cmd, str(e) ))
                exitCode = -1
                pass

            if exitCode != 0:
                self.log.info("Error while checking proxy subject for %s"%pf)
                continue

            ps = ps.split('\n')
            if len(ps)>0: 
                ps = str(ps[0]).strip()
                if proxySubject in ps or ps in proxySubject:
                    return pf 
        return None



