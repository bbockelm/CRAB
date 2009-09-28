#!/usr/bin/env python
"""
_RegisterWorker_

Implements thread logic used to perform Crab task reconstruction on server-side.

"""

__revision__ = "$Id: RegisterWorker.py,v 1.23 2009/09/02 14:21:33 spiga Exp $"
__version__ = "$Revision: 1.23 $"

import string
import sys, os
import time
import traceback
import commands
from threading import Thread
from xml.dom import minidom
import sha

from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig

from ProdCommon.Credential.CredentialAPI import CredentialAPI
from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

from CrabServerWorker.CrabWorkerAPI import CrabWorkerAPI

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
        self.blDBsession = BossLiteAPI('MySQL', pool=self.configs['blSessionPool'])
        self.seEl = SElement(self.configs['SEurl'], self.configs['SEproto'], self.configs['SEport'])
        self.local_queue = self.configs['messageQueue']

        self.cfg_params = {}
        self.CredAPI = CredentialAPI({'credential':self.configs['credentialType'], 'logger':self.log}) 
        self.cmdRng = "[]"
        self.schedName= self.configs['scheduler'].upper()

        self.useGlExecDelegation = self.configs['glExecDelegation'] == 'true'

        # run the worker
        try:
            self.start()
        except Exception, e:
            self.log.info('RegisterWorker exception : %s'%self.myName)
            self.log.info( traceback.format_exc() )
        ## CW DB init
        self.cwdb = CrabWorkerAPI( self.blDBsession.bossLiteDB )
        return

    def run(self):
        self.log.info("RegisterWorker %s initialized"%self.myName)
        self.local_queue.put( (self.myName, "TaskRegister:TaskArrival", self.taskName) )

        # reconstruct command structures
        if not self.parseCommandXML() == 0:
            self.local_queue.put((self.myName, "RegisterWorkerComponent:RegisterWorkerFailed", self.taskName))
            return

        # pair proxy to task
        if not self.associateProxyToTask() == 0 :
            self.local_queue.put((self.myName, "RegisterWorkerComponent:RegisterWorkerFailed", self.taskName))
            return

        # declare and customize the task object on the server
        reconstructedTask = self.declareAndLocalizeTask()
        if reconstructedTask is None:
            self.local_queue.put((self.myName, "RegisterWorkerComponent:RegisterWorkerFailed", self.taskName))
            return
        else:
            reconstructedTask = self.alterPath(reconstructedTask)

        # register jobs of the task object on the server (we_job)
        registeredTask = self.registerTask(reconstructedTask)
        if registeredTask != 0:
            self.local_queue.put((self.myName, "RegisterWorkerComponent:RegisterWorkerFailed", self.taskName))
            return

        # check if the ISB are where they should
        if self.inputFileCheck(reconstructedTask) == False:
             self.local_queue.put((self.myName, "RegisterWorkerComponent:RegisterWorkerFailed", self.taskName))
             return

        if self.type == 'fullySpecified': # default by client side to backward compatibility
           payload = self.taskName +"::"+ str(self.configs['retries']) +"::"+ self.cmdRng
           self.local_queue.put( (self.myName, "TaskRegisterComponent:NewTaskRegistered", payload) )
        else:
           id = 'taskid'
           wmbs = WMBSInterface(_input_stuff_)
           result = wmbs.run()

           # interact with WMBS ... + messaggio "ritorna qui tra n-ore"
           # prepare a payload for the interaction message
           # self.local_queue.put( (self.myName, "TaskRegisterComponent:msgXYZ", payload) )

        self.log.info("RegisterWorker %s finished"%self.myName)
        return

    def sendResult(self, status, reason, logMsg):
        self.log.info(logMsg)
        msg = self.myName + "::" + self.taskName + "::"
        msg += str(status) + "::" + reason + "::" + str(time.time() - self.tInit)
        self.local_queue.put( (self.myName, "TaskRegisterComponent:WorkerResult", msg) )
        return

####################################
    # RegisterWorker methods
####################################

    def parseCommandXML(self):
        status = 0
        self.log.info('Worker %s parsing creation command'%self.myName)
        cmdSpecFile = os.path.join(self.wdir, self.taskName + '_spec/cmd.xml' )
        try:
            doc = minidom.parse(cmdSpecFile)
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]

            self.cfg_params = eval( cmdXML.getAttribute("CfgParamDict"), {}, {} )
            self.cmdRng =  str( cmdXML.getAttribute('Range') )
            self.owner = str( cmdXML.getAttribute('Subject') )

            ## This is to make the scheduler configurable from user
            #self.schedName = str( cmdXML.getAttribute('Scheduler') ).upper()

            self.flavour = str( cmdXML.getAttribute('Flavour') )
            self.type = str( cmdXML.getAttribute('Type') )
        except Exception, e:
            status = 6
            reason = "Error while parsing command XML for task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
        return status

    def declareAndLocalizeTask(self):
        """
        """
        taskObj = None
        self.log.info('Worker %s declaring new task'%self.myName)
        taskSpecFile = os.path.join(self.wdir, self.taskName + '_spec/task.xml' )
        try:
            tmpTask = None
            try:
                tmpTask = self.blDBsession.loadTaskByName(self.taskName)
            except Exception, ee:
                tmpTask = None

            if tmpTask is not None:
                self.log.info("Worker %s - Task %s already registered in BossLite"%(self.myName,self.taskName) )
                return tmpTask

            taskObj = self.blDBsession.declare(taskSpecFile, self.credential)
            taskObj['user_proxy'] = self.credential
        except Exception, e:
            status = 6
            reason = "Error while declaring task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( str(e) )
            self.log.info( traceback.format_exc() )
            return None

        return taskObj

    def alterPath(self, taskObj):
        """
        here the Input and Output SB locations
        are defined and related fields are filled.
        These infos are finally picked-up for JDL creation
        for this reason the related management is scheduler
        dependent.
        Allowed Schedulers Name:
        1) GLITE
        2) GLITECOLL
        3) CONDOR_G
        4) ARC
        5) LSF
        6) CAF
        7) GLIDEIN
        """
        self.log.info('Worker %s altering paths'%self.myName)

        remoteSBlist = [ os.path.basename(f) for f in taskObj['globalSandbox'].split(',') ]
        self.log.info("[%s]" %self.cfg_params['CRAB.se_remote_dir'])
        self.log.info("[%s]" %self.configs['storagePath'])
        #if self.cfg_params['CRAB.se_remote_dir'] == "":
        #    remoteSBlist = [ os.path.join( '/'+self.cfg_params['CRAB.se_remote_dir'], f ) for f in remoteSBlist ]
        #else:
        remoteSBlist = [ os.path.join( os.path.join(self.configs['storagePath'], self.taskName), f ) for f in remoteSBlist ]
        self.log.info(str(remoteSBlist))

        if len(remoteSBlist) > 0:
            if self.schedName in ['GLITE','GLITECOLL']:
                # get TURL for WMS bypass and manage paths
                self.log.info('Worker %s getting TURL (Scheduler Name %s)  '%(self.myName, self.schedName))
                turlFileCandidate = remoteSBlist[0]
                self.preamble = SBinterface(self.seEl).getTurl( turlFileCandidate, self.credential )
                self.preamble = self.preamble.split(remoteSBlist[0])[0]
            elif self.schedName in ['LSF', 'CAF', 'CONDOR_G', 'ARC', 'GLIDEIN']:
                self.log.info('Worker %s  NO TURL needed (Scheduler Name %s)  '%(self.myName, self.schedName))
                self.preamble = ''
            else:
                self.log.info('Worker %s  Scheduler %s  Not Known  '%(self.myName, self.schedName))
                return None

            # correct the task attributes w.r.t. the Preamble
            taskObj['globalSandbox'] = ','.join( remoteSBlist )
            taskObj['startDirectory'] = self.preamble
            taskObj['outputDirectory'] = self.preamble + self.cfg_params['CRAB.se_remote_dir']
            taskObj['cfgName'] = self.preamble + os.path.join ( self.cfg_params['CRAB.se_remote_dir'], os.path.basename(taskObj['cfgName']) )

            self.log.debug("Worker %s. Reference Preamble: %s"%(self.myName, taskObj['outputDirectory']) )

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
                return None

        return taskObj


    def registerTask(self, taskArg):
        """  
        """  

        taskName = taskArg['name']

        range = eval(self.cmdRng)

        for job in taskArg.jobs:
            jobName = job['name']
            cacheArea = os.path.join( self.wdir, taskName + '_spec', jobName )
            we_status = 'create'
            if job['jobId'] in range : we_status = 'Submitting'
            jobDetails = {
                          'id':jobName, 'job_type':'Processing', 'cache':cacheArea, \
                          'owner':taskName, 'status': we_status, \
                          'max_retries': self.configs['retries'], 'max_racers':1 \
                         }
            jobAlreadyRegistered = False
            try:
                jobAlreadyRegistered = self.cwdb.existsWEJob(jobName)
            except Exception, e:
                ##TODO: need to differnciate when more then 1 entry per job (limit case)
                logMsg = 'Error while checking job registration: assuming %s as not registered'%jobName
                logMsg +=  traceback.format_exc()
                self.log.info (logMsg)
                jobAlreadyRegistered = False

            if jobAlreadyRegistered == True:
                continue

            self.log.debug('Registering %s'%jobName)
            try:
                self.cwdb.registerWEJob(jobDetails)
            except Exception, e:
                logMsg = 'Error while registering job for JT: %s'%jobName
                logMsg +=  traceback.format_exc()
                self.log.info (logMsg)
                return 1
            self.log.debug('Registration for %s performed'%jobName)
        return 0

    def inputFileCheck(self, task):
        self.log.info('Worker %s checking input sandbox'%self.myName)
        sbList = task['globalSandbox'].split(',')
        if len(sbList)==0: return True
        ###VERY Termporary: FIXME
        username=''
        if self.schedName.upper() in ['LSF','CAF']: 
            username='%s::'%task['name'].split('_')[0]
        ###VERY Termporary: FIXME

        try:
            for f in sbList:
                remoteFile = f #os.path.join( str(self.cfg_params['CRAB.se_remote_dir']), f)
                checkCount = 3

                fileFound = False
                while (checkCount > 0):
                    sbi = SBinterface( self.seEl )
                    ###VERY Termporary: FIXME
                    try:
                        fileFound = sbi.checkExists(remoteFile, username+self.credential)
                    except Exception, e:
                        self.log.info( "Error while checking staged sandbox: %s"%str(e) )
                        fileFound = False

                    ###VERY Termporary: FIXME
                    if fileFound == True: break
                    checkCount -= 1
                    self.log.info("Worker %s. Checking file %s"%(self.myName, remoteFile))
                    time.sleep(10)
                    pass

                if fileFound == False:
                    status = 20
                    reason = "Worker %s. Missing file %s"%(self.myName, remoteFile)
                    self.sendResult(status, reason, reason)
                    return False

        except Exception, e:
            status = 20
            reason = "Worker %s. Error checking if file %s exists"%(self.myName, remoteFile)
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            return False
        return True

    def associateProxyToTask(self):
        """
        Check whether there are macroscopic conditions that prevent the task to be submitted.
        At the same time performs the proxy <--> task association.
        """
        self.log.info('Worker %s pairing task and proxy'%self.myName)
        #if int(self.configs['allow_anonymous']) != 0: # and (subj=='anonymous'):
        #    self.proxy = 'anonymous'
        #    return 0

        try:
            self.credential = self.getProxy()
        except Exception, e:
            reason = "Warning: error while linking the proxy file for task %s."%self.taskName
            self.log.info(reason)
            self.log.info( traceback.format_exc() )

        if  self.credential :
            self.log.info("Project -> Task association: %s -> %s"%(self.taskName, self.credential) )
            status = 0
        else:
            status = 20
            reason = "Unable to locate a proper proxy for the task %s"%(self.taskName)
            self.sendResult(status, reason, reason)
        return status

    def getProxy(self):
        """
        """
 
        proxyFilename = os.path.join(self.configs['ProxiesDir'], sha.new(self.owner).hexdigest() ) 

        vo = self.cfg_params.get('VO', 'cms')
        role = self.cfg_params.get('EDG.role', None)
        group = self.cfg_params.get('EDG.group', None)
        proxyServer = self.cfg_params.get("GRID.proxy_server", 'myproxy.cern.ch')

        try:
            # force the CredAPI to refer to the same MyProxy server used by the client 
            self.CredAPI.credObj.myproxyServer = proxyServer
            self.CredAPI.logonMyProxy(proxyFilename, self.owner, vo, group, role)
        except Exception, e:
            self.log.info("Error while retrieving proxy for %s: %s"%(self.owner, str(e) ))
            self.log.info( traceback.format_exc() )
            return None

        if self.useGlExecDelegation == True:
            #### TODO
            #### Sanjay fix here

            # userId = ...
            # serverUserId = ...

            #newProxyfilename = '/tmp/%s'%userId 
            #### Suggestion: probably settint ProxyCache folder as 'del_proxy' counterpart is enough, 
            #### then only a change of ownership is required

            #cmd = 'mv %s %s'%(proxyFilename, newProxyfilename)
            #cmd += && chown %s %s'%(serverUserId, newProxyfilename)
            #ret = os.system(cmd) 
            #if ret != 0:
            #    self.log.info( "Error while moving proxy %s for glExec"%proxyFilename )
            #    return None

            # proxyFilename = newProxyfilename
            pass


        return proxyFilename

