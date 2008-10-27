#!/usr/bin/env python
"""
_RegisterWorker_

Implements thread logic used to perform Crab task reconstruction on server-side.

"""

__revision__ = "$Id: RegisterWorker.py,v 1.10 2008/09/26 07:36:27 spiga Exp $"
__version__ = "$Revision: 1.10 $"

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
        self.blDBsession = BossLiteAPI('MySQL', dbConfig, pool=self.configs['blSessionPool'])
        self.seEl = SElement(self.configs['SEurl'], self.configs['SEproto'], self.configs['SEport'])
        self.local_queue = self.configs['messageQueue']
        
        self.cfg_params = {}
        self.proxy = "anonymous"
        self.cmdRng = "[]"
        
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
        if not self.associateProxyToTask() == 0 or len(self.proxy)==0:
            self.local_queue.put((self.myName, "RegisterWorkerComponent:RegisterWorkerFailed", self.taskName))
            return

        # declare and customize the task object on the server
        reconstructedTask = self.declareAndLocalizeTask() 
        if reconstructedTask is None:
            self.local_queue.put((self.myName, "RegisterWorkerComponent:RegisterWorkerFailed", self.taskName))
            return

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
            self.proxySubject = str( cmdXML.getAttribute('Subject') )

            self.schedName = str( cmdXML.getAttribute('Scheduler') ).upper()

            self.flavour = str( cmdXML.getAttribute('Flavour') ) 
            self.type = str( cmdXML.getAttribute('Type') )
        except Exception, e:
            status = 6
            reason = "Error while parsing command XML for task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
        return status

    def declareAndLocalizeTask(self):
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
                self.log.info("Task %s already registered in BossLite"%self.taskName)
                return tmpTask
 
            taskObj = self.blDBsession.declare(taskSpecFile, self.proxy)
            taskObj['user_proxy'] = self.proxy
        except Exception, e:
            status = 6
            reason = "Error while declaring task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            return None
          
        if taskObj is not None:
            taskObj = self.alterPath(taskObj)
         
        # all done
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
        """
        self.log.info('Worker %s altering paths'%self.myName)

        remoteSBlist = [ os.path.basename(f) for f in taskObj['globalSandbox'].split(',') ]
        remoteSBlist = [ os.path.join( '/'+self.cfg_params['CRAB.se_remote_dir'], f ) for f in remoteSBlist ]

        if len(remoteSBlist) > 0:
            if self.schedName in ['GLITE','GLITECOLL']:
                # get TURL for WMS bypass and manage paths
                self.log.info('Worker %s getting TURL (Scheduler Name %s)  '%(self.myName, self.schedName))
                turlFileCandidate = remoteSBlist[0]
                self.preamble = SBinterface(self.seEl).getTurl( turlFileCandidate, self.proxy )
                self.preamble = self.preamble.split(remoteSBlist[0])[0]
            elif self.schedName in ['LSF','CAF']:
                self.log.info('Worker %s  NO TURL needed (Scheduler Name %s)  '%(self.myName, self.schedName))
                self.preamble = ''
            elif self.schedName in ['CONDOR_G','ARC'] : 
                self.preamble = ''
                self.log.info('Worker %s  NO TURL needed (Scheduler Name %s)  '%(self.myName, self.schedName))
            else:
                self.log.info('Worker %s  Scheduler %s  Not Known  '%(self.myName, self.schedName))
                return None

            # correct the task attributes w.r.t. the Preamble
            taskObj['globalSandbox'] = ','.join( remoteSBlist )
            taskObj['startDirectory'] = self.preamble
            taskObj['outputDirectory'] = self.preamble + self.cfg_params['CRAB.se_remote_dir']
            taskObj['cfgName'] = self.preamble + os.path.basename(taskObj['cfgName'])
 
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
        self.log.info('Worker %s checking input sanbox'%self.myName) 
        sbList = task['globalSandbox'].split(',')
        if len(sbList)==0: return True 

        try:
            for f in sbList:
                remoteFile = f #os.path.join( str(self.cfg_params['CRAB.se_remote_dir']), f)
                checkCount = 3

                fileFound = False
                while (checkCount > 0):
                    sbi = SBinterface( self.seEl )
                    fileFound = sbi.checkExists(remoteFile, self.proxy)
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
        self.proxy = ""
        self.log.info('Worker %s pairing task and proxy'%self.myName)
        if int(self.configs['allow_anonymous']) != 0: # and (subj=='anonymous'):
            self.proxy = 'anonymous'
            return 0

        try:
            assocFile = self.getProxyFile()
            #assocFile = self.getProxyFileMyProxy()
            if assocFile is not None: 
                self.proxy = str(assocFile)
                self.log.info("Project -> Task association: %s -> %s"%(self.taskName, assocFile) )
                return 0
        except Exception, e:
            reason = "Warning: error while linking the proxy file for task %s."%self.taskName 
            self.log.info(reason)
            self.log.info( traceback.format_exc() )
            
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

    def getProxyFileMyProxy(self):
        from CrabServer.myproxyDelegation import myProxyDelegationServerside as myproxyService
        import sha # to compose secure proxy name
        #
        ##TODO check if these are set in the main component and/or transfered from client-side 
        srvKeyPath = self.configs.get('X509_KEY', '~/.globus/hostkey.pem') 
        srvCertPath = self.configs.get('X509_CERT', '~/.globus/hostcert.pem')
        pf = os.path.join(self.configs['ProxiesDir'], sha.new(self.proxySubject).hexdigest() ) # proxy filename
        #
        myproxySrv = self.cfg_params.get('EDG.proxy_server', 'myproxy-fts.cern.ch') # from client
        ## retrieve the proxy
        try:
            mp = myproxyService(srvKeyPath, srvCertPath, myproxySrv) # this could be turned into a class attribute
            mp.getDelegatedProxy(pf, proxyArgs=self.cfg_params['EDG.proxyInfos'])
            # proxyInfos not a default, strictly required ( stores VOMS extensions), from client
            return pf
        except Exception, e:
            self.log.info("Error while retrieving proxy for %s: %s"%(self.proxySubject, str(e) ))
            self.log.info( traceback.format_exc() )
            pass
        return None



