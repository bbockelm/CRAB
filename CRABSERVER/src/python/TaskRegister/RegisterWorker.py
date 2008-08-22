#!/usr/bin/env python
"""
_RegisterWorker_

Implements thread logic used to perform Crab task reconstruction on server-side.

"""

__revision__ = "$Id: RegisterWorker.py,v 1.4 2008/07/31 10:21:51 farinafa Exp $"
__version__ = "$Revision: 1.4 $"

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
        return
        
    def run(self):
        self.log.info("RegisterWorker %s initialized"%self.myName)
        self.local_queue.put( (self.myName, "TaskRegister:TaskArrival", self.taskName) )

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
                self.log.info("Task already registered: %s"%self.taskName)
                return tmpTask
 
            taskObj = self.blDBsession.declare(taskSpecFile, self.proxy)
            taskObj['user_proxy'] = self.proxy
        except Exception, e:
            status = 6
            reason = "Error while declaring task %s, it will not be processed"%self.taskName
            self.sendResult(status, reason, reason)
            self.log.info( traceback.format_exc() )
            return None
        
        # get TURL for WMS bypass and manage paths
        self.log.info('Worker %s getting TURL and altering paths'%self.myName)
        if taskObj is not None:
            remoteSBlist = [ os.path.basename(f) for f in taskObj['globalSandbox'].split(',') ]
            remoteSBlist = [ os.path.join( '/'+self.cfg_params['CRAB.se_remote_dir'], f ) for f in remoteSBlist ]

            if len(remoteSBlist) > 0:
                # get the TURL for the first sandbox file
                turlFileCandidate = remoteSBlist[0]
                self.TURLpreamble = SBinterface(self.seEl).getTurl( turlFileCandidate, self.proxy )
    
                # stores only the path without the base filename and correct the last char
                self.TURLpreamble = self.TURLpreamble.split(remoteSBlist[0])[0]

                # correct the task attributes w.r.t. the TURL
                taskObj['globalSandbox'] = ','.join( remoteSBlist )
                taskObj['startDirectory'] = self.TURLpreamble
                taskObj['outputDirectory'] = self.TURLpreamble + self.cfg_params['CRAB.se_remote_dir']
                taskObj['scriptName'] = self.TURLpreamble + os.path.basename(taskObj['scriptName']) 
                taskObj['cfgName'] = self.TURLpreamble + os.path.basename(taskObj['cfgName'])
 
                self.log.debug("Worker %s. Reference TURL: %s"%(self.myName, taskObj['outputDirectory']) )

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
        # all done
        return taskObj 

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

        proxyLink = os.path.join(self.wdir, (self.taskName + '_spec/userProxy'))
        if os.path.exists(proxyLink):
            self.log.info("Project -> Task already associated: %s"%self.taskName )
            self.proxy = str(proxyLink)
            return 0 

        assocFile = self.getProxyFile()
        if assocFile: 
            try:
                cmd = 'ln -s %s %s'%(assocFile, proxyLink)
                cmd = cmd + ' && chmod 600 %s'%assocFile
                if os.system(cmd) == 0:
                    self.proxy = str(proxyLink)
                else:
                    self.proxy = str(assocFile)

                self.log.info("Project -> Task association: %s -> %s"%(self.taskName, assocFile) )

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

        # inactive code for myproxy management of delegated user proxies
        #
        #from myproxyDelegation import myProxyDelegationServerside as myproxyService
        #import sha # to compose secure proxy name
        #
        ##TODO check if these are set in the main component and/or transfered from client-side 
        #srvKeyPath = self.configs.get('X509_KEY', '~/.globus/hostkey.pem') 
        #srvCertPath = self.configs.get('X509_CERT', '~/.globus/hostcert.pem')
        #pf = os.path.join(self.configs['ProxiesDir'], sha.new(self.proxySubject).hexdigest() ) # proxy filename
        #
        #myproxySrv = self.cfg_params.get('EDG.proxy_server', 'myproxy.cern.ch') # from client
        ## retrieve the proxy 
        #try:
        #    mp = myproxyService(srvKeyPath, srvCertPath, myproxySrv) # this could be turned into a class attribute
        #    mp.getDelegatedProxy(pf, proxyArgs=self.cfg_params['EDG.proxyInfos'])
        #    # proxyInfos not a default, strictly required ( stores VOMS extensions), from client
        #    return pf
        #except Exception, e:
        #    self.log.info("Error while retrieving proxy for %s: %s"%(self.proxySubject, str(e) ))
        #    self.log.debug( traceback.format_exc() )
        #    pass
        return None



