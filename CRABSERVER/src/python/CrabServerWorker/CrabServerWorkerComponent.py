#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.42 $"
__revision__ = "$Id: CrabServerWorkerComponent.py,v 1.42 2008/06/04 18:26:08 farinafa Exp $"

import os
import pickle
import popen2

# logging
import logging
from logging.handlers import RotatingFileHandler
import traceback
import xml.dom.minidom

from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService

from CrabServerWorker.FatWorker import FatWorker

class CrabServerWorkerComponent:
    """
    _CrabServerWorkerComponentV2_
    
    """

################################
#   Standard Component Core Methods      
################################

    def __init__(self, **args):
        logging.info(" [CrabServerWorker Ver2 starting...]")

        self.args = {}
        self.args.setdefault("Logfile", None)
        self.args.setdefault("dropBoxPath", None)
        self.args.setdefault('ProxiesDir', None)

        self.args.setdefault('maxThreads', 5)
        self.args.setdefault('maxCmdAttempts', 5)

        self.args.setdefault('allow_anonymous', 0)
        self.args.setdefault('resourceBroker', 'CERN')
        self.args.setdefault('WMSserviceList', '')
        self.args.setdefault('EDG_retry_count', 3)
        self.args.setdefault('EDG_shallow_retry_count', 3)

        self.args.setdefault('Protocol', 'local')
        self.args.setdefault('storageName', 'localhost')
        self.args.setdefault('storagePort', '')
        self.args.setdefault('storagePath', self.args["dropBoxPath"])
        self.args.setdefault('maxRetries', '0') 
        self.args.setdefault('cpCmd','cp')
        self.args.setdefault('rfioServer','')
        self.args.update(args)
        
        if self.args['storagePath'] == None and self.args['Protocol'] == 'local': 
            self.args['storagePath'] = self.args["dropBoxPath"]

        self.args['WMSserviceList'] = [] + str(self.args['WMSserviceList']).split(',')        

        # define log file
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        # create log handler
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 7)
        # define log format
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        # component resources
        ## persistent properties
        self.taskPool = {}  # for data persistency
        self.proxyMap = {}  # a cache for the available user tasks
        self.subTimes = []  # moving average for submission delays
        self.subStats = {'succ':0, 'retry':0, 'fail':0}
 
        ## volatile properties
        self.wdir = self.args['ComponentDir']
        if self.args['dropBoxPath']:
            self.wdir = self.args['dropBoxPath']
        
        self.maxAttempts = int(self.args['maxCmdAttempts'])    
        self.availWorkers = int(self.args['maxThreads'])
        self.workerSet = {} # thread collection
        self.outcomeCounter = 0
        
        # allocate the scheduling logic (out of order submission)
        # TODO, not now. Smarter scheduling policy
        # needs to know the number of events or an analogous quantity (e.g. number of sites)
        #
        # HOWTO IMPLEMENT: a loopback message on the sorted tasks.
        # This implies a fake message between the Crab-WS and the submission handler
        # (delay to schedule msgs <-> prioritized FCFS fair scheduler w/o queues OR DLT-aware scheduler

        logging.info("-----------------------------------------")
        logging.info("CrabServerWorkerComponent ver2 Started...")
        logging.info("Component dropbox working directory: %s\n"%self.wdir)        
        pass
    
    def startComponent(self):
        """
        _startComponent_

        Start up the component
        """
        self.ms = MessageService()
        self.ms.registerAs("CrabServerWorkerComponent")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewTask")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewCommand")
        self.ms.subscribeTo("CrabServerWorkerComponent:FatWorkerResult")
        #
        self.ms.subscribeTo("CrabServerWorkerComponent:StartDebug")
        self.ms.subscribeTo("CrabServerWorkerComponent:EndDebug")
        # error handler signal
        self.ms.subscribeTo("ResubmitJob") 

        self.dematerializeStatus()   
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CrabServerWorkerComponent: %s %s" % ( type, payload))
            self.__call__(type, payload)

            self.updateProxyMap()            
            self.materializeStatus()
        #
        return

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Event: %s %s" % (event, payload))

        if event == "CRAB_Cmd_Mgr:NewTask":
            self.newSubmission(payload)
        elif event == "CRAB_Cmd_Mgr:NewCommand":
            self.handleCommands(payload)
        elif event == "CrabServerWorkerComponent:FatWorkerResult":
            self.handleWorkerResults(payload)
        elif event == "ResubmitJob":
            self.handleResubmission(payload)
        elif event == "CrabServerWorkerComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
        elif event == "CrabServerWorkerComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.info('Unknown message received %s'%event)
        return 

################################
#   CWver2 business-logic Methods      
################################
    def newSubmission(self, payload):
        """
        Submission Driver. Prepares the datastructures needed by the server-side to enact
        the real tasks submissions and triggers the FatWorker threads that perform the
        interactions with the Grid.
        
        """
        
        if self.availWorkers <= 0:
            # calculate resub delay by using average completion time
            dT = sum(self.subTimes)/(len(self.subTimes) + 1.0)
            dT = int(dT)
            comp_time = '%s:%s:%s'%(str(dT/3600).zfill(2), str((dT/60)%60).zfill(2), str(dT%60).zfill(2))
            self.ms.publish("CRAB_Cmd_Mgr:NewTask", payload, comp_time)
            self.ms.commit()
            return

        taskUniqName = ''+payload
        status = -1
        reason = "Generic Error"

        try:
            cmdSpecFile = os.path.join(self.wdir, (taskUniqName + '_spec/cmd.xml'))
            doc = xml.dom.minidom.parse(cmdSpecFile)
            node = doc.getElementsByTagName("TaskCommand")[0]
            status, reason = self.associateProxyToTask(taskUniqName, node)
            doc.unlink()
        except Exception, e:
            logging.info( traceback.format_exc() )
            status = 6
            reason = "Error while parsing command XML for task %s, it won't be processed"%taskUniqName
            self.ms.publish("CrabServerWorkerComponent:TaskNotSubmitted", taskUniqName + "::" + str(status) + "::" + reason)
            self.ms.commit()
            return 
        
        if (status != 0):
            self.ms.publish("CrabServerWorkerComponent:TaskNotSubmitted", taskUniqName + "::" + str(status) + "::" + reason)
            self.ms.commit()  
            return

        ## real submission stuff
        workerCfg = self.prepareWorkerBaseStatus()
        workerCfg['submitKind'] = 'first' 
        workerCfg['taskname'] = taskUniqName
        workerCfg['proxy'] = reason
        workerCfg['resubCount'] = self.args['maxCmdAttempts']

        thrName = "worker_"+str(int(self.args['maxThreads']) - self.availWorkers)+"_"+taskUniqName
        self.taskPool[thrName] = ("CRAB_Cmd_Mgr:NewTask", payload)

        self.workerSet[thrName] = FatWorker(logging, thrName, workerCfg)
        self.availWorkers -= 1
        return

    def handleCommands(self, payload):
        
        if self.availWorkers <= 0:
            self.ms.publish("CRAB_Cmd_Mgr:NewCommand", payload, "00:01:00")
            self.ms.commit()
            return

        taskUniqName, resubCount = payload.split('::')
        cmdType = ""
        status = -1
        reason = "Generic Error"
    
        try:
            cmdSpecFile = os.path.join(self.wdir, (taskUniqName + '_spec/cmd.xml') )
            doc = xml.dom.minidom.parse(cmdSpecFile)
            node = doc.getElementsByTagName("TaskCommand")[0]            
            status, reason = self.associateProxyToTask(taskUniqName, node)
            cmdType = str(node.getAttribute('Command'))
            doc.unlink()
        except Exception, e:
            logging.info( traceback.format_exc() ) 
            status = 6
            reason = "Error while parsing command XML for task %s, It won't be processed"%taskUniqName
            self.ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", taskUniqName + "::" + str(status) + "::" + reason)
            self.ms.commit()
            return
 
        # skip non-interesting messages
        if cmdType not in ['kill', 'submit', 'resubmit']:
            return

        ## FAST-KILL handler
        if cmdType == 'kill':
            if taskUniqName in self.workerSet:
                del self.workerSet[taskUniqName]
            if taskUniqName in self.taskPool:
                del self.taskPool[taskUniqName]
            return
        
        if (status == 4):
            # CAVEAT in this case it means that everything is ok
            status = 0
            reason = self.proxyMap[ str(node.getAttribute('Subject')) ]

        if (status != 0):
            self.ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", taskUniqName + "::" + str(status) + "::" + reason)
            self.ms.commit()  
            return

        # run FatWorker
        workerCfg = self.prepareWorkerBaseStatus()
        workerCfg['submitKind'] = 'subsequent'
        workerCfg['taskname'] = taskUniqName
        workerCfg['proxy'] = reason
        workerCfg['resubCount'] = resubCount

        thrName = "worker_"+str(int(self.args['maxThreads']) - self.availWorkers)+"_"+taskUniqName
        self.taskPool[thrName] = ("CRAB_Cmd_Mgr:NewCommand", payload)

        self.workerSet[thrName] = FatWorker(logging, thrName, workerCfg)
        self.availWorkers -= 1
        return

################################
#   Resubmission Method for signals from ErrorHandler
################################

    def handleResubmission(self, payload):
        try:
            taskId, jobId, siteToBan = payload.split('::')
        except Exception, e:
            logging.info("Bad resubmission message format. Resubmission will not be performed")
            return 

        if self.availWorkers <= 0:
            self.ms.publish("ResubmitJob", payload, "00:00:30")
            self.ms.commit()
            return

        workerCfg = self.prepareWorkerBaseStatus()
        workerCfg['submitKind'] = 'errHdlTriggered'
        workerCfg['taskname'] = str(taskId)
        workerCfg['proxy'] = ''
        workerCfg['resubCount'] = 2

        # Error Handler specific parameters
        workerCfg['ce_dynBList'] += [siteToBan]
        workerCfg['taskId'] = taskId
        workerCfg['jobId'] = jobId

        # run FatWorker
        thrName = "worker_"+str(int(self.args['maxThreads']) - self.availWorkers)+"_"+str(taskId)+"."+str(jobId)
        self.taskPool[thrName] = ("ResubmitJob", payload)

        self.workerSet[thrName] = FatWorker(logging, thrName, workerCfg)
        self.availWorkers -= 1
        return 

################################
#   Ret-code handler Method
################################

    def handleWorkerResults(self, payload):
        workerName, taskUniqName, status, reason, timing = payload.split('::')
        status = int(status)
        self.outcomeCounter += 1

        ## Free submission resources
        self.availWorkers += 1
        if self.availWorkers > int(self.args['maxThreads']):
            self.availWorkers = int(self.args['maxThreads'])

        if workerName in self.workerSet:
            del self.workerSet[workerName]
        if workerName in self.taskPool: 
            del self.taskPool[workerName]

        ## Track workers outcomes
        successfulCodes = [0, -2] # full and partial submissions
        retryItCodes = [20, 21, 30, 31, -1] # temporary failure conditions mainly
        giveUpCodes = [10, 11, 66, 6] # severe failure conditions
 
        if status in successfulCodes:
            self.subStats['succ'] += 1 
            if status == -2:
                self.subStats['retry'] += 1
            self.subTimes.append(float(timing))
            if len(self.subTimes) > 64:
                self.subTimes.pop(0)
            
        elif status in retryItCodes: 
            self.subStats['retry'] += 1

        elif status in giveUpCodes:
            self.subStats['fail'] += 1
        else: 
            logging.info('Unknown status for worker message %s'%payload)

        # print statistics at fixed periods
        if self.outcomeCounter > 64:
            self.outcomeCounter = 0
            logging.info('Worker activity summary:\n%s'%str(self.subStats) )
        return

################################
#   Auxiliary Methods      
################################
    
    def updateProxyMap(self):
        if int(self.args['allow_anonymous']) !=0 :
            return
 
        pfList = []
        proxyDir = self.args['ProxiesDir']

        # old gridsite version
        for root, dirs, files in os.walk(proxyDir):
            for f in files:
                if f == 'userproxy.pem':
                    pfList.append(os.path.join(root, f))

        # new gridsite version
        if len(pfList) == 0:
            pfList = [ os.path.join(proxyDir, q)  for q in os.listdir(proxyDir) if q[0]!="." ]

        # Get an associative map between each proxy file and its subject
        for pf in pfList:
            if pf in self.proxyMap.values():
                continue

            try:
                ps = str(os.popen3('openssl x509 -in '+pf+' -subject -noout')[1].readlines()[0]).strip()
            except Exception, e:
                missingProxy = 0
                psCandidates = list(self.proxyMap.keys())

                # remove expired proxies from cache 
                for ps2Remove in psCandidates:
                    if not os.path.exists(self.proxyMap[ps2Remove]):
                        del self.proxyMap[ps2Remove]
                        missingProxy = 1

                # check whether the problem is something else...
                if missingProxy == 0:
                    logging.info("Error while checking proxy subject for %s"%pf)
                    continue

            if len(ps) > 0:
                self.proxyMap[ps] = pf
        #     
        return

    def associateProxyToTask(self, taskUniqName, node):
        """
        Check whether there are macroscopic conditions that prevent the task to be submitted.
        At the same time performs the proxy <--> task association.
        """
        reason = ""
        subj = str(node.getAttribute('Subject'))

        # is it an unwanted clone?
        if taskUniqName in self.workerSet:
            reason = "Already submitting task %s. It won't be processed"%taskUniqName
            logging.info(reason)
            return 4, reason

        if int(self.args['allow_anonymous']) != 0: # and (subj=='anonymous'):
            return 0, 'anonymous'
 
        for psubj in self.proxyMap:
            if subj in psubj: 
                assocFile = self.proxyMap[psubj]
                logging.info("Project -> Task association: %s -> %s"%(taskUniqName, assocFile) )
                try:
                    proxyLink = os.path.join(self.wdir, (taskUniqName + '_spec/userProxy'))
                    if not os.path.exists(proxyLink):
                        cmd = 'ln -s %s %s'%(assocFile, proxyLink)
                        cmd = cmd + ' && chmod 600 %s'%assocFile
                        os.system(cmd)
                except Exception, e:
                    reason = "Warning: error while linking the proxy file for task %s."%taskUniqName 
                    logging.info(reason)
                    logging.info( traceback.format_exc() )
                    return 0, assocFile 
                return 0, assocFile
            
        reason = "Unable to locate a proper proxy for the task %s with subject %s"%(taskUniqName, subj)
        logging.info(reason)
        logging.debug(self.proxyMap)
        return 2, reason

    def materializeStatus(self):
        ldump = []
        ldump.append(self.taskPool)
        ldump.append(self.proxyMap)
        ldump.append(self.subTimes)
        ldump.append(self.subStats)
        try:
            f = open(self.wdir+"/cw_status.set", 'w')
            pickle.dump(ldump, f)
            f.close()
        except Exception, e:
            logging.info("Error while materializing component status\n"+e)
        return

    def dematerializeStatus(self):
        try:
            f = open(self.wdir+"/cw_status.set", 'r')
            ldump = pickle.load(f)
            f.close()
            self.taskPool, self.proxyMap, self.subTimes, self.subStats = ldump
        except Exception, e:
            logging.info("Failed to open cw_status.set. Building a new status\n" + str(e) )
            self.taskPool = {}
            self.proxyMap = {}
            self.subTimes = []
            self.subStats = {'succ':0, 'retry':0, 'fail':0}
            self.materializeStatus()
            return

        # cold restart for crashes
        delay = -1
        for t in self.taskPool:
            type, payload = self.taskPool[t]
            delay += 1
            dT = delay/float(self.args['maxThreads'])
            waitTime = '%s:%s:%s'%(str(dT/3600).zfill(2), str((dT/60)%60).zfill(2), str(dT%60).zfill(2))
            self.ms.publish(type, payload, waitTime)
            self.ms.commit()
        return

    def prepareWorkerBaseStatus(self):
        workerCfg = {}
        
        workerCfg['rb'] = '' + self.args['resourceBroker']
        workerCfg['wdir'] = '' + self.wdir
        
        workerCfg['SEproto'] = self.args['Protocol']
        workerCfg['SEurl'] = self.args['storageName']
        workerCfg['SEport'] = self.args['storagePort']

        workerCfg['wmsEndpoint'] = self.args['WMSserviceList'][self.outcomeCounter%len(self.args['WMSserviceList'])]
        workerCfg['se_dynBList'] = []
        workerCfg['ce_dynBList'] = []
        workerCfg['EDG_retry_count'] = self.args['EDG_retry_count']
        workerCfg['EDG_shallow_retry_count'] = self.args['EDG_shallow_retry_count']
        workerCfg['allow_anonymous'] = int(self.args['allow_anonymous'])
        workerCfg['maxRetries'] = int(self.args['maxRetries'])
        workerCfg['cpCmd'] = self.args['cpCmd']
        workerCfg['rfioServer'] = self.args['rfioServer']

        return workerCfg
        
