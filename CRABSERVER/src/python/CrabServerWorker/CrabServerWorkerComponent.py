#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.45 $"
__revision__ = "$Id: CrabServerWorkerComponent.py,v 1.45 2008/06/10 12:59:56 farinafa Exp $"

import os
import pickle

import logging
from logging.handlers import RotatingFileHandler
import traceback
from xml.dom import minidom

from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

from CrabServerWorker.FatWorker import FatWorker
from CrabServerWorker.RegisterWorker import *
from CrabServerWorker.SchedulingWorker import SchedulingWorker, scheduleRequests, descheduleRequests

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
        
        self.args.setdefault('Logfile', None)
        self.args.setdefault('dropBoxPath', None)
        self.args.setdefault('ProxiesDir', None)
        
        # SE support parameters
        self.args.setdefault('Protocol', 'local')
        self.args.setdefault('storageName', 'localhost')
        self.args.setdefault('storagePort', '')
        self.args.setdefault('storagePath', self.args["dropBoxPath"])
        self.args.update(args)
        
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
        logging.getLogger().setLevel(logging.DEBUG)

        # component resources
        if self.args['storagePath'] == None and self.args['Protocol'] == 'local': 
            self.args['storagePath'] = self.args["dropBoxPath"]
            
        ## persistent properties
        self.taskPool = {}  # for data persistence
        self.subTimes = []  # moving average for submission delays
        self.subStats = {'succ':0, 'retry':0, 'fail':0}
 
        ## volatile properties
        self.wdir = self.args['ComponentDir']
        if self.args['dropBoxPath']:
            self.wdir = self.args['dropBoxPath']

        self.maxAttempts = int( self.args.get('maxCmdAttempts', 5) )
        self.availWorkers = int( self.args.get('maxThreads', 5) )
        self.maxThreads = self.availWorkers 
        self.workerSet = {} # thread collection
        
        # pre-allocate a pool of message service instances that will be passed to the workers
        # optimize the overhead for the allocation 
        self.localMsPool = [ MessageService() for mId in xrange( self.maxThreads ) ]
        
        # allocate the scheduling logic
        self.swSchedQ = scheduleRequests
        self.swDeschedQ = descheduleRequests
                
        self.blDBsession = BossLiteAPI('MySQL', dbConfig)

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
      
        self.ms.subscribeTo("CrabServerWorkerComponent:NewTaskRegistered")
        self.ms.subscribeTo("CrabServerWorkerComponent:Submission")
        self.ms.subscribeTo("CrabServerWorkerComponent:FatWorkerResult")
     
        self.ms.subscribeTo("KillTask")
        self.ms.subscribeTo("ResubmitJob")
        
        self.ms.subscribeTo("CrabServerWorkerComponent:StartDebug")
        self.ms.subscribeTo("CrabServerWorkerComponent:EndDebug")
        
        # initialize the local message services pool and schedule logic
        self.schedWorker = SchedulingWorker(self.maxThreads, logging)

        for mId in xrange(self.maxThreads):
            self.localMsPool[mId].registerAs('worker_%d'%mId)

        # usual loop 
        self.dematerializeStatus()   
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CrabServerWorkerComponent: %s %s" % ( type, payload))
            self.__call__(type, payload)
            self.materializeStatus()
        #
        return

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Event: %s %s" % (event, payload))

        # register new task
        if event == "CRAB_Cmd_Mgr:NewTask":
            self.newTaskRegistration(payload)
            
        # enqueue task submissions and perform them
        elif event in ["CrabServerWorkerComponent:NewTaskRegistered", "ResubmitJob", "CRAB_Cmd_Mgr:NewCommand"]: 
            self.enqueueForSubmission(event, payload) # self.handleResubmission(payload)
            
        elif event == "CrabServerWorkerComponent:Submission":
            self.triggerSubmissionWorker(payload)
        
        # fast-kill tasks
        elif event == "KillTask": 
            self.forceDequeuing(payload)        
        
        # worker feedbacks
        elif event == "CrabServerWorkerComponent:FatWorkerResult":
            self.handleWorkerResults(payload)
        
        # usual stuff
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
    def newTaskRegistration(self, payload):
        """
        Task Registration. Prepares the data structures needed by the server-side to enact
        the real tasks submissions and triggers the RegisterWorker threads.
        """
        
        if self.availWorkers <= 0:
            # calculate resub delay by using average completion time
            dT = sum(self.subTimes)/(len(self.subTimes) + 1.0)
            dT = int(dT)
            comp_time = '%s:%s:%s'%(str(dT/3600).zfill(2), str((dT/60)%60).zfill(2), str(dT%60).zfill(2))
            self.ms.publish("CRAB_Cmd_Mgr:NewTask", payload, comp_time)
            self.ms.commit()
            return

        taskUniqName = str(payload)
        workerId = self.maxThreads - self.availWorkers
        thrName = "worker_"+str(workerId)
        
        actionType = "registerNewTask"
        self.taskPool[thrName] = ("CRAB_Cmd_Mgr:NewTask", taskUniqName)
        
        workerCfg = self.prepareWorkerBaseStatus(taskUniqName, workerId, actionType)
        workerCfg['ProxiesDir'] = self.args['ProxiesDir']
        workerCfg['allow_anonymous'] = self.args['allow_anonymous']
        self.availWorkers -= 1
        self.workerSet[thrName] = RegisterWorker(logging, thrName, workerCfg)
        return
        
    def triggerSubmissionWorker(self, payload):
        """
        Submission Command Handler. Triggers the FatWorker threads that perform the
        interactions with the Grid/Local schedulers.
        """
        
        if self.availWorkers <= 0:
            self.ms.publish("CrabServerWorkerComponent:Submission", payload, "00:01:00")
            self.ms.commit()
            return

        items = payload.split('::')
        taskUniqName, resubCount, cmdRng = items[0:3]
        siteToBan = ''
        if len(items) == 4:
            siteToBan = items[3]
        
        workerId = self.maxThreads - self.availWorkers
        thrName = "worker_"+str(workerId)
        
        self.taskPool[thrName] = ("CrabServerWorkerComponent:Submission", payload)

        workerCfg = self.prepareWorkerBaseStatus(taskUniqName, workerId)
        workerCfg['submissionRange'] = eval( cmdRng, {}, {} )
        workerCfg['retries'] = resubCount
        
        ## Additional attributes
        workerCfg['maxRetries'] = self.maxAttempts # NEEDED ONLY FOR wfEntities registration !!! How to avoid?
        workerCfg['se_dynBList'] = [] # TODO does have sense to black-list SEs dynamically?
        workerCfg['ce_dynBList'] = []
        if siteToBan : workerCfg['ce_dynBList'].append(siteToBan)

        workerCfg['cpCmd'] = self.args.get('cpCmd', 'cp')
        workerCfg['rb'] = self.args.get('resourceBroker', 'CERN')
        workerCfg['rfioServer'] = self.args.get('rfioServer', '') 
        workerCfg['EDG_retry_count'] = int(self.args.get('EDG_retry_count', 3) ) 
        workerCfg['EDG_shallow_retry_count'] = int(self.args.get('EDG_shallow_retry_count', 3) ) 
        
        # Specific WMS choice
        workerCfg['wmsEndpoint'] = ''
        customWmsList = str( self.args.get('WMSserviceList', '') ).split(',')
        if len(customWmsList)>0:
            outcomeCounter = sum( self.subStats.values() )
            workerCfg['wmsEndpoint'] = customWmsList[ outcomeCounter % len(customWmsList) ] 
        
        # Worker Factory
        self.availWorkers -= 1
        self.workerSet[thrName] = FatWorker(logging, thrName, workerCfg)
        return

################################
#   Ret-code handler Method
################################

    def handleWorkerResults(self, payload):
        workerName, taskUniqName, status, reason, timing = payload.split('::')
        status = int(status)
        
        ## Free submission resources
        self.availWorkers += 1
        if self.availWorkers > self.maxThreads:
            self.availWorkers = self.maxThreads

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
        outcomeCounter = sum( self.subStats.values() )
        if (outcomeCounter % 20) == 0:
            logging.info('CrabServerWorker component activity summary:\n%s'%str(self.subStats) )
        return


################################
#   New Methods given by the CW factorization (TODO)      
################################

    def enqueueForSubmission(self, event, payload):
        items = payload.split('::')
        taskName, cmdRng, siteToBan = ('', '[]', '')
        
        if event in ['CrabServerWorkerComponent:NewTaskRegistered', 'CRAB_Cmd_Mgr:NewCommand']:
            taskName, retryCounter, cmdRng = items[0:3]
        
        elif event == 'ResubmitJob':
            taskId, jobId = items[0:2]
            try:
                taskName = self.blDBsession.loadTask(taskId, deep=False)['name']
            except Exception, e:
                logging.info("Unable to load task from BossLite. Submission request won\'t be scheduled for taskId=%d"%taskId)
                logging.info(traceback.format_exc())
                return
            
            cmdRng = str( [int(jobId)] )
            retryCounter = '2'
            if len(items) == 3 and items[2] != "#fake_site#": 
                siteToBan = items[2]
                    
        self.swSchedQ.put( (event, taskName, cmdRng, retryCounter, siteToBan) )
        return
    
    def forceDequeuing(self, payload):
        # FAST-KILL
        try:
            taskUniqName, fake_proxy, rng = payload.split(':')
        except Exception, e:
            logging.info('Failed to split the payload for the Kill request:%s'%payload)
            return

        # check whether the task has been previously registered        
        taskObj = None
        try:
            taskObj = self.blDBsession.loadTaskByName(self.taskName)
        except Exception, e:
            taskObj = None

        # remove stuff from persistence tables
        if taskUniqName in self.workerSet: del self.workerSet[taskUniqName]
        if taskUniqName in self.taskPool:  del self.taskPool[taskUniqName]

        # drive the SchedulingWorker to deschedule the task if needed
        self.swDeschedQ.put(taskUniqName)
 
        # force registration and kill
        taskPath = os.path.join(self.wdir, taskUniqName + '_spec/task.xml' )
        if taskObj is None:
           if os.path.exists(taskPath): 
               # not yet registered task
               self.newTaskRegistration(taskUniqName)
               self.ms.publish("KillTask", payload, "00:05:00")
               logging.info("Kill request for not registered task. Postponed kill.")
           else:
               # task never arrived on this server. give up 
               fakeResMsg = "CWmainThr::%s::"%taskUniqName
               fakeResMsg += "6::Missing declaration file for task, unable to kill::0.0"
               self.ms.publish("CrabServerWorkerComponent:FatWorkerResult", fakeResMsg)
               logging.info("Missing declaration file for task, unable to kill %s. Skip command."%taskUniqName)
           self.commit()
         return
    
################################
#   Auxiliary Methods      
################################

    def materializeStatus(self):
        ldump = [self.taskPool, self.subTimes, self.subStats]
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
            self.taskPool, self.subTimes, self.subStats = ldump
        except Exception, e:
            logging.info("Failed to open cw_status.set. Building a new status\n" + str(e) )
            self.taskPool = {}
            self.subTimes = []
            self.subStats = {'succ':0, 'retry':0, 'fail':0}
            self.materializeStatus()
            return

        # cold restart for crashes
        delay = -1
        for t in self.taskPool:
            type, payload = self.taskPool[t]
            delay += 1
            dT = delay/float(self.maxThreads)
            waitTime = '%s:%s:%s'%(str(dT/3600).zfill(2), str((dT/60)%60).zfill(2), str(dT%60).zfill(2))
            self.ms.publish(type, payload, waitTime)
            self.ms.commit()
        return

    def prepareWorkerBaseStatus(self, taskUniqName, workerId, actionType = "standardSubmission"):
        workerCfg = {}        
        workerCfg['wdir'] = self.wdir
        workerCfg['SEproto'] = self.args['Protocol']
        workerCfg['SEurl'] = self.args['storageName']
        workerCfg['SEport'] = self.args['storagePort']

        workerCfg['taskname'] = taskUniqName
        workerCfg['actionType'] = actionType
        workerCfg['retries'] = int( self.args.get('maxRetries', 0) )
        workerCfg['messageService'] = self.localMsPool[ workerId ]
        
        workerCfg['allow_anonymous'] = int( self.args.setdefault('allow_anonymous', 0) )
        return workerCfg
        
