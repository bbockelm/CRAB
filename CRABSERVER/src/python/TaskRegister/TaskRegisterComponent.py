#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.4 $"
__revision__ = "$Id: TaskRegisterComponent.py,v 1.4 2008/08/22 13:31:51 spiga Exp $"

import os
import pickle

import logging
from logging.handlers import RotatingFileHandler
import traceback
import Queue

from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

from TaskRegister.RegisterWorker import *

class TaskRegisterComponent:
    """
    _TaskRegisterComponent_
    
    """
    
################################
#   Standard Component Core Methods      
################################
    def __init__(self, **args):
        logging.info(" [TaskRegisterComponent starting...]")
        self.args = {}
        
        self.args.setdefault('Logfile', None)
        self.args.setdefault('dropBoxPath', None)
        self.args.setdefault('ProxiesDir', None)

        # SE support parameters
        # Protocol = local cannot be the default. Any default allowd 
        # for this parameter... it must be defined from config file. 
        self.args.setdefault('Protocol', '')
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

        ## persistent properties
        self.taskPool = {}  # for data persistence
        self.subTimes = []  # moving average for submission delays

        ## volatile properties
        self.wdir = self.args['ComponentDir']
        if self.args['dropBoxPath']: self.wdir = self.args['dropBoxPath']
        self.maxThreads = int( self.args.get('maxThreads', 5) )
        self.availWorkersIds = [ "worker_%d"%mId for mId in xrange(self.maxThreads) ]
        self.workerSet = {} # thread collection

        # shared sessions and queue
        self.blDBsession = BossLiteAPI('MySQL', dbConfig, makePool=True)
        self.sharedQueue = Queue.Queue()
        pass
    
    def startComponent(self):
        """
        _startComponent_
        Start up the component
        """
        self.ms = MessageService()
        self.ms.registerAs("TaskRegisterComponent")
        
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewTask")
        self.ms.subscribeTo("TaskRegisterComponent:StartDebug")
        self.ms.subscribeTo("TaskRegisterComponent:EndDebug")
       
        self.dematerializeStatus()
        #
        # non blocking call event handler loop
        # this allows us to perform actions even if there are no messages
        #
        try:  
            while True:
                type, payload = self.ms.get( wait = False )

                # dispatch loop for Queued messages
                while True:
                    try:
                        senderId, evt, pload = self.sharedQueue.get_nowait()
                        self.ms.publish(evt, pload)
                        logging.debug("Publish Event: %s %s" % (evt, pload))

                        # free resource if no more needed
                        if evt in ["TaskRegisterComponent:NewTaskRegistered", "TaskRegisterComponent:WorkerResult"]: 
                            del self.taskPool[senderId]
                            self.availWorkersIds.append(senderId)
                    except Queue.Empty, e:
                        break

                self.ms.commit()
                self.materializeStatus()
                if type is None:
                    time.sleep( self.ms.pollTime )
                    continue
                self.__call__(type, payload)
        except Exception, e:
            logging.info(e)
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
            
        # usual stuff
        elif event == "TaskRegisterComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
        elif event == "TaskRegisterComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.info('Unknown message received %s'%event)
        return 

################################
    def newTaskRegistration(self, payload):
        """
        Task Registration. Prepares the data structures needed by the server-side to enact
        the real tasks submissions and triggers the RegisterWorker threads.
        """
       
        if len(self.availWorkersIds) <= 0:
            # calculate resub delay by using average completion time
            dT = sum(self.subTimes)/(len(self.subTimes) + 1.0)
            dT = int(dT)
            comp_time = '%s:%s:%s'%(str(dT/3600).zfill(2), str((dT/60)%60).zfill(2), str(dT%60).zfill(2))
            self.ms.publish("CRAB_Cmd_Mgr:NewTask", payload, comp_time)
            self.ms.commit()
            return

        taskUniqName = str(payload)
        thrName = self.availWorkersIds.pop(0)
        self.taskPool[thrName] = ("CRAB_Cmd_Mgr:NewTask", taskUniqName)
        
        actionType = "registerNewTask"
        workerCfg = self.prepareWorkerBaseStatus(taskUniqName, thrName, actionType)
        workerCfg['ProxiesDir'] = self.args['ProxiesDir']
        workerCfg['allow_anonymous'] = self.args['allow_anonymous']
        try:
            self.workerSet[thrName] = RegisterWorker(logging, thrName, workerCfg)
        except Exception, e:
            logging.info('Unable to allocate registration thread: %s'%thrName)
            logging.info(traceback.format_exc())
        return

################################
#   Auxiliary Methods      
################################

    def materializeStatus(self):
        ldump = self.taskPool
        try:
            f = open(self.wdir+"/reg_status.set", 'w')
            pickle.dump(ldump, f)
            f.close()
        except Exception, e:
            logging.info("Error while materializing component status\n"+e)
        return

    def dematerializeStatus(self):
        try:
            f = open(self.wdir+"/reg_status.set", 'r')
            ldump = pickle.load(f)
            f.close()
            self.taskPool = ldump
        except Exception, e:
            logging.info("Failed to open reg_status.set. Building a new status\n" + str(e) )
            self.taskPool = {}
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
        self.taskPool = {}
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

        workerCfg['messageQueue'] = self.sharedQueue
        workerCfg['blSessionPool'] = self.blDBsession.bossLiteDB.getPool()

        workerCfg['allow_anonymous'] = int( self.args.setdefault('allow_anonymous', 0) )
        return workerCfg
        
