#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.16 $"
__revision__ = "$Id: TaskRegisterComponent.py,v 1.16 2009/09/02 14:21:33 spiga Exp $"

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
from ProdCommon.BossLite.Common.Exceptions import TaskError, JobError

from TaskRegister.RegisterWorker import *

# CW DB API
from CrabServerWorker.CrabWorkerAPI import CrabWorkerAPI


class TaskRegisterComponent:
    """
    _TaskRegisterComponent_
    
    """
    
################################
#   Standard Component Core Methods      
################################
    def __init__(self, **args):
        self.args = {}
        
        self.args.setdefault('Logfile', None)
        self.args.setdefault('CacheDir', None)
        self.args.setdefault('ProxiesDir', None)

        # SE support parameters
        # Protocol = local cannot be the default. Any default allowd 
        # for this parameter... it must be defined from config file. 
        self.args.setdefault('Protocol', '')
        self.args.setdefault('storageName', 'localhost')
        self.args.setdefault('storagePort', '')
        self.args.setdefault('storagePath', self.args["CacheDir"])

        # specific delegation strategy for glExex
        self.args.setdefault('glExecDelegation', 'false')

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
        logging.getLogger().setLevel(logging.INFO)

        ## persistent properties
        self.subTimes = []  # moving average for submission delays

        self.killingRequestes = {} # fastKill request track

        ## volatile properties
        self.wdir = self.args['ComponentDir']
        if self.args['CacheDir']: self.wdir = self.args['CacheDir']
        self.maxThreads = int( self.args.get('maxThreads', 5) )
        self.availWorkersIds = [ "worker_%d"%mId for mId in xrange(self.maxThreads) ]
        self.workerSet = {} # thread collection

        # shared sessions and queue
        self.blDBsession = BossLiteAPI('MySQL', dbConfig, makePool=True)
        self.sessionPool = self.blDBsession.bossLiteDB.getPool()

        self.sharedQueue = Queue.Queue()

        logging.info(" ")
        logging.info("Starting component...")
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
        self.ms.subscribeTo("KillTask")

        #self.dematerializeStatus()
        #
        # non blocking call event handler loop
        # this allows us to perform actions even if there are no messages
        #
        try:  
            while True:
                # dispatch loop for Queued messages
                while True:
                    try:
                        senderId, evt, pload = self.sharedQueue.get_nowait()
                        taskUniqName = pload.split("::")[0]

                        # free resource if no more needed
                        if evt in ["TaskRegisterComponent:NewTaskRegistered"] and taskUniqName not in self.killingRequestes:
                                self.availWorkersIds.append(senderId)
                                self.ms.publish(evt, pload)
                                logging.info("Publish Event: %s %s" % (evt, pload))
                                self.ms.commit()
                        elif evt in ["RegisterWorkerComponent:RegisterWorkerFailed"]:
                                logging.info("Task %s failed"%taskUniqName)
                                self.markTaskAsNotSubmitted(taskUniqName, 'all')
                        elif taskUniqName in self.killingRequestes:
                                logging.info("Task %s killed by user"%taskUniqName)
                                self.markTaskAsNotSubmitted(taskUniqName, self.killingRequestes[taskUniqName])
                                del self.killingRequestes[taskUniqName]
                    except Queue.Empty, e:
                        logging.debug("Queue empty: " + str(e))
                        break
                    except Exception, exc:
                        logging.error("'Generic' problem: " + str(exc))
                        logging.error( str(traceback.format_exc()) )

                if len(self.availWorkersIds) > 0:
                    try:
                        type, payload = self.ms.get( wait = False )

                        if type is None:
                            time.sleep( self.ms.pollTime )
                            continue
                        else:
                            self.__call__(type, payload)
                            self.ms.commit()
                    except Exception, exc:
                        logging.error("ERROR: Problem managing message...")
                        logging.error(str(exc))
        except Exception, e:
            logging.error(e)
            logging.info(traceback.format_exc())

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
        elif event == "KillTask":
            taskUniqName, cmdRng = payload.split(':')
            self.killingRequestes[taskUniqName] = cmdRng
        # usual stuff
        elif event == "TaskRegisterComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
        elif event == "TaskRegisterComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
        else:
            logging.info('Unknown message received %s + %s'%(event,payload))
        return True

################################
    def newTaskRegistration(self, payload):
        """
        Task Registration. Prepares the data structures needed by the server-side to enact
        the real tasks submissions and triggers the RegisterWorker threads.
        """
 
        logging.info("Checking it: %s"%(str(payload)) )
        taskUniqName, TotJobs, cmdRng =  payload.split('::')
        thrName = self.availWorkersIds.pop(0)
        
        actionType = "registerNewTask"
        workerCfg = self.prepareWorkerBaseStatus(taskUniqName, thrName, actionType)
        workerCfg['ProxiesDir'] = self.args['ProxiesDir']
        workerCfg['credentialType'] = self.args['credentialType']
        workerCfg['storagePath'] = self.args['storagePath']
        try:
            self.workerSet[thrName] = RegisterWorker(logging, thrName, workerCfg)
        except Exception, e:
            logging.info('Unable to allocate registration thread: %s'%thrName)
            logging.info(traceback.format_exc())
        return True

################################
    def markTaskAsNotSubmitted(self, taskUniqName, cmdRng):
        # load task and init CW APIs Session
        taskObj = None
        cwdb = None
        try:
            taskObj = self.blDBsession.loadTaskByName(taskUniqName)
        except TaskError, te:
            logging.error("Problem loading task '%s': %s" %(taskUniqName,str(te)))
        except Exception, exc:
            logging.error("Generic exception when loading task from blite: %s" %str(exc))

        try:
            cwdb = CrabWorkerAPI( self.blDBsession.bossLiteDB )
            if taskObj is not None:
                if cmdRng == 'all':
                    cmdRng = [ j['jobId'] for j in taskObj.jobs ]
                else:
                    cmdRng = eval(cmdRng, {}, {}) 
            else:
                cmdRng = eval(str(self.getRangeFromXml(self.wdir, taskUniqName)), {}, {})
        except Exception, e:
            logging.info('Unable to allocate CW API Session or unable to load %s.'%taskUniqName)
            logging.info(traceback.format_exc())
            return

        if taskObj is not None:
            # register we_Jobs
            jobSpecId = []
            for job in taskObj.jobs:
                jobName = job['name']
                cacheArea = os.path.join( self.wdir, str(taskUniqName + '_spec'), jobName )
                jobDetails = {'id':jobName, 'job_type':'Processing', 'cache':cacheArea, \
                              'owner':taskUniqName, 'status': 'create', \
                              'max_retries':self.args['maxRetries'], 'max_racers':1 }

                try:
                    if cwdb.existsWEJob(jobName) == True:
                        continue
                    cwdb.registerWEJob(jobDetails)
                    if job['jobId'] in cmdRng:
                        jobSpecId.append(jobName)
                except Exception, e:
                    logging.error(str(e))
                    logging.error(traceback.format_exc())
                    continue

            # mark as failed
            cwdb.stopResubmission(jobSpecId)
            for jId in jobSpecId:
                try:
                    cwdb.updateWEStatus(jId, 'reallyFinished')
                except Exception, e:
                    logging.error(str(e))
                    logging.error(traceback.format_exc())
                    continue
            logging.info('Task %s successfully marked as fast-killed'%taskUniqName)
        else:
            logging.error("Fallback not submitted marking for '%s'"%taskUniqName)
            for jobbe in cmdRng:
                jobName = taskUniqName + "_job" + str(jobbe)
                try:
                    if not cwdb.existsWEJob(jobName):
                        cacheArea = os.path.join( self.wdir, str(taskUniqName + '_spec'), jobName )
                        jobDetails = {'id':jobName, 'job_type':'Processing', 'cache':cacheArea, \
                                      'owner':taskUniqName, 'status': 'reallyFinished', \
                                      'max_retries':self.args['maxRetries'], 'max_racers':1 }
                        cwdb.registerWEJob(jobDetails)
                    else:
                        cwdb.updateWEStatus(jId, 'reallyFinished')
                except Exception, e:
                    logging.error(str(e))
                    logging.error(traceback.format_exc())

        return 

################################
#   Auxiliary Methods      
################################

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
        workerCfg['blSessionPool'] = self.sessionPool
        workerCfg['credentialType'] = self.args.setdefault('credentialType',None ) 
        workerCfg['single_user'] = self.args.setdefault('singleUser',None ) 
        workerCfg['scheduler'] = self.args.setdefault('scheduler','glite' )

        workerCfg['glExecDelegation'] = self.args['glExecDelegation']

        return workerCfg
        
    def getRangeFromXml(self, wdir, taskName):
        from xml.dom import minidom
        status = 0
        cmdSpecFile = os.path.join(wdir, taskName + '_spec/cmd.xml' )
        try:
            doc = minidom.parse(cmdSpecFile)
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]
            cmdRng =  eval( str(cmdXML.getAttribute('Range')) )
        except Exception, e:
            return []
        return cmdRng

