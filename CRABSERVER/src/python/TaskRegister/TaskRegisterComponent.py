#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.25 $"
__revision__ = "$Id: TaskRegisterComponent.py,v 1.25 2010/08/10 21:58:14 spiga Exp $"

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

# WMCORE 
from WMCore.WMFactory import WMFactory
from WMCore.WMInit import WMInit

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
        self.args.setdefault("HeartBeatDelay", "00:05:00")

        self.args.update(args)
        
        if len(self.args["HeartBeatDelay"]) != 8:
            self.HeartBeatDelay="00:05:00"
        else:
            self.HeartBeatDelay=self.args["HeartBeatDelay"]
       
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
        if self.args['CacheDir']: 
            self.wdir = self.args['CacheDir']
            if not os.path.isdir(self.wdir):
                os.makedirs(self.wdir)

        self.maxThreads = int( self.args.get('maxThreads', 5) )
        self.availWorkersIds = [ "worker_%d"%mId for mId in xrange(self.maxThreads) ]
        self.workerSet = {} # thread collection

        # shared sessions and queue
        self.blDBsession = BossLiteAPI('MySQL', dbConfig, makePool=True)
        self.sessionPool = self.blDBsession.bossLiteDB.getPool()

        self.sharedQueue = Queue.Queue()

        # Get configuration
        self.init = WMInit()
        self.init.setLogging()
        self.init.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))

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

        self.ms.subscribeTo("TaskRegisterComponent:HeartBeat")
        self.ms.remove("TaskRegisterComponent:HeartBeat")
        self.ms.publish("TaskRegisterComponent:HeartBeat","",self.HeartBeatDelay)
        self.ms.commit()

        # TaskRegister registration in WMCore.MsgService
        self.myThread = threading.currentThread()
        self.factory = WMFactory("msgService", "WMCore.MsgService."+ \
                             self.myThread.dialect)
        self.newMsgService = self.myThread.factory['msgService'].loadObject("MsgService")
        self.myThread.transaction.begin()
        self.newMsgService.registerAs("TaskRegisterComponent")
        self.myThread.transaction.commit()

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

                        # dealloc threadId
                        if senderId not in self.availWorkersIds:
                            self.availWorkersIds.append(senderId)

                        # dispatch the messages and update status 
                        if evt in ["TaskRegisterComponent:NewTaskRegisteredPartially"]:
                                logging.info("Task %s registred Partially"%taskUniqName) 
                        elif evt in ["TaskRegisterComponent:NewTaskRegistered"] and taskUniqName not in self.killingRequestes:
                                self.ms.publish(evt, pload)
                                logging.info("Publish Event: %s %s" % (evt, pload))
                                self.ms.commit()
                        elif evt in ["RegisterWorkerComponent:WorkerFailsToRegisterPartially"]:
                                logging.info("Task %s failed partially"%taskUniqName)
                        elif evt in ["RegisterWorkerComponent:RegisterWorkerFailed"]:
                                logging.info("Task %s failed"%taskUniqName)
                                self.markTaskAsNotSubmitted(taskUniqName, 'all')
                                self.ms.publish(evt, pload)
                                logging.info("Publish Event: %s %s" % (evt, pload))
                                self.ms.commit()
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
        elif event == "TaskRegisterComponent:HeartBeat":
            logging.info("HeartBeat: I'm alive ")
            self.ms.publish("TaskRegisterComponent:HeartBeat","",self.HeartBeatDelay)
            self.ms.commit()
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

        if int(TotJobs) > 5000 :
            logging.info("TASK HAS TOO MANY JOBS [%s] SKIP IT" % TotJobs)
            return False

        actionType = "registerNewTask"
        workerCfg = self.prepareWorkerBaseStatus(taskUniqName, thrName, actionType)
        workerCfg['ProxiesDir'] = self.args['ProxiesDir']
        workerCfg['credentialType'] = self.args['credentialType']
        workerCfg['storagePath'] = self.args['storagePath']

        workerCfg['defaultScheduler'] = self.args.setdefault('defaultScheduler','glite' )
        workerCfg['supportedSchedulers'] = self.args.get('supportedSchedulers', '').upper().split(',')

        try:
            # Shared newMsgService object passed to thread
            self.workerSet[thrName] = RegisterWorker(logging, thrName, workerCfg, self.newMsgService)
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
            if taskObj is None:
                raise TaskError("loadTaskByName returned a None object")

            # parse range
            if cmdRng == 'all':
                cmdRng = [ j['jobId'] for j in taskObj.jobs ]
            else:
                cmdRng = eval(cmdRng, {}, {})

        except TaskError, te:
            logging.error("Problem loading task '%s': %s" %(taskUniqName, str(te) ) )
            return
        except Exception, exc:
            logging.error("Generic exception when loading task from blite: %s" %str(exc))
            return
 
        try:
            cwdb = CrabWorkerAPI( self.blDBsession.bossLiteDB )
        except Exception, e:
            logging.info('Unable to allocate CW API Session for task  %s.'%taskUniqName)
            logging.info(traceback.format_exc())
            return

        jobSpecId = []

        ## set job status, close running instances and prepare SpecIds for we_ tables
        for j in taskObj.jobs:
            if j['jobId'] in cmdRng:
                jobName = j['name']
                jobSpecId.append(jobName)

                # close running jobs and mark state 
                try:
                    self.blDBsession.getRunningInstance(j)
                except Exception, exc: 
                    logMsg = "Problem extracting runningJob for %s: '%s'"%(str(j), str(exc))
                    logMsg += "Creating a new runningJob instance"
                    self.log.error(logMsg)
                    self.blDBsession.getNewRunningInstance(j)

                j.runningJob['state'] = "SubFailed"
                j.runningJob['closed'] = "Y"

                # mark we_Jobs  
                cacheArea = os.path.join( self.wdir, str(taskUniqName + '_spec'), jobName )
                jobDetails = {'id':jobName, 'job_type':'Processing', 'cache':cacheArea, \
                              'owner':taskUniqName, 'status': 'create', \
                              'max_retries':self.args['maxRetries'], 'max_racers':1 }

                try:
                    if not cwdb.existsWEJob(jobName):
                        cwdb.registerWEJob(jobDetails)
                    cwdb.updateWEStatus(jobName, 'reallyFinished')
                except Exception, e:
                    logging.error(str(e))
                    logging.error(traceback.format_exc())

        try: 
            self.blDBsession.updateDB( taskObj )
            cwdb.stopResubmission(jobSpecId)
        except Exception, e:
            logging.error(str(e))
            logging.error(traceback.format_exc())

        logging.info('Task %s successfully marked as fast-killed'%taskUniqName)
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

