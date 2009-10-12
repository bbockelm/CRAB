#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.90 $"
__revision__ = "$Id: CrabServerWorkerComponent.py,v 1.90 2009/09/30 00:02:44 riahi Exp $"

import os, pickle, time, copy

import logging
from logging.handlers import RotatingFileHandler
import traceback
from xml.dom import minidom
import Queue

from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as wfJob

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

from CrabServerWorker.FatWorker import FatWorker
from CrabServerWorker.SchedulingLogic import SchedulingLogic, scheduleRequests, descheduleRequests

from CrabWorkerAPI import CrabWorkerAPI

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
        self.args.setdefault('CacheDir', None)
        self.args.setdefault('ProxiesDir', None)

        self.args.setdefault('uiConfigWMS', None)
        self.args.setdefault('configFileName', None)
        self.args.setdefault('glexecPath', None)
        self.args.setdefault('glexecWrapper', None)
        self.args.setdefault('CondorQCacheDir', None)

        # SE support parameters
        # Protocol = local cannot be the default. Any default allowd
        # for this parameter... it must be defined from config file.
        self.args.setdefault('Protocol', '')
        self.args.setdefault('storageName', 'localhost')
        self.args.setdefault('storagePort', '')
        self.args.setdefault('storagePath', self.args["CacheDir"])
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

        # component resources
        if self.args['storagePath'] == None and self.args['Protocol'] == 'local':
            self.args['storagePath'] = self.args["CacheDir"]

        ## persistent properties
        self.taskPool = {}  # for data persistence
        self.subTimes = []  # moving average for submission delays
        self.subStats = {'succ':0, 'retry':0, 'fail':0}

        ## volatile properties
        self.wdir = self.args['ComponentDir']
        if self.args['CacheDir']:
            self.wdir = self.args['CacheDir']



        self.maxAttempts = int( self.args.get('maxRetries', 3) )
        self.maxThreads = int( self.args.get('maxThreads', 5) )

        # pre-allocate pool instances that will be passed to the workers
        # optimize the overhead for the allocation
        self.availWorkersIds = [ "worker_%d"%mId for mId in xrange(self.maxThreads) ]
        self.workerSet = {} # thread collection

        # allocate the scheduling logic
        self.swSchedQ = scheduleRequests
        self.swDeschedQ = descheduleRequests
        self.fwResultsQ = Queue.Queue()

        self.blDBsession = BossLiteAPI('MySQL', dbConfig, makePool=True)
        self.cwdb = CrabWorkerAPI( self.blDBsession.bossLiteDB )


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

        # Proxy support
        self.ms.subscribeTo("CrabJobCreatorComponent:NewTaskRegistered")
        self.ms.publish("ProxySubscribe","CrabJobCreatorComponent:NewTaskRegistered")

        self.ms.subscribeTo("TaskRegisterComponent:NewTaskRegistered")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewCommand")

        self.ms.subscribeTo("CrabServerWorkerComponent:Submission")
        self.ms.subscribeTo("CrabServerWorkerComponent:FatWorkerResult")


        self.ms.subscribeTo("KillTask")
        self.ms.subscribeTo("ResubmitJob")

        self.ms.subscribeTo("CrabServerWorkerComponent:StartDebug")
        self.ms.subscribeTo("CrabServerWorkerComponent:EndDebug")

        # initialize the local message services pool and schedule logic
        self.schedLogic = SchedulingLogic(self.maxThreads, logging, self.fwResultsQ, 360)
        # no parametric for now: 10*self.ms.pollTime. a deterministic deadline time seem to be properly set around 5-6 minutes

        self.dematerializeStatus()
        try:
            while True:

                type, payload = self.ms.get( wait = False )
 
                # perform here scheduling activities
                self.schedLogic.applySchedulingLogic()

                # dispatch loop for enqueued messages
                while True:
                    try:
                        senderId, evt, pload = self.fwResultsQ.get_nowait()
                        logging.info("Publishing " + str(evt))
                        self.ms.publish(evt, pload)
                        logging.debug("Publish Event: %s %s" % (evt, pload))
                    except Queue.Empty, e: break

                # standard event handler
                self.ms.commit()
                if type is None:
                    time.sleep( self.ms.pollTime )
                    continue
                self.__call__(type, payload)
        except Exception, e:
            logging.info(e)
        return

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Event: %s %s" % (event, payload))

        # enqueue task submissions and perform them
        if event in ["CrabJobCreatorComponent:NewTaskRegistered", "TaskRegisterComponent:NewTaskRegistered", "ResubmitJob", "CRAB_Cmd_Mgr:NewCommand"]:
            try:
                self.initUiConfigs()
            except Exception, exc:
                logging.info(str(exc))
                logging.info(traceback.format_exc())

            self.enqueueForSubmission(event, payload)

        # fast-kill tasks
        elif event == "KillTask":
            self.forceDequeuing(payload)

        elif event == "CrabServerWorkerComponent:Submission":
            self.triggerSubmissionWorker(payload)


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

    def triggerSubmissionWorker(self, payload):
        """
        Submission Command Handler. Triggers the FatWorker threads that perform the
        interactions with the Grid/Local schedulers.
        """

        if len(self.availWorkersIds) <= 0:
            self.ms.publish("CrabServerWorkerComponent:Submission", payload, "00:01:00")
            self.ms.commit()
            return

        items = payload.split('::')
        taskUniqName, resubCount, cmdRng = items[0:3]
        siteToBan = ''
        if len(items) == 4: siteToBan = items[3]

        thrName = self.availWorkersIds.pop(0)
        self.taskPool[thrName] = ("CrabServerWorkerComponent:Submission", payload)
        self.materializeStatus()

        workerCfg = self.prepareWorkerBaseStatus(taskUniqName, thrName)
        workerCfg['submissionRange'] = eval( cmdRng, {}, {} )
        workerCfg['retries'] = resubCount
        workerCfg['maxRetries'] = self.maxAttempts
        workerCfg['se_dynBList'] = []
        workerCfg['ce_dynBList'] = []
        if siteToBan : workerCfg['ce_dynBList'].append( eval(siteToBan, {}, {}) )
        workerCfg['cpCmd'] = self.args.get('cpCmd', 'cp')
        workerCfg['rfioServer'] = self.args.get('rfioServer', '')
        workerCfg['EDG_retry_count'] = int(self.args.get('EDG_retry_count', 3) )
        workerCfg['EDG_shallow_retry_count'] = int(self.args.get('EDG_shallow_retry_count', 3) )
        workerCfg['glexec'] = self.args.get('glexecPath', '')
        workerCfg['glexecWrapper'] = self.args.get('glexecWrapper', '')
        workerCfg['renewProxy'] = self.args.get('renewProxy', '')
        workerCfg['CondorQCacheDir'] = self.args.get('CondorQCacheDir', '')
        workerCfg['scheduler'] = self.args.setdefault('scheduler','glite' )

        # Specific WMS choice
        workerCfg['wmsEndpoint'] = ''
        customWmsList = str( self.args.get('WMSserviceList', '') ).split(',')
        customWmsList = [ c for c in customWmsList if len(c.strip())>0]
        if len(customWmsList)>0:
            outcomeCounter = sum( self.subStats.values() )
            workerCfg['wmsEndpoint'] = customWmsList[ outcomeCounter % len(customWmsList) ]

        # Worker Factory
        try:
            self.workerSet[thrName] = FatWorker(logging, thrName, workerCfg)
        except Exception, e:
            logging.info('Unable to allocate submission thread: %s'%thrName)
            logging.info(traceback.format_exc())
        return

################################
#   Ret-code handler Method
################################

    def handleWorkerResults(self, payload):
        workerName, taskUniqName, status, reason, timing = payload.split('::')
        status = int(status)

        ## Free submission resources
        if len(workerName)>0:
            # useful to discriminate message from - to the main component (eg. resub failure feedback)
            self.availWorkersIds.append(workerName)

        if workerName in self.taskPool:
            del self.taskPool[workerName]
            self.materializeStatus()
        if workerName in self.workerSet:
            del self.workerSet[workerName]

        ## Track workers outcomes
        successfulCodes = [0, -2] # full and partial submissions
        retryItCodes = [20, 21, 30, 31, -1] # temporary failure conditions mainly
        giveUpCodes = [10, 11, 66, 6] # severe failure conditions

        if status in successfulCodes:
            self.subStats['succ'] += 1
            if status == -2: self.subStats['retry'] += 1
            self.subTimes.append(float(timing))
            if len(self.subTimes) > 64: self.subTimes.pop(0)

        elif status in retryItCodes:
            self.subStats['retry'] += 1
        elif status in giveUpCodes:
            self.subStats['fail'] += 1
        else:
            logging.info('Unknown status for worker message %s'%payload)

        outcomeCounter = sum( self.subStats.values() )
        if (outcomeCounter % 20) == 0:
            logging.info('CrabServerWorker component activity summary: %s'%str(self.subStats) )
        return


################################
#   New Methods given by the CW factorization
################################

    def enqueueForSubmission(self, event, payload):
        items = payload.split('::')
        taskName, cmdRng, siteToBan, retryCounter = ('', '[]', '', '2')
        command = ''

        if event == 'TaskRegisterComponent:NewTaskRegistered' or event == 'CrabJobCreatorComponent:NewTaskRegistered':
            taskName, retryCounter, cmdRng = items[0:3]
        if (event == 'CRAB_Cmd_Mgr:NewCommand') and (items[3] in ['submit','resubmit']):
            taskName, retryCounter, cmdRng = items[0:3]
            command = str(items[3])
        elif event == 'ResubmitJob':
            taskId, jobId = (-1, -1)
            taskId, jobId = items[0:2]
            cmdRng = str( [int(jobId)] )
            if len(items) == 3 and items[2] != "#fake_site#":
                siteToBan = items[2]

            # load task and job data
            try:
                task, taskName, job = (None, None, None)

                task = self.blDBsession.load(taskId, jobId)
                taskName = task['name']
                job = task.jobs[0]

            except Exception, e:
                logging.info("Unable to load task %s from BossLite."%taskName)
                logging.info("Submission request won\'t be scheduled for taskId=%s, jobId=%s"%(str(taskId), str(jobId)) )
                logging.info(traceback.format_exc())
                return

            # check for resubmission count and increment it
            try:
                self.cwdb.increaseSubmission( job['name'] )
                retryCounter = self.cwdb.getWERemainingRetries( job['name'] )
            except Exception, e:
                logging.info("Error while getting WF-Entities for job %s"%job['name'])

            if retryCounter <= 0:
                # no more re-submission attempts, give up.
                try:
                   self.cwdb.updateWEStatus( job['name'], 'reallyFinished' )
                except Exception, e:
                   logging.info("Error while declaring WF-Entities failed for job %sfailed "%job['name'])
                   logging.info(str(e))

                # Propagate info emulating a message in FW results queue
                logging.info("Resubmission has no more attempts: give up with job %s"%job['name'])
                status, reason = ("6", "Command for job %s has no more attempts. Give up."%job['name'])
                payload = "%s::%s::%s::%s"%(taskName, status, reason, job['name'])
                self.fwResultsQ.put(('', "CrabServerWorkerComponent:SubmitNotSucceeded", payload))
                return

        if len(taskName) > 0 :
            schedEntry = (event, taskName, cmdRng, str(retryCounter), siteToBan)
            logging.debug( "Scheduling entry: %s" % str(schedEntry) )
            self.swSchedQ.put( schedEntry )
        else:
            if command != '' :
                MsgLog  = "Empty task name. Bad format scheduling request. Task will be skipped"
                MsgLog += "\t\t event = %s, payload =  %s"%(str(event), str(items))
                logging.info( MgLog )
        return

    def forceDequeuing(self, payload):
        # FAST-KILL
        try:
            taskUniqName, rng = payload.split(':')
        except Exception, e:
            logging.info('Failed to split the payload for the Kill request:%s'%payload)
            return

        # remove stuff from persistence tables
        if taskUniqName in self.workerSet: del self.workerSet[taskUniqName]
        if taskUniqName in self.taskPool:  del self.taskPool[taskUniqName]

        # drive the SchedulingWorker to deschedule the task if needed
        self.swDeschedQ.put(taskUniqName)

        # check whether the task has been previously registered
        taskObj = None
        try:
            taskObj = self.blDBsession.loadTaskByName(taskUniqName)
        except Exception, e:
            logging.info('Error while loading %s for fast kill'%taskUniqName )

        return

################################
#   Auxiliary Methods
################################

    def materializeStatus(self):
        ldump = [self.taskPool, self.subTimes, self.subStats]
        if len(self.taskPool)>0:
            logging.debug("Materialized disaster recovery cache: %s"%str(self.taskPool) )
        try:
            f = open(self.wdir+"/cw_status.set", 'w')
            pickle.dump(ldump, f)
            f.close()
        except Exception, e:
            logging.info("Error while materializing component status\n"+str(e))
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
            logging.info("Publishing " + str(type))
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

        if self.args['uiConfigWMS'] != "" and self.args['uiConfigWMS'] != None:
            workerCfg['serviceFile'] = self.args['uiConfigWMS']
        elif self.args['configFileName'] != "" and self.args['configFileName'] != None:
            workerCfg['serviceFile'] = os.path.join( self.wdir, self.args['configFileName'] )

        workerCfg['actionType'] = actionType
        workerCfg['retries'] = int( self.args.get('maxRetries', 3) )

        workerCfg['messageQueue'] = self.fwResultsQ
        workerCfg['blSessionPool'] = self.blDBsession.bossLiteDB.getPool()

        workerCfg['credentialType'] = self.args['credentialType']
        return workerCfg

################################
#   Auxiliary Methods
################################

    def initUiConfigs(self):
        """
        Download the UI Configuration files for the different Schedulers
        These files will be used by Submitting threads to address to correct Brokers
        """

        if self.args['uiConfigWMS'] != "" and self.args['uiConfigWMS'] != None:
            ## file not to be downloaded - manually mantained
            if not os.path.exists( self.args['uiConfigWMS'] ):
                logging.info("WARNING: configuration file '%s' not found!"%(self.args['uiConfigWMS']))
        elif self.args["baseConfUrl"] != "" and self.args["baseConfUrl"] != None:
            ## periodic download of configuration file
            from WMCore.Services.Service import Service
            wmcorecache = {}
            wmcorecache['logger'] = logging.getLogger()
            wmcorecache['endpoint'] = self.args["baseConfUrl"]
            wmcorecache['cachepath'] = self.wdir   ## cache area
            wmcorecache['cacheduration'] = 0.5     ## half an hour
            wmcorecache['timeout'] = 20            ## seconds
            wmcorecache['type'] = "txt/csv"        ## ??
            if self.args['configFileName'] != "" and self.args['configFileName'] != None:
                logging.info("Downloading configuration...")
                servo = Service( wmcorecache )
                servo.refreshCache(self.args['configFileName'], self.args['configFileName'])
            else:
                logging.info("WARNING: configuration file name not found!")
        else:
            logging.info("WARNING: service configuration not found!")

