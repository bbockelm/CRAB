#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.0 $"
__revision__ = "$Id: CrabServerWorkerComponentV2.py,v 1.0 2007/12/17 06:33:00 farinafa Exp $"

import os
import pickle
import popen2

# logging
import logging
from logging.handlers import RotatingFileHandler

import xml.dom.minidom

from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService

from CrabServerWorker.FatWorker import FatWorker

# from CrabServer.DashboardAPI import apmonSend, apmonFree    

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
        ## persistent properties
        self.taskPool = {}  # for data persistency
        self.proxyMap = {}  # a cache for the available user tasks
        self.subTimes = []  # moving average for submission delays
        self.subStats = {'succ':0, 'retry':0, 'fail':0}
 
        ## volatile properties
        self.wdir = self.args['ComponentDir']
        if self.args['dropBoxPath']:
            self.wdir = self.args['dropBoxPath']
        
        self.maxAttempts = self.args['maxCmdAttempts']    
        self.availWorkers = self.args['maxThreads']
        self.workerSet = {} # thread collection
        self.outcomeCounter = 0
        
        # allocate the scheduling logic (out of order submission)
        # TODO, not now. Smarter scheduling policy
        # needs to know the number of events or an analogous quantity (e.g. number of sites)
        #
        # HOWTO IMPLEMENT: a loopback message on the sorted tasks.
        # This implies a fake message between the Crab-WS and the submission handler
        # (delay to schedule msgs <-> prioritized FCFS fair scheduler w/o queues OR DLT-aware scheduler

        logging.info("CrabServerWorkerComponent ver2 Started...")
        logging.info("Component dropbox working directory: %s"%self.wdir)        
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
        # TODO # Fabio
        # It should register to ErrorHandler messages.
        # So that it can decide whether to retry submissions
        #
        self.ms.subscribeTo("CrabServerWorkerComponent:StartDebug")
        self.ms.subscribeTo("CrabServerWorkerComponent:EndDebug")

        self.dematerializeStatus()   
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CrabServerWorkerComponent: %s %s" % ( type, payload))
            self.__call__(type, payload)
        #
        apmonFree()
        return

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Event: %s %s" % (event, payload))

        if event == "CRAB_Cmd_Mgr:NewTask":
            self.updateProxyMap()
            self.newSubmission(payload)
            self.materializeStatus()
            
        elif event == "CRAB_Cmd_Mgr:NewCommand":
            self.updateProxyMap()
            self.handleCommands(payload)
            self.materializeStatus()

        elif event == "CrabServerWorkerComponent:FatWorkerResult":
            self.handleWorkerResults(payload)
            self.materializeStatus()

        if event == "CrabServerWorkerComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)

        if event == "CrabServerWorkerComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)

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

        taskDescriptor, cmdDescriptor, taskUniqName = payload.split('::')
        
        # no workers availabe (delay submission)
        if self.availWorkers <= 0:
            # calculate resub delay by using average completion time
            dT = sum(self.subTimes)/(len(self.subTimes) + 1.0)
            dT = int(dT)
            comp_time = '%s:%s:%s'%(str(dT/3600).zfill(2), str((dT/60)%60).zfill(2), str(dT%60).zfill(2))
            self.ms.publish("CRAB_Cmd_Mgr:NewTask", payload, comp_time)
            self.ms.commit()
            return
        
        status, reason = self.taskIsSubmittable(cmdDescriptor, taskUniqName)
        if (status != 0):
            self.ms.publish("CrabServerWorkerComponent:TaskNotSubmitted", taskUniqName + "::" + str(status) + "::" + reason)
            self.ms.commit()  
            return

        # prepare the task working directories
        taskDir = self.prepareTaskDir(taskDescriptor, cmdDescriptor, reason, taskUniqName)
        if (taskDir is None):
            status = 8
            reason = "Problem while creating the task base directory"
            self.ms.publish("CrabServerWorkerComponent:TaskNotSubmitted", taskUniqName + "::" + str(status) + "::" + reason)
            self.ms.commit()
            return

        ## DashBoard information preparation
        # dashParams = {'jobId':'TaskMeta', 'taskId':'unknown', 'jobId':'unknown'}
        # dashParams.update( dashB_mlCommonInfo )

        # submit pre DashBoard information
        # apmonSend(dashParams['taskId'], dashParams['jobId'], dashParams)
        # logging.debug('Submission DashBoard Pre-Submission report: '+str(dashParams))
        
        ## real submission stuff
        thrName = "worker_"+(self.args['maxThreads'] - availWorkers)+"_"+taskUniqName            
        self.workerSet[taskUniqName] = FatWorker(logging, thrName, self.args['resourceBroker'], \
                                                taskDescriptor, cmdDescriptor, taskUniqName, \
                                                reason, taskDir, resubCount) # , dashParams, [])
        self.taskPool[taskUniqName] = ("CRAB_Cmd_Mgr:NewTask", payload)
        self.availWorkers -= 1
        return

    def handleCommands(self, payload):
        cmdDescriptor, taskUniqName, resubCount = payload.split('::')
        node = xml.dom.minidom.parseString(cmdDescriptor).getElementsByTagName("TaskAttributes")
        cmdType = '' + str(node.getAttribute('Command'))
        del node

        # no more TTL. Send failure and give up
        if resubCount < 0:
            logging.info("Command for task %s has no more attempts. Give up."%taskUniqName)
            self.ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", taskUniqName + "::" + str(status) + "::" + reason) 
            self.ms.commit()
            return 
 
        # skip non-interesting messages
        if cmdType not in ['kill', 'submit']:
            return

        ## FAST-KILL handler
        if cmdType == 'kill':
            if taskUniqName in self.workerSet:
                del self.workerSet[taskUniqName]

            self.ms.publish("KillTask", taskUniqName)
            self.ms.commit()

            if taskUniqName in self.taskPool:
                del self.taskPool[taskUniqName]
            return

        ## SUBSEQUENT SUBMISSIONS for a task
        if self.availWorkers <= 0:
            self.ms.publish("CRAB_Cmd_Mgr:NewCommand", payload, "00:01:00")
            self.ms.commit()
            return
        
        status = -1
        status, reason = self.taskIsSubmittable(cmdDescriptor, taskUniqName)
        if (status == 4):
            # CAVEAT in this case it means that everything is ok
            status = 0
            reason = self.proxyMap[ node.getAttribute('Subject') ]

        if (status != 0):
            self.ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", taskUniqName + "::" + str(status) + "::" + reason)
            self.ms.commit()  
            return

        taskDir = self.wdir + '/' + taskUniqName
        if not os.path.exists(taskDir):
            status = 8
            reason = "Task working directory %s does not exist. The task did not submit here."%taskDir
            self.ms.publish("CrabServerWorkerComponent:SubmitNotSucceeded", taskUniqName + "::" + str(status) + "::" + reason)
            self.ms.commit()
            return
        
        # Dashboard parameters    
        # dashParams = {'jobId':'TaskMeta', 'taskId':'unknown', 'jobId':'unknown'}
        # dashParams.update( dashB_mlCommonInfo )
 
        # run FatWorker
        thrName = "worker_"+(self.args['maxThreads'] - availWorkers)+"_"+taskUniqName
        self.workerSet[thrName] = FatWorker(logging, thrName, self.args['resourceBroker'], \
                                                         None, cmdDescriptor, taskUniqName, \
                                                         reason, taskDir, resubCount) #, dashParams, [])
        self.taskPool[thrName] = ("CRAB_Cmd_Mgr:NewCommand", payload)
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
        availWorkers += 1
        if availWorkers > self.args['maxThreads']:
            availWorkers = self.args['maxThreads']
        del self.workerSet[workerName]
        del self.taskPool[workerName]

        ## Track workers outcomes
        successfulCodes = [0, -2] # full and partial submissions
        retryItCodes = [20, 21, 30, 31, -1] # temporary failure conditions mainly
        giveUpCodes = [10, 11] # severe failure conditions
 
        if status in successfulCodes:
            self.subStats['succ'] += 1 
            if status == -2:
                self.subStats['retry'] += 1
            # moving average with fixed window (adapt if needed but not parametric) 
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
        pfList = []
        proxyDir = self.args['ProxiesDir']
        
        # old gridsite version
        for root, dirs, files in os.walk(proxyDir):
            for f in files:
                if f == 'userproxy.pem':
                    pfList.append(os.path.join(root, f))

        # new gridsite version
        if len(pfList)==0:
            pfList = [ proxyDir + '/'+q  for q in os.listdir(proxyDir) if q[0]!="." ]

        # Get an associative map between each proxy file and its subject
        for pf in pfList:
            if pf in self.proxyMap.values():
                continue
            
            ps = str(os.popen3('openssl x509 -in '+pf+' -subject -noout')[1].readlines()[0]).strip()
            if len(ps) > 0:
                self.proxyMap[ps] = pf
        #     
        return

    def taskIsSubmittable(self, cmdDescriptor, taskUniqName):
        """
        Check whether there are macroscopic conditions that prevent the task to be submitted.
        At the same time performs the proxy <--> task association.
        """
        reason = ""

        # has it a proper proxy?
        try:
            doc = xml.dom.minidom.parseString(cmdDescriptor)
        except Exception, e:
            reason = "Error while parsing command XML for task %s. It won't be processed."%taskUniqName
            logging.info(reason)
            return 6, reason
        
        node = doc.getElementsByTagName("TaskAttributes")
       
        if (self.args['allow_anonymous']!=0) and (node.getAttribute('Subject')=='anonymous'):
            return 0, 'anonymous'
 
        if node.getAttribute('Subject') not in self.proxyMap:
            reason = "Unable to locate a proper proxy for the task %s"%taskUniqName
            return 2, reason
        
        # is it an unwanted clone?
        if taskUniqName in self.workerSet:
            reason = "Already submitting task %s. It won't be processed"%taskUniqName
            logging.info(reason)
            return 4, reason
        
        reason = self.proxyMap[ node.getAttribute('Subject') ]
        self.log.info("Project -> Task association: %s -> %s"%(taskUniqName, reason) )

        # get mlCommonInfo
        # dashB_mlCommon = {} 
        # if node.hasAttribute('mlCommonInfo'):
        #    for i in str( node.getAttribute('mlCommonInfo') ).split('\n'):
        #        val = i.split(':')
        #        dashB_mlCommon[val[0]] = str(val[1]).strip()

        return 0, reason # , dashB_mlCommon

    def prepareTaskDir(self, taskDescriptor, cmdDescriptor, proxyFile, taskUniqName):
        """
        create the task directory storing the commodity files for the task
        
        """
        taskDir = self.wdir + '/' + taskUniqName
        
        # create folders if needed
        if os.path.exists(taskDir):
            self.log.info("Already existing folder for task %s. The user proxy is already linked there."%taskUniqName)
            return None

        try:    
            os.mkdirs(taskDir + '/res')
            os.mkdir(taskDir + '/log')
        except Exception, e:
            self.log.info("Error crating directories for task %s"%taskUniqName)
            return None

        # materialize files for safety 
        try:
            f = open(taskDir + '/task_' + taskUniqName + '.xml', 'w')
            f.write(taskDescriptor)
            f.close()
            del f 
            f = open(taskDir + '/cmd_' + taskUniqName + '.xml', 'w')
            f.write(cmdDescriptor)
            f.close()
        except Exception, e:
            self.log.info("Error saving xml files for task %s"%taskUniqName)
            return None
        
        # link the proxy (if required) 
        if proxyFile != 'anonymous':
            try:
                cmd = 'ln -s %s %s'%(proxyFile, taskDir + '/userProxy')
                cmd = cmd + ' && chmod 600 %s'%proxyFile
                os.system(cmd)
            except Exception, e:
                self.log.info("Error creating the link to proxy for %s"%taskUniqName)
                return None
        return taskDir

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
            self.taskPool, self.proxyMap, self.subTimes = ldump
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
            dT = delay/self.args['maxThreads']
            waitTime = '%s:%s:%s'%(str(dT/3600).zfill(2), str((dT/60)%60).zfill(2), str(dT%60).zfill(2))
            self.ms.publish(type, payload, waitTime)
            self.ms.commit()
        return

