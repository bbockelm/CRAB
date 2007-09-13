#!/usr/bin/env python
"""
_CW_WatchDogComponent.py_

"""

__version__ = "$Revision: 1.2 $"
__revision__ = "$Id: CW_WatchDogComponent.py,v 1.2 2007/07/17 17:20:00 farinafa Exp $"

import os
import socket
import cPickle as pickle
import logging
import time
from logging.handlers import RotatingFileHandler
import commands
import MySQLdb
from MessageService.MessageService import MessageService

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

ms = MessageService()
ms.registerAs("Component1")
ms.subscribeTo("Message1")


class CW_WatchDogComponent:
    """
    _CW_WatchDogComponent_

    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args['dropBoxPath'] = None
        self.args.update(args)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        
        #  Log Handler is a rotating file that rolls over when the
        #  file hits 1MB size, 3 most recent files are kept
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        
        #  Set up formatting for the logger and set the
        #  logging level to info level
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        logging.info("CW_WatchDog Component Started...")

        # Custom attributes
        self.files = {}
        self.dropBoxPath = str(self.args['dropBoxPath'])
        self.queueSize = 0
        self.threshold = 15
        self.timeStat = (0,0)
        self.fastKillList = {}

        # Statistics attributes
        self.arrivedCounter = 0
        self.successCounter = 0
        self.failCounter = 0
        self.resubmitCounter = 0
        self.lastResetSended = time.time()
        

        self.totArrivedCounter = 0
        self.totSuccessCounter = 0
        self.totFailCounter = 0
        self.totResubmitCounter = 0
      
        tmpMs =  MessageService()
        tmpMs.registerAs("CW_WatchDogStat")
        tmpMs.publish("CW_WatchDogComponent:resetStatistics",' ','01:00:00')
        tmpMs.commit()
        self.lastResetSended = time.time() 
        del tmpMs
 
        #Dematerialize the queue
        try:
            f = open(self.dropBoxPath+"/watchDog.set",'r')
            (self.files, self.fastKillList) = pickle.load(f)
            f.close()
            logging.info("Opening WatchDog Status")
        except IOError, ex:
            logging.info("Failed to open watchDog.set. Building a new status item")
            self.files = {}
            pass

        self.queueSize = len(self.files)
        pass

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """

        logging.info("Event: %s %s" % (event, payload))

        if event == "ProxyTarballAssociatorComponent:CrabWork":
            self.arrivedCounter += 1
            self.addEntry(payload)
            return

        if event == "CrabServerWorkerNotifyThread:Retry":
            self.resubmitCounter += 1
            return

        if event in ["CrabServerWorkerComponent:CrabWorkPerformed", "CrabServerWorkerComponent:CrabWorkPerformedPartial"]:
            self.successCounter += 1 
            self.removeEntry(payload)
            return

        if event in ["CrabServerWorkerComponent:CrabWorkFailed"]:
            self.failCounter += 1
            self.removeEntry(payload)
            return

        if event in ["CrabServerWorkerComponent:FastKill", "TaskKilled"]:
            self.removeEntry(payload)
            return  

        if event == "CW_WatchDogComponent:synchronize":
            self.coldRestart()
            return

        if event == "CommandManagerComponent:prekill":
            self.forwardPreKill(payload)
            return

        if event == "CW_WatchDogComponent:resetStatistics":
            self.resetStat(payload)
            return


        # Logging events
	if event == "CW_WatchDogComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "CW_WatchDogComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        return

    # ------------------------- 
    # component logic methods
    # -------------------------

    def resetStat(self, payload):
        actualTime = time.time()
        # Check if message is consistent or it arrives from a previous instance of the component
        # Safe interval is -5 sec ... +60 sec
        if (actualTime > self.lastResetSended + (1*3600)-5)  and (actualTime < self.lastResetSended + (1*3600)+60):

            # Update total counter
            self.totArrivedCounter += self.arrivedCounter
            self.totSuccessCounter += self.successCounter
            self.totFailCounter += self.failCounter
            self.totResubmitCounter += self.resubmitCounter

            # Prepare logging message
            statMsg = "\n LAST h: Arrived "+ str(self.arrivedCounter)+ "|| Success " + str(self.successCounter)+ \
                      "|| Failed " +str(self.failCounter)
            if self.arrivedCounter != 0:
                self.averageResubmissionTask = self.resubmitCounter/float(self.arrivedCounter)
                statMsg += "|| Average resubmissions/Task "+str(self.averageResubmissionTask)

            statMsg += "\n \n TOTAL : Arrived "+ str(self.totArrivedCounter)+ "|| Success " + str(self.totSuccessCounter)+ \
                   "|| Failed " +str(self.totFailCounter)
            if self.totArrivedCounter != 0:
                self.totAverageResubmissionTask = self.totResubmitCounter/float(self.totArrivedCounter)
                statMsg += "|| Average resubmissions/Task "+str(self.totAverageResubmissionTask)

            statMsg += "\n"

            logging.info(statMsg)

            self.arrivedCounter = 0
            self.successCounter = 0
            self.failCounter = 0
            self.resubmitCounter = 0

            # Publish new reset message and save the time
            tmpMs =  MessageService()
            tmpMs.registerAs("CW_WatchDogStat")
            tmpMs.publish("CW_WatchDogComponent:resetStatistics",' ','01:00:00')
            tmpMs.commit()
            self.lastResetSended = actualTime
            del tmpMs
        return



    # ProxyTarballAssociatorComponent:CrabWork
    # Payload = ProxyAbsolutePath::TaskAbsolutePath::JobsToSubmit::RetryNumber
    def addEntry(self, payload):  

        # Extract Task Name and record arrival time
        taskWorkDir = payload.split('::')[1]
        taskName = str(taskWorkDir.split('/')[-1])
        aTime = time.time()
        range = payload.split('::')[2]
        logging.debug("Task arrived, it must be added to the queue: " + str(taskName))

        # Add the new task to the dictionary taskName:(ProxyTarPayload, time)
        self.files[taskName] = (payload, aTime)
        #self.files[(taskName,range)] = (payload, aTime) SWITCH WHEN SUBMISSION RANGE IS ACTIVEATED
        self.queueSize = len(self.files)
        logging.info("Task "+ str(taskName) +" arrived. Now materialize the status! ") 

        # Check queue size
        if self.queueSize > self.threshold:
            logging.info("Warning: Queue size is "+ str(self.queueSize) +". Too long for a safe running in case of failure.")

        self.materializeFiles()
        logging.info("\n")
        pass



    # CrabWorkPerformedPartial Payload          = TaskName::10::[10]
    # CrabWorkPerformed, CrabWorkFailed Payload = TaskName
    # FastKill                                  = TaskName
    # TaskKilled Payload                        = TaskName::JobsToKill
    def removeEntry(self, payload):
        taskName = str(payload.split('::')[0])
        #range = str(payload.split('::')[1])  SWITCH WHEN SUBMISSION RANGE IS ACTIVEATED
        logging.debug("Task "+ str(taskName) +" terminated.")

        # Recover arrival time from the dictionary
        if taskName in self.files:
        # if (taskName, range) in self.files:   SWITCH WHEN SUBMISSION RANGE IS ACTIVEATED
            aRecoveredTime = self.files[taskName][1]
            logging.debug("aRecoveredTime="+str(aRecoveredTime))

            # Update statistics
            totTime,nTask = self.timeStat
            self.timeStat = (totTime + time.time()- aRecoveredTime, nTask + 1)
            logging.debug("timeStat calculated")

            # Remove the terminated task
            logging.debug("Remove the terminated task")
            del self.files[taskName]
            #del self.files[(taskName,range)]   SWITCH WHEN SUBMISSION RANGE IS ACTIVEATED
            self.queueSize = len(self.files)
            logging.info("Task "+ str(taskName) +" terminated. Now materialize the status!")
 
            # If it's present remove also the fastKill command   HOW TO MENAGE PREKILL AND PARTIAL SUBMISSIONS?
            if taskName in self.fastKillList:
                del self.fastKillList[taskName]
                logging.info("FastKill message for task "+ str(taskName) +" removed. Now materialize the status!")

            self.materializeFiles()

        elif taskName in self.fastKillList:
            del self.fastKillList[taskName]
            logging.info("No task "+ str(taskName) +" in the queue but a FastKill message is present for it... Removed!")
            self.materializeFiles()

        else:
            logging.info("No object is present in the queue for task "+str(taskName))
        logging.info("\n")
        pass 

    def coldRestart(self):  # NDR NO PROBLEM WITH PARTIAL SUBMISSIONS
        try:
            if self.queueSize!=0:
                tmpMs =  MessageService()
                tmpMs.registerAs("CW_WatchDogForward")
                logging.info( str(len(self.fastKillList)) + " fastKill directives pending.")
                logging.info(self.fastKillList)
                logging.info( str(self.queueSize) + " object in the queue! Publish messages!" )
                logging.info(self.files)
               
                for pendingKill in self.fastKillList:
                    tmpMs.publish("WatchDogComponent:scheduledKill", str(self.fastKillList[pendingKill]))
                    tmpMs.commit()

                    # For consistent kill in case of CW crash during submission
                    tmpMs.publish('KillTask', self.fastKillList[pendingKill]) 
                    tmpMs.commit()

                for pendingTask in self.files:
                    tmpMs.publish("ProxyTarballAssociatorComponent:CrabWork", str(self.files[pendingTask][0]) )
                    tmpMs.commit()

                del tmpMs
                logging.info("Messages to CrabWorker Sent")
        except Exception, e:
            logging.info("Problem in WatchDog publishing.\n%s"%str(e))
        logging.info("\n")
        pass


    # prekill Payload = taskName:AbsoluteProxyPath:JobsToKill
    def forwardPreKill(self, payload):                #  HOW TO MENAGE PREKILL AND PARTIAL SUBMISSIONS?
        taskName = str(payload.split(':')[0])
        # taskName = str(payload.split('::')[0]) Activate when switch to new message standard
        tmpMs =  MessageService()
        tmpMs.registerAs("CW_WatchDogForward")
        if taskName in self.files and  taskName not in self.fastKillList: # no messages duplication
            tmpMs.publish("WatchDogComponent:scheduledKill", payload)
            logging.info("Kill scheduled for task %s"%taskName)

            # persistence of kill # FIX Fabio
            self.fastKillList[taskName] = payload
            self.materializeFiles()
            logging.info("FastKill in the queue: "+str(self.fastKillList)+'\n')
        else:
            tmpMs.publish("TaskKilledFailed", taskName+'::'+ str(payload.split(':')[2]))
            #tmpMs.publish("TaskKilledFailed", taskName+'::'+ str(payload.split('::')[2])) Activate when switch to new message standard
            logging.info("No task %s in the queue: TaskKillFailed message published"%taskName)
        tmpMs.commit()
        del tmpMs
        logging.info("\n")
        pass
    
    #
    # auxiliary methods
    # 
    def materializeFiles(self):
        try:
            dataDump = (self.files, self.fastKillList)

            # Materialize the component status to preserve it from crashes
            f = open(self.dropBoxPath+"/watchDog_tmp", 'w')
            pickle.dump(dataDump, f)
            f.close()
            os.rename(self.dropBoxPath+"/watchDog_tmp", self.dropBoxPath+"/watchDog.set")
            logging.debug("Status Materialized!") 
        except Exception, e:
            logging.info("Warning: Unable to materialize status.\n"+str(e))
            pass
        return

    def startComponent(self):
        """
        _startComponent_

        Start up the component
        """

        self.ms = MessageService()
        self.ms.registerAs("CW_WatchDogComponent")

        # subscribe to messages
        self.ms.subscribeTo("ProxyTarballAssociatorComponent:CrabWork")

        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformedPartial")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkFailed")
        self.ms.subscribeTo("CrabServerWorkerComponent:FastKill")

        self.ms.subscribeTo("CrabServerWorkerNotifyThread:Retry")

        self.ms.subscribeTo("CW_WatchDogComponent:synchronize")
        self.ms.subscribeTo("CommandManagerComponent:prekill")
        self.ms.subscribeTo("TaskKilled")

        self.ms.subscribeTo("CW_WatchDogComponent:resetStatistics")  
        self.ms.subscribeTo("CW_WatchDogComponent:StartDebug")
        self.ms.subscribeTo("CW_WatchDogComponent:EndDebug")

        # Inizialize counter for statistics
        self.counterForStat = 0
        try:
            Session.set_database(dbConfig)
            Session.connect('CWWD_session')
 
            while True :
                Session.start_transaction('CWWD_session')
                Session.set_session('CWWD_session')

                # Events listening and translation
                type, payload = self.ms.get()
                logging.debug("CW_WatchDogComponent: %s %s" % ( type, payload))
                self.ms.commit()
                self.__call__(type, payload)

                # Stamp statistics every 150 events
                self.counterForStat += 1

                if self.counterForStat >= 10:
                    self.counterForStat = 0
                    s = "Statistics: QueueSize "+ str(self.queueSize)  
                    if self.timeStat[1]!=0:
                        s += " AverageSubmissionTime " + str(self.timeStat[0]/self.timeStat[1])  
                    logging.info(s)
                # 
                Session.commit('CWWD_session')
                pass 
            Session.close('CWWD_session')
        except Exception, e:
            logging.info("Errors during main cycle: Try to debug!\n"+str(e))


