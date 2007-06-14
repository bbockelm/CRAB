#!/usr/bin/env python
"""
_CW_WatchDogComponent.py_

"""

__version__ = "$Revision: 1.0 $"
__revision__ = "$Id: CW_WatchDogComponent.py,v 1.0 2007/05/28 15:14:37 mMerlo Exp $"

import os
import socket
import cPickle as pickle
import logging
import time
from logging.handlers import RotatingFileHandler
import commands
from MessageService.MessageService import MessageService

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
        #
        #  Log Handler is a rotating file that rolls over when the
        #  file hits 1MB size, 3 most recent files are kept
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        #
        #  Set up formatting for the logger and set the
        #  logging level to info level
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.DEBUG)

        logging.info("CW_WatchDog Component Started...")

        # Custom attributes
        self.files = {}
        self.dropBoxPath = str(self.args['dropBoxPath'])
        self.queueSize = 0
        self.threshold = 15
        self.timeStat = (0,0)

        #Dematerialize the queue
        try:
            f = open(self.dropBoxPath+"/watchDog.set",'r')
            self.files = pickle.load(f)
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

        # ProxyTarballAssociatorComponent:CrabWork
        #Payload = /tmp/del_proxies//515800dc531f5476-7e04591647358283:
        #/flatfiles/cms//crab_crab_0_070523_091939_ae1fb440-072a-48a7-ae27-628eb1841fc9:
        #/flatfiles/cms//crab_crab_0_070523_091939_ae1fb440-072a-48a7-ae27-628eb1841fc9:
        #5 #
        if event == "ProxyTarballAssociatorComponent:CrabWork":
             # Extract Task Name and record arrival time
             taskWorkDir = payload.split(':')[1]
             taskName = str(taskWorkDir.split('/')[-1])
             aTime = time.time()
             logging.info("Task arrived, it must be added to the queue: " + str(taskName))

             # Add the new task to the dictionary taskName:(ProxyTarPayload, time)
             self.files[taskName] = (payload, aTime)
             self.queueSize = len(self.files)
             logging.info("Task "+ str(taskName) +" added with arrival time " + str(aTime) + ". Now materialize the status!") # switch to debug #Fabio

             # Increment queue size counter
             if self.queueSize > self.threshold:
                  logging.info("Warning: Queue size is "+ str(self.queueSize) +". Too long for a safe running in case of failure.")

             self.materializeFiles()

             return

        if event in ["CrabServerWorkerComponent:CrabWorkPerformed", "CrabServerWorkerComponent:CrabWorkFailed", 
                     "CrabServerWorkerComponent:CrabWorkPerformedPartial"]:
             logging.info("Task terminated, it must be removed from the queue: " + str(payload))
             taskName = str(payload.split(':')[0])
             logging.debug("Task name: " + str(taskName))             

             # Recover arrival time from the dictionary
             if taskName in self.files:
                  aRecoveredTime = self.files[taskName][1]
                  logging.debug("aRecoveredTime="+str(aRecoveredTime))

                  # Update statistics
                  totTime,nTask = self.timeStat
                  self.timeStat = (totTime + time.time()- aRecoveredTime, nTask + 1)
                  logging.debug("timeStat calculated")

                  # Remove the terminated task
                  logging.debug("Remove the terminated task")
                  del self.files[taskName]
                  self.queueSize = len(self.files)
                  logging.info("Task "+ str(taskName) +" removed from the queue. Now materialize the status!") # switch to debug #Fabio
                  self.materializeFiles()
             else:
                  logging.info("Task "+str(taskName)+" is not present in the queue.")
             
             return

        # send the stored data to CrabWorker
        if event == "CW_WatchDogComponent:synchronize":
             try:
                  if self.queueSize!=0:
                       tmpMs =  MessageService()
                       tmpMs.registerAs("CW_WatchDogForward")

                       logging.info( str(self.queueSize) + " object in the queue! Publish messages!" )
                       logging.info(self.files)
                       for pendingTask in self.files:
                            tmpMs.publish("ProxyTarballAssociatorComponent:CrabWork", str(self.files[pendingTask][0]) )
                            tmpMs.commit()

                       del tmpMs
                       logging.info("Messages to CrabWorker Sent")    

             except Exception, e:
                  logging.info("Problem in WatchDog publishing.\n%s"%str(e))
             return

        # Logging events
	if event == "CW_WatchDogComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "CW_WatchDogComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        return
   
    def materializeFiles(self):
        try:
             # Materialize the component status to preserve it from crashes
             f = open(self.dropBoxPath+"/watchDog_tmp", 'w')
             pickle.dump(self.files, f)
             f.close()
             os.rename(self.dropBoxPath+"/watchDog_tmp", self.dropBoxPath+"/watchDog.set")
             logging.info("Status Materialized!") # switch to debug
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

        self.ms.subscribeTo("CW_WatchDogComponent:synchronize")

        self.ms.subscribeTo("CW_WatchDogComponent:StartDebug")
        self.ms.subscribeTo("CW_WatchDogComponent:EndDebug")

        # Inizialize counter for statistics
        self.counterForStat = 0
        try:
             while True :
                 # Events listening and translation
                 type, payload = self.ms.get()
                 logging.debug("CW_WatchDogComponent: %s %s" % ( type, payload))
                 self.ms.commit()
                 self.__call__(type, payload)

                 # Stamp statistics every 150 events
                 self.counterForStat += 1

                 if self.counterForStat >= 10:
                      self.counterForStat = 0
                      s = "Statistics:QueueSize "+ str(self.queueSize) +" ManagedTask "+ str(self.timeStat[1]) 
                      if self.timeStat[1]!=0:
                           s += " AverageSubmissionTime " + str(self.timeStat[0]/self.timeStat[1])  
                      logging.info(s)
             pass
        except Exception, e:
             logging.info("Errors during main cycle: Try to debug!\n"+str(e))
