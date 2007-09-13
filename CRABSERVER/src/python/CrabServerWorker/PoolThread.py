#!/usr/bin/env python
"""
_PoolThreads_

Implements a pool of threads used to distribute Crab workload.

"""

__revision__ = "$Id: PoolThread.py,v 1.10 2007/09/07 09:47:31 farinafa Exp $"
__version__ = "$Revision: 1.10 $"

import sys
from threading import Thread
import Queue
from random import Random
import time

## from TaskTracking.TaskStateAPI import *

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

"""
Class: PoolThread
"""

class PoolThread:
    """
    An instance of this class implements a pool of threads that performs
    Crab work.
    """
    
    def __init__(self, numThreads, workerComponent, log):
        """
        initialize the pool by creating an input and an output queue,
        and a specified number of threads, that will perform the action
        performCrabWork on the workerComponent.
        """

        self.logging = log

        # create queues
        self.requests = Queue.Queue()
        self.results = Queue.Queue()
        
        # store reference to component that uses the pool
        self.component = workerComponent
        
        # at least one thread...
        if numThreads < 1:
            numThreads = 1
            
        # create threads
        self.threads = []        
        self.createThreads(numThreads)

    def createThreads(self, numThreads):
        """
        create numThreads threads.
        """
            
        for i in range(numThreads):
            self.threads.append(CrabWorker(self.requests,
                                            self.results,
                                            self.component, self.logging))
            
    def insertRequest(self, request):
        """
        insert a request into the input queue of the pool.
        """
        self.requests.put(request)
	pass
        
    def getResult(self):
        """
        return the first result from the output queue of the pool.
        """
        value = self.results.get()
        return value

"""
Class: CrabWorker
"""    

class CrabWorker(Thread):
    """
    An instance of this class implements a thread that performs
    crab work.
    """
    
    def __init__(self, inQueue, outQueue, component, log):
        """
        initialize input and output queues, register the component
        and start thread work.
        """
        
        Thread.__init__(self)
        self.inQueue = inQueue
        self.outQueue = outQueue
        self.component = component
        self.logging = log
        self.start()
        
    def run(self):
        """
        main body of the thread: get a request from the input queue,
        process it and store its return value in the output queue.
        """
        # do forever
        while True:
            # get request
            payload, retry = self.inQueue.get()
	    
            # execute CRAB work
            try:
                returnData, retCode = self.component.performCrabWork(payload, retry)
                # store return code in output queue
                self.outQueue.put( (returnData, retCode) )
            except:
                logging.info("Exception when processing payload: " + str(payload))
                logging.info(e)
            
"""
Class: Notifier
"""
        
class Notifier(Thread):
    """
    An instance of this class obtains the results from the output queue
    of the thread pool and notifies through the message service about the
    exit condition of the Crab work performed
    """
    
    def __init__(self, pool, ms, logging):
        """
        initialize the pool instance and start the thread, which will
        send messages by using the message service instance provided.
        """
        Thread.__init__(self)
        self.pool = pool
        self.ms = ms
        self.logging = logging
        self.start()
        
    def run(self):
        """
        main body of the thread. Wait for a result to arrive and send the
        corresponding message.
        """
        # do forever
        while True:
            try:    
                # get result
                result, code = self.pool.getResult()

                # parse the result and prepare the messages
                msgList = self.parseResult(result, code)
                if len(msgList) == 0:
                     continue

                # publish results using one-shot sessions
                # Session.set_database(dbConfig)
                # Session.connect()
                # Session.start_transaction()

                for topic, msg, wtime in msgList:
                     self.ms.publish(topic, msg, wtime)
                     self.ms.commit()

                # Session.commit_all()
                # Session.close_all()
                
            except Exception, e:
                self.logging.info("Notify Thread problem: "+str(e))
            pass

    def parseResult(self, result, code):
        retList = []

        if int(code) == 0: 
             if str(result) in self.pool.component.killSet.keys():
                  self.logging.info("Delayed fast kill for task %s. The task will be killed by JobKiller."%result)
                  #
                  killMsg = str(self.pool.component.killSet[result])
                  self.pool.component.killSet[result] = -1
                  #
                  retList.append( ("KillTask", killMsg, "00:00:00") )

             self.logging.info("CrabWorkPerformed: "+ str(result))
             retList.append( ("CrabServerWorkerComponent:CrabWorkPerformed", result, "00:00:00") )
             return retList

        elif int(code) == -2:
                    self.logging.info("CrabWorkFailed: "+ str(result))
                    retList.append( ("CrabServerWorkerComponent:CrabWorkFailed", result, "00:00:00") )
                    return retList

        elif int(code) == -1:
                    r = Random()
                    self.logging.info("CrabWorkRetry: "+ str(result))
                    twait = "00:%d%d:00"%(r.randint(0, 0), r.randint(0, 9))
                    #
                    self.logging.info("Short Delay time (modify for production code):"+str(twait)) 
                    retList.append( ("CrabServerWorkerNotifyThread:Retry", result, twait) )
                    return retList

        elif int(code) == -3:
                    tName = result.split('::')[0] 
                    if str(tName) in self.pool.component.killSet.keys():
                         killMsg = str(self.pool.component.killSet[tName])
                         self.pool.component.killSet[tName] = -1
                         #
                         self.logging.info("Delayed fast kill for task %s. The task will be killed by JobKiller."%tName)
                         retList.append( ("KillTask", killMsg, "00:00:00") )
                         return retList

                    self.logging.info("CrabWorkPerformed with partial submission: "+ str(result))
                    retList.append( ("CrabServerWorkerComponent:CrabWorkPerformedPartial", result, "00:00:00") )
                    return retList

        elif int(code) == -4:
                    self.logging.info("Fast kill for task %s. This task wont be submitted."%str(result))
                    retList.append( ("CrabServerWorkerComponent:FastKill", result, "00:00:00") )
                    return retList

        else:
                    self.logging.info("WARNING: Unexpected result from worker pool.")
        #
        return retList

