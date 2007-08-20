#!/usr/bin/env python
"""
_PoolThreads_

Implements a pool of threads used to distribute Crab workload.

"""

__revision__ = "$Id: PoolThread.py,v 1.9 2007/07/17 18:05:39 farinafa Exp $"
__version__ = "$Revision: 1.9 $"

import sys
from threading import Thread
import Queue
from random import Random
import time

from TaskTracking.TaskStateAPI import *

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
        # To prevent zombies -- self.setDaemon(1)
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
        self.r = Random()
        ## self.setDaemon(1)
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
                # send the message
                if int(code) == 0: 
                    # Missed fast kill
                    if str(result) in self.pool.component.killSet.keys():
                         killMsg = str(self.pool.component.killSet[result])
                         self.pool.component.killSet[result] = -1
                         self.logging.info("Delayed fast kill for task %s. The task will be killed by JobKiller."%result)
                         self.ms.publish("KillTask", killMsg)
                         self.ms.commit()

                    # Real successful submissions
                    self.logging.info("CrabWorkPerformed: "+ str(result))
                    self.ms.publish("CrabServerWorkerComponent:CrabWorkPerformed", str(result))
                    self.ms.commit()
                elif int(code) == -2:
                    self.logging.info("CrabWorkFailed: "+ str(result))
                    self.ms.publish("CrabServerWorkerComponent:CrabWorkFailed", str(result))
                    self.ms.commit()
                elif int(code) == -1:
                    self.logging.info("CrabWorkRetry: "+ str(result))
                    twait = "00:%d%d:00"%(self.r.randint(0, 0), self.r.randint(0, 9))
                    self.logging.info("Short Delay time (modify for production code):"+str(twait)) 
                    countDest = 0
                    countDest += self.ms.publish("CrabServerWorkerNotifyThread:Retry", str(result), twait) 
                    self.logging.info("Retry listeners count:" + str(countDest))
                    self.ms.commit()
                elif int(code) == -3:
                    tName = result.split('::')[0] 
                    if str(tName) in self.pool.component.killSet.keys():
                         killMsg = str(self.pool.component.killSet[tName])
                         self.pool.component.killSet[tName] = -1
                         self.logging.info("Delayed fast kill for task %s. The task will be killed by JobKiller."%tName)
                         self.ms.publish("KillTask", killMsg)
                         self.ms.commit()
                         continue
                    self.logging.info("CrabWorkPerformed with partial submission: "+ str(result))
                    self.ms.publish("CrabServerWorkerComponent:CrabWorkPerformedPartial", str(result))
                    self.ms.commit()
                elif int(code) == -4:
                    self.ms.publish("CrabServerWorkerComponent:FastKill", str(result))
                    self.ms.commit()
                    self.logging.info("Fast kill for task %s. This task wont be submitted."%str(result))
                else:
                    self.logging.info("WARNING: Unexpected result from worker pool.")
            except Exception, e:
                 self.logging.info("Notify Thread problem: "+str(e))
        pass           

