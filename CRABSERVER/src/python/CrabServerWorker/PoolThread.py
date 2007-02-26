#!/usr/bin/env python
"""
_PoolThreads_

Implements a pool of threads used to distribute Crab workload.

"""

__revision__ = "$Id$"
__version__ = "$Revision$"

import sys
from threading import Thread
import Queue

"""
Class: PoolThread
"""

class PoolThread:
    """
    An instance of this class implements a pool of threads that performs
    Crab work.
    """
    
    def __init__(self, numThreads, workerComponent):
        """
        initialize the pool by creating an input and an output queue,
        and a specified number of threads, that will perform the action
        performCrabWork on the workerComponent.
        """
        
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
                                            self.component))
            
    def insertRequest(self, request):
        """
        insert a request into the input queue of the pool.
        """
        self.requests.put(request)
        
    def getResult(self):
        """
        return the first result from the output queue of the pool.
        """

        return self.results.get()

"""
Class: CrabWorker
"""    

class CrabWorker(Thread):
    """
    An instance of this class implements a thread that performs
    crab work.
    """
    
    def __init__(self, inQueue, outQueue, component):
        """
        initialize input and output queues, register the component
        and start thread work.
        """
        
        Thread.__init__(self)
        self.inQueue = inQueue
        self.outQueue = outQueue
        self.component = component
        self.setDaemon(1)
        self.start()
        
    def run(self):
        """
        main body of the thread: get a request from the input queue,
        process it and store its return value in the output queue.
        """
        
        # do forever
        while True:
            
            # get request
            payload = self.inQueue.get()

            # execute CRAB work
            try:
                returnData = str(self.component.performCrabWork(payload))
                
            # generate error message when processing fails
            except:
                returnData = "Exception when processing payload: " + \
                                payload
                
            # store return code in output queue
            self.outQueue.put(returnData)
            
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
        self.setDaemon(1)
        self.start()
        
    def run(self):
        """
        main body of the thread. Wait for a result to arrive and send the
        corresponding message.
        """
        
        # do forever
        while True:
            
            # get result
            result = self.pool.getResult()
            
            # send the message
            self.ms.publish("CrabServerWorkerComponent:CrabWorkPerformed", result)
            self.ms.commit()
            
            # logging info
            self.logging.info("CrabWorkPerformed: "+ result)
            
            
