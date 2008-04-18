import sys
from threading import Thread
from random import Random
import time

class JabberThread(Thread):
    
    def __init__(self, summoner, log, throughput, ms):
        """
        @ summoner: class that invoked the Jabber and that maintains the go_on_accepting_load attribute
        @ log: logging system
        @ throughput: avrerage number of seconds per task that must be granted
        @ ms: messageService class for listening completed tasks
        """
        Thread.__init__(self)
        self.summoner = summoner
        self.logsys = log
        self.thr = throughput
        self.ms = ms

        self.start()
        pass

    def run(self):
        """
        JabberThread main loop: get task completion messages, sample time and evaluate the throughput.
        If the time exceeds too much the requirements (+10% to avoid fluctuations) then stop accepting
        new tasks.
        """
        tPre = time.time()
        
        while True:
            # get messages
            type, payload = self.ms.get()
            self.ms.commit()
            self.logsys.debug("JabberThread: %s %s" %(type, payload) )

            tPost = time.time()
            deltaT = tPost - tPre
            self.logsys.info("AvgThroughput: %f s (%d / day)"%(deltaT, int(0.5+86400/(deltaT+1)) ) )
            
            # jabber disabled
            if self.thr <= 0.0:
                continue

            # alter the guard on the proxy service
            if deltaT > 1.1 * self.thr:
                self.summoner.go_on_accepting_load = 0 #False
            else:
                self.summoner.go_on_accepting_load = 1 #True
        pass
        
    
