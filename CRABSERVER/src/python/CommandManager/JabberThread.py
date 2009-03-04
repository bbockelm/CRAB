import sys
from threading import Thread
from random import Random
#import time

# Message service import
from MessageService.MessageService import MessageService


class JabberThread(Thread):
    
    def __init__(self, summoner, log, throughput):
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
        self.go_on_accepting_load = 1

        self.start()
        pass

    def run(self):
        """
        JabberThread main loop: get task completion messages, sample time and evaluate the throughput.
        If the time exceeds too much the requirements (+10% to avoid fluctuations) then stop accepting
        new tasks.
        """
        self.logsys.info("Starting JabberThread")

        self.ms = MessageService()
        self.ms.registerAs("CRAB_CmdMgr_jabber")

        # messages implying meaningful network load
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewTask")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewCommand")

        import time
        tPre = time.time()
        if int(self.thr) == 0:
            self.go_on_accepting_load = 2
            self.logsys.info("Stopping accepting load")

        count = 0
        while True:
            # get messages
            type, payload = None, None
            try:
                type, payload = self.ms.get()
                self.ms.commit()
            except Exception, exc:
                self.logsys.error("ERROR: problem interacting with the message service")
                self.logsys.error(str(exc))
                time.sleep(2)
                continue

            self.logsys.debug("JabberThread: %s %s" %(type, payload) )

            tPost = time.time()
            deltaT = tPost - tPre

            if count%2000 == 0:             
                self.logsys.info("AvgThroughput: %f s (%d connections / day)"%(deltaT, int(0.5+86400/(deltaT+1)) ) )
                count = 0
            count += 1 
            
            # jabber disabled
            if self.thr < 0.0:
                continue

            # alter the guard on the proxy service
            if  int(self.thr) != 0:
                if deltaT > 1.1 * self.thr:
                    self.go_on_accepting_load = 0 #False
                    self.logsys.info("Stopping accepting load")  
                else:
                    self.go_on_accepting_load = 1 #True
        pass
        
    
