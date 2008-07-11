
import traceback, copy, re
import string, sys, os, time
from xml.dom import minidom
from threading import Thread
from Queue import Queue, Empty
from MessageService.MessageService import MessageService

scheduleRequests = Queue()
descheduleRequests = Queue()

class SchedulingWorker(Thread):
    
    def __init__(self, nWorkers, logger):
        Thread.__init__(self)
        self.log = logger 

        self.ms = MessageService()
        self.ms.registerAs('CSW_SchedulingWorker')

        self.scheduleReq = scheduleRequests
        self.descheduleReq = descheduleRequests
        
        # queue item structure
        self.schedMapSubmissions = {}
        self.schedMapResubmissions = {}
        
        # scheduling thread task independent infos 
        self.nWorkers = nWorkers
        self.sleepTime = 12.0
                
        self.start()
        return
    
    def run(self):
        self.log.info("SchedulingWorker Started. Sleep time:%s"%self.sleepTime)
        while True:
            # collect scheduling directives
            req = 1
            while req is not None:
                try:
                    req = scheduleRequests.get_nowait()
                except Empty, e:
                    req = None
                self.insertRequestInTables(req)
       
            # collect de-schedule requests
            req = 1
            while req is not None:
                try:
                    req = scheduleRequests.get_nowait()
                except Empty, e:
                    req = None
                self.removeRequestFromTables(req)
            
            # perform scheduling strategies
            self.scheduleSubmissions()
            self.scheduleResubmissions()

            # loop control
            time.sleep(self.sleepTime)
        pass

# Queue structure management methods
    def insertRequestInTables(self, itemToInsert=None):
        # incoming data structure (event, taskName, cmdRng, retryCounter, siteToBan)
        # representation structure in queues
        #     q_item == taskName : {rng:[ integers ], bannedSites: [strings], retryCounter, \
        #                deadline : timestamp, priority : 0, ...others (e.g. prioritize users) ...}  

        if itemToInsert is None:
            return
  
        event, taskName, cmdRng, retryCounter, siteToBan = itemToInsert
        cmdRngList = eval(cmdRng, {}, {})
        
        qItem = {'taskName':taskName, 'rng':[], 'bannedSites':[], \
                 'retryCounter':None, 'deadline': time.time(), 'priority':0 }
        # taskName is redundant but its useful, instead of using schedMap*.items() tuples
        
        if event in ['CrabServerWorkerComponent:NewTaskRegistered', 'CRAB_Cmd_Mgr:NewCommand']:
            if taskName in self.schedMapSubmissions:
                qItem.update( self.schedMapSubmissions[taskName] )
            # fill the queueItem attributes
            qItem['rng'] = list(set( qItem['rng'] + cmdRngList ))
            qItem['bannedSites'] = list(set( qItem['bannedSites'] + siteToBan.split(',') ))
            if qItem['retryCounter'] is None or \
                (event=='CRAB_Cmd_Mgr:NewCommand' and int(retryCounter)>int(qItem['retryCounter']) ):
                    # set if there is no counter or be polite in case of subsequent submissions 
                    qItem['retryCounter'] = retryCounter
                    
            self.schedMapSubmissions[taskName] = qItem
        elif event == 'ResubmitJob':
            if taskName in self.schedMapResubmissions:
                qItem.update( self.schedMapResubmissions[taskName] )
            
            qItem['rng'] = list(set( qItem['rng'] + cmdRngList ))
            qItem['bannedSites'] = list(set( qItem['bannedSites'] + siteToBan.split(',') ))
            qItem['retryCounter'] = retryCounter
            qItem['deadline'] = time.time() + 3.0 * self.sleepTime
            
            self.schedMapResubmissions[taskName] = qItem        
        else:
            self.log.info("Unknown scheduling request will be ignored for %s"%taskName)
            self.log.debug(itemToInsert)
        return

    def removeRequestFromTables(self, entryToRemove=None):
        if entryToRemove is None:
            return
 
        if entryToRemove in self.schedMapSubmissions: 
            del self.schedMapSubmissions[entryToRemove]
        
        if entryToRemove in self.schedMapResubmissions: 
            del self.schedMapResubmissions[entryToRemove]
        return

# Scheduling strategies
    def scheduleSubmissions(self):
        # let's start with an easy FIFO
        # sort the map values according the chosen criteria
        #     e.g. of the sorted + lambda usage
        #     sorted(l, lambda x,y: cmp(y['c'],x['c']) or cmp(y['b'],x['b']) )
        schedList = []
        schedList += sorted(self.schedMapSubmissions.values(), \
                           lambda x,y: cmp(x['deadline'], y['deadline']) )
        # SJF/Min-Min: lambda x,y:  cmp(x['deadline'], y['deadline']) or cmp(len(x['rng']), len(y['rng'])) ) 
        
        # release messages for submission
        # "CrabServerWorkerComponent:Submission", payload = taskUniqName, resubCount, cmdRng
        if len(schedList)>0:
            self.log.debug("Scheduling order (submit): %s"%str(schedList)) 
            for s in schedList:
                payload = s['taskName'] +'::'+ str(s['retryCounter']) +'::'+ str(s['rng'])  
                self.ms.publish("CrabServerWorkerComponent:Submission", payload)
                del self.schedMapSubmissions[ s['taskName'] ]  
            self.ms.commit()
        return
    
    def scheduleResubmissions(self):
        # collect the requests for task and use a deadline criterions as release strategy
        curT = time.time()
        schedList = [] + self.schedMapResubmissions.values()
        # filter on deadline expiration
        schedList = [i for i in schedList if i['deadline'] > curT]
        
        if len(schedList)>0:
            self.log.debug("Scheduling order (resubmit): %s"%str(schedList)) 
            for s in schedList:
                payload = s['taskName'] +'::'+ str(s['retryCounter']) +'::'+ str(s['rng'])
                self.ms.publish("CrabServerWorkerComponent:Submission", payload)
                del self.schedMapResubmissions[ s['taskName'] ]  
            self.ms.commit()
        return
    
    

