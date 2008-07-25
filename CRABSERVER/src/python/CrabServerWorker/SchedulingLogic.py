
import traceback 
import time
from Queue import Queue, Empty

scheduleRequests = Queue()
descheduleRequests = Queue()

class SchedulingLogic:
    
    def __init__(self, nWorkers, logger, queue=None, sleepTime=12.0):
        self.log = logger 
        self.sleepTime = sleepTime

        self.scheduleReq = scheduleRequests
        self.descheduleReq = descheduleRequests
        self.messageQueue = queue
        
        # queue item structure
        self.schedMapSubmissions = {}
        self.schedMapResubmissions = {}
        
        # scheduling thread task independent infos 
        self.nWorkers = nWorkers
        return
    
    def applySchedulingLogic(self):
        # collect scheduling directives
        req = 1
        while req is not None:
            try:
                req = scheduleRequests.get_nowait()
            except Empty, e:
                req = None
            self._insertRequestInTables(req)

        # collect de-schedule requests
        req = 1
        while req is not None:
            try:
                req = scheduleRequests.get_nowait()
            except Empty, e:
                req = None
            self._removeRequestFromTables(req)

        # perform scheduling strategies
        self._scheduleSubmissions()
        self._scheduleResubmissions()
        return

# ------------------------------------
# Queue structure management methods
# ------------------------------------

    def _insertRequestInTables(self, itemToInsert=None):
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
        
        if event in ['TaskRegisterComponent:NewTaskRegistered', 'CRAB_Cmd_Mgr:NewCommand']:
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

    def _removeRequestFromTables(self, entryToRemove=None):
        if entryToRemove is None:
            return
 
        if entryToRemove in self.schedMapSubmissions: 
            del self.schedMapSubmissions[entryToRemove]
        
        if entryToRemove in self.schedMapResubmissions: 
            del self.schedMapResubmissions[entryToRemove]
        return

# ------------------------------------
# Scheduling strategies
# ------------------------------------

    def _scheduleSubmissions(self):
        # let's start with an easy FIFO
        # sort the map values according the chosen criteria
        #     e.g. of the sorted + lambda usage
        #     sorted(l, lambda x,y: cmp(y['c'],x['c']) or cmp(y['b'],x['b']) )

        schedList = []
        schedList += sorted(self.schedMapSubmissions.values(), lambda x,y: cmp(x['deadline'], y['deadline']) )
        # SJF/Min-Min: lambda x,y:  cmp(x['deadline'], y['deadline']) or cmp(len(x['rng']), len(y['rng'])) ) 
        
        if len(schedList)>0:
            self.log.debug("Scheduling order (submit): %s"%str(schedList)) 
            for s in schedList:
                payload = s['taskName'] +'::'+ str(s['retryCounter']) +'::'+ str(s['rng'])  
                del self.schedMapSubmissions[ s['taskName'] ] 
                self.messageQueue.put( ('schedThr', "CrabServerWorkerComponent:Submission", payload) ) 
        return
    
    def _scheduleResubmissions(self):
        # collect the requests for task and use a deadline criterions as release strategy
        curT = time.time()
        schedList = [] + self.schedMapResubmissions.values()

        # filter on deadline expiration
        schedList = [i for i in schedList if i['deadline'] > curT]
        
        if len(schedList)>0:
            self.log.debug("Scheduling order (resubmit): %s"%str(schedList)) 
            for s in schedList:
                payload = s['taskName'] +'::'+ str(s['retryCounter']) +'::'+ str(s['rng'])
                del self.schedMapResubmissions[ s['taskName'] ] 
                self.messageQueue.put( ('schedThr', "CrabServerWorkerComponent:Submission", payload) ) 
        return
    
    

