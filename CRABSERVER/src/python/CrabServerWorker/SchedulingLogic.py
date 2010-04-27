
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

        if len(siteToBan) > 0 :
            qItem['bannedSites'] = list(set( qItem['bannedSites'] + siteToBan.split(',') ))

        if event in ['CrabJobCreatorComponent:NewTaskRegistered','TaskRegisterComponent:NewTaskRegistered', 'CRAB_Cmd_Mgr:NewCommand']:
            if taskName in self.schedMapSubmissions:
                qItem.update( self.schedMapSubmissions[taskName] )

            # fill the queueItem attributes
            qItem['rng'] = list(set( cmdRngList ))
            if qItem['retryCounter'] is None or \
                (event=='CRAB_Cmd_Mgr:NewCommand' and int(retryCounter)>int(qItem['retryCounter']) ):
                    # set if there is no counter or be polite in case of subsequent submissions 
                    qItem['retryCounter'] = retryCounter
                    
            self.schedMapSubmissions[taskName] = qItem

        elif event == 'ResubmitJob':
            if taskName in self.schedMapResubmissions:
                qItem.update( self.schedMapResubmissions[taskName] )

            # fill attributes           
            qItem['rng'] = list(set( qItem['rng'] + cmdRngList )) 
            qItem['retryCounter'] = retryCounter
            qItem['deadline'] = time.time() + self.sleepTime
            
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
        #     sorted(l, lambda x,y: cmp(y['c'], x['c']) or cmp(y['b'], x['b']) )
        schedList = []
        schedList += sorted(self.schedMapSubmissions.values(), lambda x,y: cmp(x['deadline'], y['deadline']) )

        # SJF/Min-Min: lambda x,y:  cmp(x['deadline'], y['deadline']) or cmp(len(x['rng']), len(y['rng'])) ) 
        if len(schedList) > 0:
            self.log.debug("Scheduling order (submit): %s"%str(schedList))
            submissionSource = 'user'
 
            for s in schedList:
                payload = s['taskName'] +'::'+ str(s['retryCounter']) +'::'+ str(s['rng'])+'::'+submissionSource 
                if len(s['bannedSites']) > 0:
                    payload += '::'+ str(s['bannedSites'])
 
                del self.schedMapSubmissions[ s['taskName'] ] 
                self.messageQueue.put( ('schedThr', "CrabServerWorkerComponent:Submission", payload) ) 
        return
    
    def _scheduleResubmissions(self):
        # collect the requests for task and use a deadline criterions as release strategy
        curT = time.time()
        schedList = [] + self.schedMapResubmissions.values()

        # filter on deadline expiration
        schedList = [i for i in schedList if curT > i['deadline'] ]
        if len(schedList)>0:
            
            # TODO: turn this to debug once verified everything works correctly # Fabio
            self.log.info("Scheduling order (resubmit): %s"%str(schedList)) 
            submissionSource = 'auto'
 
            for s in schedList:
                payload = s['taskName'] +'::'+ str(s['retryCounter']) +'::'+ str(s['rng'])+'::'+submissionSource
                del self.schedMapResubmissions[ s['taskName'] ] 
                self.messageQueue.put( ('schedThr', "CrabServerWorkerComponent:Submission", payload) ) 
        return
    
    

