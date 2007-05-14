#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.4 $"
__revision__ = "$Id: CrabServerWorkerComponent.py,v 1.4 2007/05/11 15:06:17 spiga Exp $"

import os
import socket
import pickle
import logging
import time
import popen2
import random
from logging.handlers import RotatingFileHandler
from threading import Thread
import commands

from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService
from CrabServerWorker.PoolThread import PoolThread, Notifier

# Imported to allow BOSS Declaration (method registerJob) # Fabio
from JobState.JobStateAPI import JobStateChangeAPI
from ProdCommon.MCPayloads.JobSpec import JobSpec
#WB: NEEDED FOR RESUBMISSION WITH CVS JobSubmitterComponent

class CrabServerWorkerComponent:
    """
    _CrabServerWorkerComponent_

    """
    def __init__(self, **args):
        self.args = {}
        
        self.args['Logfile'] = None
        self.args['dropBoxPath'] = None
        self.args['bossClads'] = None
        self.args['maxThreads'] = 5
        self.args['debugLevel'] = 0
        self.args.update(args)
           
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        #  //
        # // Log Handler is a rotating file that rolls over when the
        #//  file hits 1MB size, 3 most recent files are kept
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        #  //
        # // Set up formatting for the logger and set the 
        #//  logging level to info level
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        logging.info("CrabServerWorkerComponent Started...")

        self.dropBoxPath = str(args['dropBoxPath'])
        self.bossClads = str(self.args['bossClads'])

        if int(self.args['debugLevel']) == 0:
             self.debug = ''
        else:
             self.debug = ' -debug ' + str(self.args['debugLevel'])

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Event: %s %s" % (event, payload))

        if event == "ProxyTarballAssociatorComponent:CrabWork":
            logging.info("ProxyTarballAssociatorComponent:CrabWork Arrived!Payload = "+str(payload))
            self.pool.insertRequest( (payload, False) )

            #Matteo add: Message to TaskTracking for New Task in Queue 
            taskWorkDir = payload.split(':')[1]
            taskName = str(taskWorkDir.split('/')[-1])
            #proxyName = payload.split(':')[0]
            #ttMsg = str(taskName+':'+proxyName)
            self.ms.publish("CrabServerWorkerComponent:TaskArrival", taskName)
            self.ms.commit()
            #logging.info("Message TaskArrival sent to TaskTracking for task "+ taskName)

            return
        if event == "CrabServerWorkerNotifyThread:Retry":
            logging.info("Resubmission :"+ str(payload))
            self.pool.insertRequest( (payload, True) )
            return
        if event == "CrabServerWorkerComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "CrabServerWorkerComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        return 
        
    def startComponent(self):
        """
        _startComponent_

        Start up the component
        """
        # create message service instances
        self.ms = MessageService()
        self.msNotify = MessageService()
                                                                                
        # register
        self.ms.registerAs("CrabServerWorkerComponent")
        self.msNotify.registerAs("CrabServerWorkerNotifyThread")
        
        # subscribe to messages
        self.ms.subscribeTo("ProxyTarballAssociatorComponent:CrabWork")
        self.ms.subscribeTo("CrabServerWorkerNotifyThread:Retry")
        self.ms.subscribeTo("CrabServerWorkerComponent:StartDebug")
        self.ms.subscribeTo("CrabServerWorkerComponent:EndDebug")
        
        # create a pool of threads that will execute a performCrabWork action
        self.pool = PoolThread(int(self.args['maxThreads']), self, logging)
        # create notifier thread
        notifier = Notifier(self.pool, self.msNotify, logging)

        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CrabServerWorkerComponent: %s %s" % ( type, payload))
            self.__call__(type, payload)

    def performCrabWork(self, payload, performRetry = False):
       proxy = payload.split(':')[0]
       CrabworkDir =  payload.split(':')[1]
       uniqDir = payload.split(':')[2]
       retry = int(payload.split(':')[3])
       retMsg = CrabworkDir.split('/')[-1]
       
       # Prepare the project and submit
       os.chdir(self.dropBoxPath)
       try:
            if performRetry == False:
            	self.registerTask(CrabworkDir)
                logging.info("Registered Task")
            else:
                logging.info("Resubmission for "+str(CrabworkDir) )
       except Exception, e:
            logging.info("Error during BOSS Registration: "+ str(e) )

       # Submit the task
       logging.info("Self: " + str(self) + "Put CRAB at work on "+ CrabworkDir + " with proxy " + proxy + " PerformRETRY " + str(performRetry))
       retCode = self.crab_submit(proxy, uniqDir, performRetry)

       # Prepare the proper payload to be managed by the notifier queue
       if retCode == 0:
            retMsg = CrabworkDir.split('/')[-1]
       elif retCode!=0 and retry==0:
            retCode = -2
            retMsg = CrabworkDir.split('/')[-1]
            logging.info("WARNING: CRAB submission failed too many times\n The task will not be submitted.")
       elif retCode!=0 and retry>0:
            retCode = -1
            retMsg = proxy+":"+CrabworkDir+":"+uniqDir+":"+str(retry-1)
            logging.info("Task "+uniqDir+ " will be tried " + str(retry-1) + " more times.")
     
       return (retMsg, retCode)

    def registerTask(self, cwDir):
        jNameBase = cwDir.split('/')[-1]
        cmd = "cat "+ cwDir+"/share/cmssw.xml | grep ruleElement"
        fout, fin = popen2.popen4(cmd)
        ruleList = fout.readlines()
        fout.close()
        fin.close()

        jobNumber = int(ruleList[0].split(' ')[1].split(':')[1])

        # register the whole task
        for jid in xrange(1, jobNumber+1):
            jobName = jNameBase + "_" + str(jid)
            JobStateChangeAPI.register(jobName, 'Processing',4 ,1)
            cacheArea = cwDir+"/res"
            cacheArea += "/job%s"%jid
            try:
                os.mkdir(cacheArea)
                # WB: NEEDED FOR RESUBMISSION WITH CVS JobSubmitterComponent
                fakeJobSpec=JobSpec()
                fakeJobSpec.save("%s/%s-JobSpec.xml"%(cacheArea,jobName))
                idfile=open("%s/%sid"%(cacheArea,jobName),'w')

                idfile.write("JobId=%s"%jid)
                idfile.close()
                # logging.info("cacheArea %s"%cacheArea)
                del fakeJobSpec
            except Exception, e:
                logging.info("BOSS Registration problem %s"%cacheArea + str(e))

            JobStateChangeAPI.create(jobName, cacheArea)
            JobStateChangeAPI.inProgress(jobName)
            JobStateChangeAPI.submit(jobName)
        pass

    def crab_submit(self, proxy, uniqDir, retry=False):
       # Preparation phase
       ret = 0
       catchErrorList = ["No compatible site found, will not submit", 
                         "TaskDB: key projectName not found", 
                         "wrong format: submission failed", 
                         "Failed to declare task Boss infile not found",
			 "Operation failed",
			 "Total of 0 jobs submitted"]
			 
       # Command composition
       action = '-submit'
       if retry==True:   
            action = '-submit' #'-resubmit' 
	    
       cmd = 'export X509_USER_PROXY='+proxy+' && '
       cmd = cmd + 'crab '+action+' all -c '+ uniqDir + self.debug
       
       # Command summoning
       errcode, outlog = commands.getstatusoutput(cmd)
      
       # -------- Previous Version -------- # Fabio
       #errcode = 0
       #errcode = os.system(cmd)
       #f = open(uniqDir+"/log/crab.log","r")
       #outlog = f.readlines()
       #f.close()
       
       # Output parsing
       if errcode != 0:
           logging.info("Submission failed for %s. Return Code=%d"%(str(uniqDir), errcode))
           ret = 1
           return ret
	   
       for l in outlog.split('\n'):
           for e in catchErrorList:
                if e in l:
                     ret = 2
                     break
           if ret == 2:
                break

       if ret==0:
           logging.info("Submission completed for "+ str(uniqDir))
       else:
           logging.info("Submission delayed for " + str(uniqDir) )
       return ret

