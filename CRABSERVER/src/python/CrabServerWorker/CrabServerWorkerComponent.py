#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.6 $"
__revision__ = "$Id: CrabServerWorkerComponent.py,v 1.6 2007/05/18 17:03:12 spiga Exp $"

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

        if int(self.args['debugLevel']) < 9:
             logging.info('Too low crab debug level for the server: debug 9 will be assumed.')
             self.args['debugLevel'] = 9

        self.debug = ' -debug ' + str(self.args['debugLevel'])

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Event: %s %s" % (event, payload))

        if event == "ProxyTarballAssociatorComponent:CrabWork":
            # logging.info("ProxyTarballAssociatorComponent:CrabWork Arrived!Payload = "+str(payload))
            self.pool.insertRequest( (payload, False) )

            #Matteo add: Message to TaskTracking for New Task in Queue 
            taskWorkDir = payload.split(':')[1]
            taskName = str(taskWorkDir.split('/')[-1])
            self.ms.publish("CrabServerWorkerComponent:TaskArrival", taskName)
            self.ms.commit()
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

        # drive the CW_WatchDog to send pending crashed tasks
        self.msNotify.publish("CW_WatchDogComponent:synchronize","")
        self.msNotify.commit()
        
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
       CrabworkDir = payload.split(':')[1]
       uniqDir = payload.split(':')[2]
       retry = int(payload.split(':')[3])
       retMsg = CrabworkDir.split('/')[-1]
       
       # Prepare the project and submit
       os.chdir(self.dropBoxPath)
       try:
            if performRetry == False:
            	self.registerTask(CrabworkDir)
                # logging.info("Registered Task")
            else:
                logging.info("Resubmission for "+str(CrabworkDir) )
       except Exception, e:
            logging.info("Error during BOSS Registration: "+ str(e) )
            pass

       # Submit the task
       logging.info("Put CRAB at work on "+ CrabworkDir + " with proxy " + proxy + " PerformRETRY " + str(performRetry))
       retCode = self.crab_submit(proxy, uniqDir, performRetry)
       # Prepare the proper payload to be managed by the notifier queue
       # #####
       # partial submission
       if type(retCode) is tuple:
            del retMsg
            retMsg = str(CrabworkDir.split('/')[-1])+'::'+str(retCode[0])+'::'+str(retCode[1])
            del retCode
            retCode = -3
            return (retMsg, retCode)
       # success
       if retCode == 0:
            retMsg = CrabworkDir.split('/')[-1]
            retCode = 0
            return (retMsg, retCode)
       # retCode in [1, 2] by default
       if retry > 0:
            # failure with retry hope 
            retCode = -1
            retMsg = proxy+":"+CrabworkDir+":"+uniqDir+":"+str(retry-1)
            logging.info("Task "+uniqDir+ " will be tried " + str(retry-1) + " more times.")
            return (retMsg, retCode)
       
       # failure without retry left
       retCode = -2
       retMsg = CrabworkDir.split('/')[-1]
       logging.info("WARNING: CRAB submission failed too many times. The task will not be submitted.")
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
                fakeJobSpec.setJobName(jobName) # WB suggested fix for new PA version # Fabio
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
       # RETURN SEMANTICS
       # 0 - no errors
       # 1 - command error
       # 2 - crab silent error catched by parsing output
       # tuple - part of the task not submitted

       # Preparation phase
       catchErrorList = ["No compatible site found, will not submit",
                         "TaskDB: key projectName not found",
                         "wrong format: submission failed",
                         "Failed to declare task Boss infile not found",
                         "Operation failed",
                         "Total of 0 jobs submitted",
                         "Stack dump raised by SOAP-ENV",
                         "14: unable to open database file"]
			 
       # Command composition
       cmd = 'export X509_USER_PROXY='+proxy+' && '
       cmd = cmd + 'crab -submit all -c '+ uniqDir + self.debug
       errcode, outlog = commands.getstatusoutput(cmd)
       
       if errcode != 0: 
            logging.info("Submission failed for %s. Return Code = %d. Resubmission will be tried."%(uniqDir, errcode))
            ret = 2 #1
            return ret

       # Catch silent errors from crab
       jobList = []
       crabSubmCounter = 0
       outlist = []
       nTotalJobs = -1

       outlist = outlog.split('\n')
       for l in outlist:
            # silent errors
            for e in catchErrorList:
                 if e in l:
                      ret = 2
                      logging.info("Submission delayed for " + str(uniqDir) )
                      return ret
            # partial submission list
            if "nj_list" in l:
                 jobList = eval( l.split("nj_list")[1] )
                 nTotalJobs = len(jobList)
                 continue
       
       # get list of actually submitted jobs # Fix Fabio
       errcode, outlog = commands.getstatusoutput('export X509_USER_PROXY=%s && crab -printId -c %s'%(proxy, uniqDir) )
       if errcode != 0:
            logging.info("Process ID listing failed for %s. Return Code = %d. "%(uniqDir, errcode))
            ret = 1 # not recoverable error
            return ret

       crabSubmCounter = 0
       outlist = outlog.split('\n')
       try:
            for l in outlist:
                 if 'Job: ' in l:
                      jid = eval(l.split('Job: ')[1].split(' ')[0]) - 1
                      if jid in jobList:
                           jobList.remove(jid)
                           crabSubmCounter += 1
       except Exception, e:
            logging.info(e)

       # check if the number of successful submission is equal to the number of expected jobs
       if len(jobList) == 0: #or nTotalJobs == crabSubmCounter:
            ret = 0
            logging.info("Submission completed for "+ str(uniqDir))
            return ret
       
       # Try to resubmit the missing jobs (heuristic approach)
       #
       #
       logging.info("Resubmission phase: %d jobs over %d submitted in %s."%(crabSubmCounter, nTotalJobs, uniqDir))
       
       errcode, outlog = commands.getstatusoutput(cmd + ' && crab -printId -c %s'%uniqDir)
       if errcode != 0:
            logging.info("Errors during partial resubmission of %s: exitCode %d. The submission will be retried."%(uniqDir, errcode) )
            ret = 2
            return ret

       # parse output and check enhancement on the pending job list
       outlist = outlog.split('\n')
       try:
            for l in outlist:
                 if 'Job: ' in l:
                      jid = eval(l.split('Job: ')[1].split(' ')[0]) - 1
                      if jid in jobList:
                           jobList.remove(jid)
                           crabSubmCounter += 1
       except Exception, e:
            logging.info(e)

       # everything is submitted
       if len(jobList) == 0: # or nTotalJobs == crabSubmCounter:
            ret = 0
            logging.info("Submission completed for "+ str(uniqDir))
            return ret
       
       # Still missing jobs
       logging.info("Still missing %d jobs for %s."%(len(jobList), uniqDir))
       # allign back crab and boss job numbers
       for idx in xrange(len(jobList)):
            jobList[idx] += 1
       #
       ret = (nTotalJobs, jobList)
       return ret

