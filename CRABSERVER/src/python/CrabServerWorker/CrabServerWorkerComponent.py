#!/usr/bin/env python
"""
_CrabServerWorkerComponent_

"""

__version__ = "$Revision: 1.0 $"
__revision__ = "$Id: CrabServerWorkerComponent.py,v 1.0 2006/11/20 15:15:00 farinafa Exp $"

import os
import socket
import pickle
import logging
import time
import popen2
from logging.handlers import RotatingFileHandler

from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService
from CrabServerWorker.PoolThread import PoolThread, Notifier

# Imported to allow BOSS Declaration (method registerJob) # Fabio
from JobState.JobStateAPI import JobStateChangeAPI
from MCPayloads.JobSpec import JobSpec
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
            self.pool.insertRequest(payload)
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
        self.ms.subscribeTo("CrabServerWorkerComponent:StartDebug")
        self.ms.subscribeTo("CrabServerWorkerComponent:EndDebug")
        
        # create a pool of threads that will execute a performCrabWork action
        self.pool = PoolThread(int(self.args['maxThreads']), self)

        # create notifier thread
        notifier = Notifier(self.pool, self.msNotify, logging)

        # get BOSS cfg informations # Fabio
        # self.cfgObject = ProdAgentConfiguration()
        # self.cfgObject.loadFromFile(config)
        # self.bossCfgDir = self.cfgObject.get("BOSS")['configDir'] 
        # logging.info("Using BOSS configuration from " + self.bossCfgDir)
        
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("CrabServerWorkerComponent: %s %s" % ( type, payload))
            self.__call__(type, payload)

    def performCrabWork(self,payload):
       proxy = payload.split(':')[0]
       CrabworkDir =  payload.split(':')[1]
       uniqDir = payload.split(':')[2]

       # Prepare the project and submit
       os.chdir(self.dropBoxPath)
       try:
            self.registerTask(CrabworkDir)
            pass
       except Exception, e:
            logging.info("Error during BOSS Registration: "+ str(e) )

       # Submit the task
       try:
            cmd = 'export X509_USER_PROXY='+proxy+'; '  # Fabio #+ CrabworkDir+'/share/userProxy;'
            cmd = cmd + 'crab -submit all -c '+ uniqDir + self.debug
            os.system(cmd)
       except Exception, e:
            logging.info("Error during CRAB call: "+ str(e) ) 

       logging.info("Put CRAB at work on "+ CrabworkDir + " with proxy " + proxy)
       logging.debug(cmd)
       return CrabworkDir.split('/')[-1]

    def registerTask(self, cwDir):
        jNameBase = cwDir.split('/')[-1]
        cmd = "cat "+ cwDir+"/share/cmssw.xml | grep ruleElement"
        fout, fin = popen2.popen4(cmd)
        ruleList = fout.readlines()
        fout.close()
        fin.close()

        jobNumber = int(ruleList[0].split(' ')[1].split(':')[1])
        logging.info("Job Numbers :"+ str(jobNumber))

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
            except Exception, e:
                logging.info("BOSS Registration problem %s"%cacheArea + str(e))

            JobStateChangeAPI.create(jobName, cacheArea)
            JobStateChangeAPI.inProgress(jobName)
            JobStateChangeAPI.submit(jobName)
        pass

    def crab_submit(self, proxy, uniqDir, workdir, dbgLv):
        from crab import Crab
        # actual code here
        dbg = 0
        if dbgLv!='':
              dbg = int(dbgLv)

        # NOTE: the multithread nature of the component could bring to some race condition problems in 
        # the X509 var env overriding by the various threads. A solution have to be discussed.
        # 1. Simplest way: use system to: isolate the execution environment
        # 2. Sychronize the submissions: whe would have parallel task registrations (time consuming part, good speed up anyway)
        # 3. Find a way to isolate the thread environments (aka convert them to processes triggered by IPC communication)
        # Fabio 
        cmd = 'export X509_USER_PROXY='+proxy+'; '
        cmd = cmd + 'crab -submit all -c '+ uniqDir + ' -debug ' + str(dbg)
        os.system(cmd)
        pass
