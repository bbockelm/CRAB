#!/usr/bin/env python
#pylint: disable-msg=W0613,W0703,E1101
"""
_CrabJobCreatorComponent_

"""
__version__ = "$Revision: 1.5 $"
__revision__ = "$Id: CrabJobCreatorComponent.py,\
                v 1.2 2009/10/13 15:19:38 riahi Exp $"

import os

import logging
from logging.handlers import RotatingFileHandler
import traceback

from MessageService.MessageService import MessageService
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

# WMCORE 
from WMCore.WMFactory import WMFactory
from WMCore.WMInit import WMInit
from WMCore.WorkerThreads.WorkerThreadManager import WorkerThreadManager
from CrabJobCreatorPoller import CrabJobCreatorPoller
from CrabJobCreator.CrabJobCreatorWorker import *

class CrabJobCreatorComponent:
    """
    _CrabJobCreatorComponent_
    
    """
    
################################
#   Standard Component Core Methods      
################################
    def __init__(self, **args):
        self.args = {}
        
        self.args.setdefault('Logfile', None)
        self.args.setdefault('CacheDir', None)
        self.args.setdefault('ProxiesDir', None)
        self.args.setdefault('CopyTimeOut', '')

        # SE support parameters
        # Protocol = local cannot be the default. Any default allowd 
        # for this parameter... it must be defined from config file. 
        self.args.setdefault('Protocol', '')
        self.args.setdefault('storageName', 'localhost')
        self.args.setdefault('storagePort', '')
        self.args.setdefault('storagePath', self.args["CacheDir"])

        # specific delegation strategy for glExex
        self.args.setdefault('glExecDelegation', 'false')

        self.args.setdefault('PollInterval',60)
        self.args.setdefault("HeartBeatDelay", "00:05:00")
        self.args.update(args)

        if len(self.args["HeartBeatDelay"]) != 8:
            self.HeartBeatDelay="00:05:00"
        else:
            self.HeartBeatDelay=self.args["HeartBeatDelay"]
        
        # define log file
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        # create log handler
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 7)
        # define log format
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        ## volatile properties
        self.wdir = self.args['ComponentDir']
        self.maxThreads = int( self.args.get('maxThreads', 5) )
        self.timePoolDB = self.args['PollInterval']

        # shared sessions 
        self.blDBsession = BossLiteAPI('MySQL', dbConfig, makePool=True)
        self.sessionPool = self.blDBsession.bossLiteDB.getPool()
        self.workerPool = self.blDBsession.bossLiteDB.getPool()

        # Get configuration
        self.init = WMInit()
        self.init.setLogging()
        self.init.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))

        self.myThread = threading.currentThread()
        self.factory = WMFactory("msgService", "WMCore.MsgService."+ \
                             self.myThread.dialect)
        self.newMsgService = self.myThread.factory['msgService']\
                          .loadObject("MsgService")

        self.ms = MessageService()

        self.workerCfg = {}
        logging.info(" ")
        logging.info("Starting component...")
    
    def startComponent(self):
        """
        _startComponent_
        Start up the component
        """
        # Registration in oldMsgService 
        self.ms.registerAs("CrabJobCreatorComponent")
        
        self.ms.subscribeTo("JobFailed")
        self.ms.subscribeTo("JobSuccess")
        self.ms.subscribeTo("CrabJobCreatorComponent:EndDebug")

        # Registration in new MsgService
        self.myThread.transaction.begin()
        self.newMsgService.registerAs("CrabJobCreatorComponent")
        self.myThread.transaction.commit()

        self.ms.subscribeTo("CrabJobCreatorComponent:HeartBeat")
        self.ms.remove("CrabJobCreatorComponent:HeartBeat")
        self.ms.publish("CrabJobCreatorComponent:HeartBeat","",self.HeartBeatDelay)
        self.ms.commit()
        self.workerCfg = self.prepareBaseStatus()      
        compWMObject = WMObject()
        manager = WorkerThreadManager(compWMObject)
        manager.addWorker(CrabJobCreatorPoller(self.workerCfg), float(self.timePoolDB))
        #######################################

        try:  
            while True:
                try:
                    event, payload = self.ms.get( wait = False )

                    if event is None:
                        time.sleep( self.ms.pollTime )
                        continue
                    else:
                        self.__call__(event, payload)
                        self.ms.commit()
                except Exception, exc:
                    logging.error("ERROR: Problem managing message...")
                    logging.error(str(exc))
        except Exception, e:
            logging.error(e)
            logging.info(traceback.format_exc())

        return

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug("Event: %s %s" % (event, payload))

        # update new job 
        if event == "JobSuccess" or event == "JobFailed":
            self.newJobUpdate(event, payload, self.workerCfg)
        # usual stuff
        elif event == "CrabJobCreatorComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
        elif event == "CrabJobCreatorComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
        elif event == "CrabJobCreatorComponent:HeartBeat":
            logging.info("HeartBeat: I'm alive ")
            self.ms.publish("CrabJobCreatorComponent:HeartBeat","",self.HeartBeatDelay)
            self.ms.commit()
        else:
            logging.info('Unknown message received %s + %s' %(event, payload))
        return True

################################

################################
#   Auxiliary Methods      
################################

    def prepareBaseStatus(self):
        """
        Prepare dictionary of config parameters
        """
        workerCfg = {}
        workerCfg['wdir'] = self.wdir
        workerCfg['SEproto'] = self.args['Protocol']
        workerCfg['SEurl'] = self.args['storageName']
        workerCfg['SEport'] = self.args['storagePort']
        workerCfg['retries'] = int( self.args.get('maxRetries', 0) )
        workerCfg['messageService'] = self.newMsgService
        workerCfg['blSessionPool'] = self.sessionPool
        workerCfg['blWorkerPool'] = self.workerPool
        workerCfg['scheduler'] = self.args.setdefault('scheduler','glite' )
        workerCfg['ProxiesDir'] = self.args['ProxiesDir']
        workerCfg['credentialType'] = self.args['credentialType']
        workerCfg['storagePath'] = self.args['storagePath']
        workerCfg['CacheDir']=self.args["CacheDir"]
        workerCfg['copyTout']=self.args["CopyTimeOut"]
        return workerCfg
 
    def newJobUpdate(self, event, payload, workerCfg):
        """
        Call updateState method to update wmbs job status of job in payload 
        """

        logging.info("Checking it: %s"%(str(payload)) )
        if event == "JobSuccess": 
            status = 'success'
        else:
            status = 'jobfailed' 
        try:
            handler = CrabJobCreatorWorker(logging, workerCfg)
            handler.updateState(payload, status)   
        except:
            logging.info('Error Calling worker: ')
            logging.info(traceback.format_exc())
        return True




##############################################################################

class DBCoreWMObject:
    """
    _DBCoreWMObject_
    
    """
    def __init__(self):
        self.dialect = "MySQL"
        self.connectUrl = "mysql://root@localhost/wmbs"

class ConfigWMObject:
    """
    _ConfigWMObject_
    
    """
    def __init__(self):
        self.CoreDatabase = DBCoreWMObject()

class WMObject:
    """
    _WMObject_
        
    """
    def __init__(self):
        self.config = ConfigWMObject()

