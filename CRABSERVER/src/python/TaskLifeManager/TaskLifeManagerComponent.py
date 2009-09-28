#!/usr/bin/env python
"""
_TaskLifeManager_

"""

__revision__ = "$Id: TaskLifeManagerComponent.py,v 1.42 2009/08/10 15:08:58 farinafa Exp $"
__version__ = "$Revision: 1.42 $"

# Message service import
from MessageService.MessageService import MessageService

# logging
import logging
from logging.handlers import RotatingFileHandler

from ProxyLife import ProxyLife

# SE_API
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface
from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.Exceptions import OperationException

# Blite API import
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import TaskError

# API modules
from TaskLifeAPI import TaskLifeAPI
from TaskTracking.TaskStateAPI import TaskStateAPI

import os
import time


##############################################################################
# TaskLifeManagerComponent class
##############################################################################

class TaskLifeManagerComponent:
    """
    _TaskLifeManagerComponent_

    Component that polls the task database and notify about finished
    tasks.

    """

    ##########################################################################
    # TaskLifeManager component initialization
    ##########################################################################

    def __init__(self, **args):
        """
        Arguments:
        
          args -- all arguments from StartComponent.
        """
        # inital log information
        logging.info(" [TaskLifeManager starting...]")

        # initialize the server
        self.args = {}
        self.args.setdefault("Logfile", None)
        self.args.setdefault("storagePath", None)
        self.args.setdefault("taskLife", "360:00:00")
        self.args.setdefault("eMailAdmin", os.environ['USER'])
        self.args.setdefault("pollingTimeCheck", 10*60)
        self.args.setdefault("Protocol", "local")
        self.args.setdefault("storageName", "localhost")
        self.args.setdefault("storagePort", None)
        self.args.setdefault("ProxiesDir", "/tmp/del_proxies")
        self.args.setdefault("credentialType", "Proxy")
        self.args.setdefault("checkProxy", "off")
        self.args.setdefault('glExecDelegation', 'false')
        # update parameters
        self.args.update(args)

        # define log file
        if self.args['Logfile'] == None:
            from os.path import join
            self.args['Logfile'] = join(self.args['ComponentDir'],
                                                "ComponentLog")
        # create log handler
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 7)
        # define log format
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        # message service instances
        self.ms = None

        # initializing SBinterface
        storage = SElement( \
                            self.args["storageName"],
                            self.args["Protocol"], \
                            self.args["storagePort"] \
                          )
        self.SeSbI = SBinterface(storage)

        #######################
        ## args constraints ##
        #####################
        # which dbox ?!?
        if self.args['CacheDir'] == None:
            self.args['CacheDir'] = self.args['ComponentDir']
        if self.args['storagePath'] == None:
            self.args['storagePath'] = self.args['CacheDir']
        logging.info("Using cache "  +str(self.args['CacheDir']) )
        logging.info("Using storage " +str(self.args['storagePath']) )

        logging.info(" Setting task life time at: "\
                          + self.args['taskLife'].split(":", 1)[0] + "days "\
                          + self.args['taskLife'].split(":", 2)[1] + "hours "\
                          + self.args['taskLife'].split(":", 2)[2] + "mins" )

        # minimum time polling
        if int(self.args['pollingTimeCheck'] < 3):
            self.args['pollingTimeCheck'] = 3 * 60

        #####################################################
        ### parameters for blite connection of ProxyLife ###
        ###################################################
        self.bossCfgDB = {\
                           'dbName': self.args['dbName'], \
                           'user': self.args['user'], \
                           'passwd': self.args['passwd'], \
                           'socketFileLocation': self.args['socketFileLocation'] \
                         }

        self.proxypath = self.args["ProxiesDir"]

        dictSE =  { 
                    "SE":   self.args["storageName"], \
                    "prot": self.args["Protocol"],    \
                    "port": self.args["storagePort"], \
                    "base": self.args['storagePath'], \
                    "mail": self.args['eMailAdmin'],  \
                    "drop": self.args['CacheDir']
                  }

        # Add "glExecDelegation" to pass it to the thread
        self.credentialCfg = {
                        "credential": self.args['credentialType'], \
                        }

        additionalParams = {
                        "glExecDelegation": self.args['glExecDelegation'], \
                        }

        self.procheck = ProxyLife(self.bossCfgDB, self.proxypath, dictSE, additionalParams)

    ##########################################################################
    # handle events
    ##########################################################################
    
    def __call__(self, event, payload):
        """
        _operator()_

        Used as callback to handle events that have been subscribed to

        Arguments:
          event -- the event name
          payload -- its arguments
          
        Return:
          none
        """


        #######################
        # Automessage 4 polling
        if event == "TaskLifeManager::poll":
            self.pollingCycle()
            return            #
        #######################

        if event == "CRAB_Cmd_Mgr:GetOutputNotification":
            taskname, jobstr = payload.split('::')
            logging.info("Deleting osb of task: " + str(taskname) + \
                         " for jobs " + str(jobstr) )
            try:
                self.deleteRetrievedOSB( taskname, jobstr )
            except Exception, ex:
                import traceback
                logging.error( "Exception raised: " + str(ex) )
                logging.error( str(traceback.format_exc()) )
                logging.info( "problems deleting osb for job " + str(jobstr) )
            return

        if event == "CRAB_Cmd_Mgr:CleanRequest":
            logging.info("Clean requested for task: " + str(payload) )
            self.cleanTask(payload)
            return

        # start debug event
        if event == "TaskLifeManager:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        # stop debug event
        if event == "TaskLifeManager:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        
        # wrong event
        logging.info("")
        logging.info("Unexpected event %s, ignored" % event)
        logging.info("   payload = " + str(payload) )
        logging.info("")

    ##########################################################################
    # clean old tasks
    ##########################################################################


    def totSeconds(self, timeStr):
        """
        _totSeconds_
        """
        dd, hh, mm = timeStr.split(":")
        return (((int(dd)*24) + int(hh))*60 + int(mm))*60

    def deleteRetrievedOSB( self, taskName, strJobs ):
        """
        deleting OSB
        """
        from os.path import join
        ttdb = TaskStateAPI()
        proxy = ttdb.getProxy(taskName)
        logging.info("[%s]"%str(proxy))
        taskPath = join(self.args['storagePath'] , taskName)
        jobList = eval(strJobs)
        #####  TEMPORARY DS FIXME
        #if self.args['Protocol'].upper() == 'RFIO':
        #    proxy = '%s::%s'%(taskName.split('_')[0],proxy)
        #####
        for idjob in jobList:
            baseToDelete = [ \
                             "out_files_"+str(idjob)+".tgz", \
                             "crab_fjr_"+str(idjob)+".xml" \
                           ]
            for file in baseToDelete:
                try:
                    self.SeSbI.delete( join(taskPath, file), proxy )
                except Exception, ex:
                    import traceback
                    logging.info( "Exception raised: " + str(ex) )
                    logging.debug( str(traceback.format_exc()) )
                    logging.info( "problems deleting osb for job " + str(idjob) )


    def cleanTask(self, taskName):
        mySession = BossLiteAPI("MySQL", self.bossCfgDB)

        # Probably not needed, ask to Mattia about
        # tlapi = TaskLifeAPI()
        # tlapi.archiveBliteTask(mySession, taskName)
        # tlapi.archiveServerTask(taskName, mySession.bossLiteDB)

        # delete the osb files, but first get the 'all' range for the task
        task = mySession.loadTaskByName(taskName)
        rng = str( range(1, len(task.jobs) + 1) )
        self.deleteRetrievedOSB(taskName, rng )

        # once the poll will execut then the task will be cleaned silently
        pass

    def pollOldTask(self, oldness = ''):
        """
        _pollOldTask_
        """
        from os.path import join
        logging.info( "Start oldness polling..." )
         
        mySession = BossLiteAPI("MySQL", self.bossCfgDB)
        tlapi = TaskLifeAPI()

        ## not cleaned task here
        notcleanedll = []

        ## get not cleaned task ('True') older then 'oldness'
        taskll = tlapi.getTaskArrivedFrom( oldness, True, mySession.bossLiteDB )
        for taskx in taskll:
            # name - arrived - ended - archived - proxy
            logging.info( "Procesing [%s] task" %str(taskx[0]) )
            if int(taskx[3]) < 2:
                logging.info("Archiving task/jobs...")
                tlapi.archiveBliteTask(mySession, taskx[0])
                tlapi.archiveServerTask(taskx[0], mySession.bossLiteDB)
            logging.info("Cleaning task file on the storage...")
            ## list directory content
            filell = []
            try:
                filell = self.SeSbI.dirContent( join(self.args['storagePath'], \
                                                     taskx[0]),\
                                                taskx[4] )
            except OperationException, exc:
                logging.error("..problem on cleaning: %s" %str(exc))
                notcleanedll.append( taskx[0] )
            ## delete each file in the task directory
            for filex in filell:
                try:
                    self.SeSbI.delete( filex, taskx[4] )
                except OperationException, exc:
                    logging.error("..problem on cleaning: %s" %str(exc))
                    ## if delete fails: add to notify list
                    if not taskx[0] in notcleanedll:
                        notcleanedll.append( taskx[0] )
            ## directory content deleted: delete the dir
            if not taskx[0] in notcleanedll:
                try:
                    self.SeSbI.delete( join(self.args['storagePath'], taskx[0]),\
                                       taskx[4])
                except OperationException, exc:
                    logging.error("..problem on cleaning: %s" %str(exc))
                    ## if delete fails: add to notify list
                    notcleanedll.append( taskx[0] )

            ## set cleaned time
            tlapi.taskCleaned( mySession.bossLiteDB, taskx[0] )

        if len(notcleanedll) > 0:
            if self.procheck != None:
                ## checks and manages proxies 
                try:
                    self.procheck.notifyToClean(notcleanedll)
                except  Exception, ex:
                    import traceback
                    logging.error("Problem on polling proxies: \n" + str(ex) )
                    logging.error(" details: \n" + str(traceback.format_exc()) )

        logging.info("Oldness polling ended.")


    ##########################################################################
    # start component execution
    ##########################################################################

    def pollingCycle( self ):
        """
        _pollingCycle_

        """

        self.pollOldTask( self.totSeconds(self.args['taskLife']) )

        if self.procheck != None:
            ## checks and manages proxies 
            try:
                self.procheck.pollProxies(self.credentialCfg)
            except Exception, ex:
                import traceback
                logging.error("Problem on polling proxies: \n" + str(ex) )
                logging.error(" details: \n" + str(traceback.format_exc()) )
            
        # Renewing polling cycle
        pollT = int(self.args['pollingTimeCheck'])
        self.ms.publish( "TaskLifeManager::poll", "", \
                         "00:%.2d:%.2d"%( (pollT/60), (pollT%60) ) \
                       )
        self.ms.commit()

	
    ##########################################################################
    # start component execution
    ##########################################################################

    def startComponent(self):
        """
        _startComponent_

        Fire up the two main threads
        """

        # create message service instances
        #self.ms = MessageService()
        self.ms = MessageService()

        # register
        self.ms.registerAs("TaskLifeManager")

        # subscribe to messages
        self.ms.subscribeTo("TaskLifeManager:StartDebug")
        self.ms.subscribeTo("TaskLifeManager:EndDebug")
        self.ms.subscribeTo("TaskLifeManager::poll")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:GetOutputNotification")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:CleanRequest")

        # generate first polling cycle
        self.ms.remove("TaskLifeManager::poll")
        self.ms.publish("TaskLifeManager::poll", "")
        self.ms.commit()
        
        # wait for messages
        while True:
            msType, payload = self.ms.get()
            if payload != None:
                logging.info( "Got ms: ['"+str(msType)+"', '"+str(payload)+"']")
                try:
                    self.__call__(msType, payload)
                    logging.info( "Ms exe: ['"+str(msType)+"', '"+str(payload)+"']")
                except Exception, ex:
                    logging.error("Problem managing message %s with payload %s" %(msType, payload))
                    logging.error(str(ex))
                    continue
            else:
                logging.error("ERROR: empty payload - " + str(msType) )
                logging.error(" ")
            self.ms.commit()

