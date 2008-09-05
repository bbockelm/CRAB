#!/usr/bin/env python
"""
_TaskTracking_

"""

__revision__ = "$Id: TaskTrackingComponent.py,v 1.100 2008/09/02 16:19:42 mcinquil Exp $"
__version__ = "$Revision: 1.100 $"

import os
import time
import datetime
import sys
import re

# Blite API import
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import TaskError, JobError

# Message service import
from MessageService.MessageService import MessageService

# threads
from threading import Thread, BoundedSemaphore
from threading import Condition

# logging
import logging
from logging.handlers import RotatingFileHandler
import ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentCore.Configuration import ProdAgentConfiguration

# DB PA
from TaskStateAPI import TaskStateAPI

# XML
from CrabServer.CreateXmlJobReport import * 
from CrabServer.XmlFramework import *

# subject & original name
from UtilSubject import *

import traceback
#queue import
import Queue
#cPickle import
import cPickle

##############################################################################
# TaskTrackingComponent class
##############################################################################

maxnum = 1
semMsgQueue = BoundedSemaphore(maxnum) #for synchronisation between thread for the msgQueue 
#### will move all var to init for avoiding changement of var value by instance of this class  
ms_sem = BoundedSemaphore(maxnum)

class TaskTrackingComponent:
    """
    _TaskTrackingComponent_

    Component that polls the task database and notify about finished
    tasks.

    """

    ##########################################################################
    # TaskTracking component initialization
    ##########################################################################

    def __init__(self, **args):
        """
        
        Arguments:
        
          args -- all arguments from StartComponent.
          
        Return:
            
          none

        """

        # initialize the server
        self.args = {}
        self.args.setdefault("PollInterval", 5 )
        self.args.setdefault("Logfile", None)
        self.args.setdefault("dropBoxPath", None)
        self.args.setdefault("Thread", 5)
        
        # update parameters
        self.args.update(args)
        logging.info("Using "+str(self.args['dropBoxPath'])+" as DropBox")

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

        # inital log information
        logging.info("TaskTracking starting... ")

        # message service instances
        self.ms = None
        self.msThread = None
 
        # init crab.cfg name
        self.workAdd = "_spec/"
        self.xmlReportFileName = "xmlReportFile.xml"
        self.tempxmlReportFile = ".tempxmlReportFileName"

	self.taskState = [\
                           "arrived", \
                           "submitting", \
                           "not submitted", \
                           "submitted", \
                           "killed", \
                           "ended", \
                           "unpacked", \
                           "partially submitted", \
                           "partially killed", \
                           "range submitted" \
                         ]

        self.bossCfgDB = {\
                           'dbName': self.args['dbName'], \
                           'user': self.args['user'], \
                           'passwd': self.args['passwd'], \
                           'socketFileLocation': self.args['socketFileLocation'] \
                         }

        self.mySession = None
        ## bossLite session
        try:
            self.mySession = BossLiteAPI("MySQL", self.bossCfgDB, makePool=True)
            self.sessionPool = self.mySession.bossLiteDB.getPool()
        except Exception, ex:
            logging.info(str(ex))
            logging.info(str(traceback.format_exc()))
            return 0

    ##########################################################################
    # handle events
    ##########################################################################
    
    def __appendDbgInfo__( self, taskName, message ):
        """
        _appendDbgInfo_
        
        update debug informations about task processing
        """
        message.setdefault('date', str(time.time()))
        #path of the logging info file 
        filepath = os.path.join( self.args['dropBoxPath'], \
                                 taskName+self.workAdd,    \
                                 'internalog.xml'          \
                               )
        #init the xml frame
        c = XmlFramework()
        #if file already exists neeed to append at the end
        if not os.path.exists( filepath ):
            c.initialize(taskName)
        else: 
            c.fromFile( filepath )
        #create the new event
        ev = Event()
        #add the dictionary information to the event
        ev.initialize(message)
        #add the node to the xml
        c.addNode(ev)
        #write the xml to the path
        c.toFile( filepath )

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

        logging.debug("Received Event: %s" % event)
        logging.debug("Payload: %s" % payload)

        OK = ""
        ERROR = ""
        taskName = payload
        logBuf = ""
        _loginfo = {}
        _loginfo.setdefault('ev', str(event))

        # new task to insert
	#if event == "DropBoxGuardianComponent:NewFile":
        if event == "CRAB_Cmd_Mgr:NewTask":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, "NewTask: %s" % taskName)
		logBuf = self.__log__(logBuf, taskName)
		self.insertNewTask( taskName )
                logBuf = self.__log__(logBuf, "               new task inserted.")
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                _loginfo.setdefault('txt', str("Arrived task: " + str(taskName)))
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: wrong payload from [" +event+ "]!!!!")
                logBuf = self.__log__(logBuf, " ")
            logging.info(logBuf)
            self.__appendDbgInfo__(taskName, _loginfo)
            return None

        if event == "CRAB_Cmd_Mgr:MailReference":
            if payload != None or payload != "" or len(payload) > 0:
                taskName, eMail, threshold = payload.split("::")
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, "E-mail "+str(eMail)+" and threshold "+str(threshold)+" arrived for task %s" % taskName)
                self.updateEmailThresh(taskName, eMail, threshold)
	        logBuf = self.__log__(logBuf, "     db updated.")
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: wrong payload from [" +event+ "]!!!!")
                logBuf = self.__log__(logBuf, " ")
            logging.info(logBuf)
            return None

	if event == "TaskRegister:TaskArrival":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                ## start dbg info ##
                OK += "  Task ["+ taskName +"] ready to be submitted and already in queue.\n"
                ## end dbg info ##
                logBuf = self.__log__(logBuf, "Submitting Task: %s" % str(taskName) )
		self.updateTaskStatus( taskName, self.taskState[1] )
                logBuf = self.__log__(logBuf, "              task updated.")
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                _loginfo.setdefault('txt', str("Task in submission queue: " + str(taskName)))
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+taskName+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            self.__appendDbgInfo__(taskName, _loginfo)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:CrabWorkPerformed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, "CrabWorkPerformed: %s" % payload)
                ## start dbg info ##
                OK += "  Task ["+ taskName +"] succesfully submitted to the grid.\n"
                ## end dbg info ##
		self.updateTaskStatus( taskName, self.taskState[3])
                logBuf = self.__log__(logBuf, "               task updated.")
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+taskName+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:CrabWorkFailed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, "CrabWorkFailed: %s" % payload)
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                ## start dbg info ##
                OK += "  Task ["+taskName+"] not submitted to the grid.\n"
                ## end dbg info ##
		self.updateTaskStatus(taskName, self.taskState[2])
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:SubmitNotSucceeded" or \
            event == "CrabServerWorkerComponent:TaskNotSubmitted":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, str(event.split(":")[1]) + ": %s" % payload)
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                taskName, taskStatus, reason = payload.split("::")
                ## start dbg info ##
                OK += "  Task ["+taskName+"] not submitted to the grid\n"+ \
                      "        status = " + str(taskStatus) + \
                      "        reason = " + str(reason)
                ## end dbg info ##
                #self.updateTaskStatus(taskName, self.taskState[2])
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:FastKill":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, event + ": " + str(payload) )
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                ## start dbg info ##
                OK += "  FastKill: task ["+payload+"] killed before the submission to the grid.\n"
                ## end dbg info ##
                self.updateTaskStatus(payload, self.taskState[4])
#                self.updateTaskStatus(payload, self.taskState[5])
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:FatWorkerResult":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, event + ": " + str(payload) )
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                threadName, taskName, code, reason, time = payload.split("::")
                _loginfo.setdefault('txt', str("Submission completed: " + str(taskName)))
                _loginfo.setdefault('reason', str(reason))
                _loginfo.setdefault('code', str(code))
                _loginfo.setdefault('time', str(time))
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            self.__appendDbgInfo__(taskName, _loginfo)
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CRAB_Cmd_Mgr:NewCommand":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, event + ": " + str(payload) )
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                taskName, count = payload.split("::")
                _loginfo.setdefault('txt', str("New command: " + str(taskName)))
                _loginfo.setdefault('count', str(count))
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            self.__appendDbgInfo__(taskName, _loginfo)
            logging.info(logBuf)

        if event == "KillTask":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, event + ": " + str(payload) )
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                taskName, fake_proxy, range = payload.split(":")
                _loginfo.setdefault('txt', str("Submisson completed: " + str(taskName)))
                _loginfo.setdefault('range', str(range))
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            self.__appendDbgInfo__(taskName, _loginfo)
            logging.info(logBuf)


        if event == "TaskKilled":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                rangeKillJobs = "all"
                if payload.find("::") != -1:
                    taskName, rangeKillJobs = payload.split("::")
                logBuf = self.__log__(logBuf, "   Killed task: %s" % taskName)
                if rangeKillJobs == "all":
                    self.updateTaskKilled( taskName, self.taskState[4] )
                else:
                    self.updateTaskKilled( taskName, self.taskState[8] )
                ## start dbg info ##
                OK += "  Task ["+str(payload.split("::")[0])+"] killed (jobs killed: "+str(rangeKillJobs)+").\n"
                ## end dbg info ##
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "TaskKilledFailed":
            if payload != None or payload != "" or len(payload) > 0: 
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                rangeKillJobs = "all"
                if payload.find("::") != -1:
                    taskName, rangeKillJobs = payload.split("::")
                logBuf = self.__log__(logBuf, "   Error killing task: %s" % taskName)
                ## start dbg info ##
                OK += "  WARNING: task ["+str(payload.split("::")[0])+"] failed to kill (jobs to be killed: "+str(rangeKillJobs)+").\n"
                ## end dbg info ##
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            else:
                logBuf = self.__log__(logBuf, " ")
                logBuf = self.__log__(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__log__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CRAB_Cmd_Mgr:GetOutputNotification":
            if payload != "" and payload != None:
                taskName, jobstr = payload.split('::')
                logging.info("Cleared jobs: " + str(jobstr) + \
                             " for task " + str(taskName) )
                try:
                    self.setCleared(taskName, eval(jobstr))
                except Exception, ex:
                    import traceback
                    logging.error( "Exception raised: " + str(ex) )
                    logging.error( str(traceback.format_exc()) )
                _loginfo.setdefault('txt', str("GetOutput performed: " + str(taskName)))
                _loginfo.setdefault('range', str(jobstr))
            else:
                logging.error("No task specified for " + str(event) )
            logging.debug('output retrieved ')
            self.__appendDbgInfo__(taskName, _loginfo)
            return

        
        # start debug event
        if event == "TaskTracking:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return None

        # stop debug event
        if event == "TaskTracking:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return None

        # wrong event
        logBuf = self.__log__(logBuf, "  <-- - -- - -->")
        logBuf = self.__log__(logBuf, "Unexpected event %s, ignored" % event)
        logBuf = self.__log__(logBuf, "  <-- - -- - -->")
        logging.info(logBuf)

        return None

    ##########################################################################
    # insert and update task in database
    ##########################################################################


    def insertNewTask( self, payload ):
        """
        _insertNewTask_
        """
        logBuf = ""
        try:
            ttdb = TaskStateAPI()
            ttdb.insertTaskPA( payload, self.taskState[0] )
        except Exception, ex:
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            logBuf = self.__log__(logBuf, "ERROR while inserting the task " + str(payload) )
            logBuf = self.__log__(logBuf, "      "+str(ex))
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
        logging.info(logBuf)

    def updateEmailThresh( self, taskname, email, threshold ):
        """
        _updateEmailThresh_
        """
        logBuf = ""

        if email == None:
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            logBuf = self.__log__(logBuf, "ERROR: missing 'eMail' for task: " + str(taskname) )
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
        elif threshold == None:
            threshold = "100"
        else:
            try:
                ttdb = TaskStateAPI()
                ttdb.updateEmailThresh( taskname, str(email), str(threshold) )
            except Exception, ex:
                logBuf = self.__log__(logBuf, "  <-- - -- - -->")
                logBuf = self.__log__(logBuf, "ERROR while updating the 'eMail' field for task: " + str(taskname) )

    def updateTaskStatus(self, payload, status):
        """
	_updateTaskStatus_

	 update the status of a task
        """
        logBuf = ""	
        ttdb = TaskStateAPI()
        try:
            ttdb.updateStatus( payload, status )
        except Exception, ex:
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            logBuf = self.__log__(logBuf, "ERROR while updating the task " + str(payload) )
            logBuf = self.__log__(logBuf, "      "+str(ex))
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            logging.info(logBuf)
            logBuf = ""

	eMail = ""
	uuid = ""
        taskObj = None
        try:
            if status == self.taskState[3] or status == self.taskState[7]:
                mySession = BossLiteAPI("MySQL", pool=self.sessionPool)

		try:
		    taskObj = mySession.loadTaskByName( payload )
		except TaskError, te:
		    taskObj = None
		    pass
		if taskObj is None:
		    logBuf = self.__log__(logBuf,"  Requested task [%s] does not exist."%(payload) )
		else:
                    proxy = taskObj['user_proxy']
	            logBuf = self.__log__(logBuf, "using proxy: [%s] "%(str(proxy)) )
                    try:
                        ttdb.updateProxy(payload, proxy) 
		    except Exception, ex:
		        logBuf = self.__log__(logBuf, "  <-- - -- - -->")
	     	        logBuf = self.__log__(logBuf, "ERROR while updating the task " + str(payload) )
		        logBuf = self.__log__(logBuf, "      "+str(ex))
		        logBuf = self.__log__(logBuf, "  <-- - -- - -->")
		        logging.info(logBuf)
		        logBuf = ""
                mySession.bossLiteDB.close()
                del mySession
	    elif status == self.taskState[2] or status == self.taskState[4]:
	        valuess = ttdb.getStatusUUIDEmail( payload )
		if valuess != None:
		    status = valuess[0]
		    if len(valuess) > 1:
		        uuid = valuess[1]
		        if len(valuess) > 2:
		    	    eMail = valuess[2]
                    if status == self.taskState[2]:
	                ## XML report file
                        dictionaryReport =  {"all": ["NotSubmitted", "", "", 0, '', 'C']}
                        self.prepareReport( payload, uuid, eMail, 0, 0, dictionaryReport, 0, 0 )
                        ## MAIL report user
                        #self.prepareTaskFailed( payload, uuid, eMail, status )
                    else:
                        ## XML report file
                        dictionaryReport =  {"all": ["Killed", "", "", 0, '', 'C']}
                        self.prepareReport( payload, uuid, eMail, 0, 0, dictionaryReport, 0, 0 )
        except Exception, ex:
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            logBuf = self.__log__(logBuf, "ERROR while reporting info about the task " + str(payload) )
            logBuf = self.__log__(logBuf, "      "+str(ex))
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            logging.info(logBuf)


    def prepareTaskFailed( self, taskName, uuid, eMail, status ):
        """
	_prepareTaskFailed_
	
	"""
	obj = UtilSubject( self.args['dropBoxPath'], taskName, uuid )
	origTaskName, userName = obj.getInfos()
        del obj
	eMaiList = self.getMoreMails(eMail)
	strEmail = ""
	for mail in eMaiList:
	    strEmail += str(mail) + ","
        ttdb = TaskStateAPI()
	ttdb.updatingNotifiedPA( taskName, 2 )
        if status == self.taskState[2]:
            self.taskNotSubmitted( self.args['dropBoxPath'] + "/" + taskName + self.workAdd + self.xmlReportFileName, taskName )
        else:
            self.taskFailed(origTaskName, strEmail[0:len(strEmail)-1], userName)
	 

    def updateTaskKilled ( self, taskName, status ):
        """
        _updateTaskKilled_
        """
        logBuf = ""
        try:
            ttdb = TaskStateAPI()
            ttdb.updateStatus( taskName, status )
        except Exception, ex:
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            logBuf = self.__log__(logBuf, "ERROR while updating the task " + str(taskName) )
            logBuf = self.__log__(logBuf, "      "+str(ex))
            logBuf = self.__log__(logBuf, "  <-- - -- - -->")
            logging.info(logBuf)

    def setCleared (self, taskName, jobList):
        """
        _setCleared_
        """
        taskObj = None
        mySession = None

        ## bossLite session
        try:
            mySession = BossLiteAPI("MySQL", pool=self.sessionPool)
            ## using session pool
            #mySession = BossLiteAPI("MySQL", self.bossCfgDB)
        except Exception, ex:
            logging.info(str(ex))
            return 0
        try:
        ## lite task load in memory
            try:
                taskObj = mySession.loadTaskByName( taskName )
            except TaskError, te:
                taskObj = None
            pass
            if taskObj is None:
                logging.info("Unable to load task [%s]."%(taskName))
            else:
                for jobbe in taskObj.jobs:
                    try:
                        mySession.getRunningInstance(jobbe)
                    except JobError, ex:
                        logging.error('Problem loading job running info')
                        continue
                    logging.info(str(jobbe['jobId']) + " in " + str(jobList))
                    if jobbe['jobId'] in jobList:
                        logging.info(str(jobbe.runningJob['status']))
                        if jobbe.runningJob['status'] in ["D","E", "DA", "SD"]:
                            logging.info("updated")
                            jobbe.runningJob['status'] = "UE"
                            jobbe.runningJob['processStatus'] = "output_requested"
                mySession.updateDB(taskObj)
                ## update xml -> W: duplicated code - need to clean
                dictReportTot = {'JobSuccess': 0, 'JobFailed': 0, 'JobInProgress': 0}
                countNotSubmitted = 0
                dictStateTot = {}
                numJobs = len(taskObj.jobs)
                dictStateTot, dictReportTot, countNotSubmitted = self.computeJobStatus(taskName, mySession, taskObj, dictStateTot, dictReportTot, countNotSubmitted)
                pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + self.workAdd + "/"
                if os.path.exists( pathToWrite ):
                    self.prepareReport( taskName, "", "", "", "", dictStateTot, numJobs, 1 )
                    self.undiscoverXmlFile( pathToWrite, taskName, self.tempxmlReportFile, self.xmlReportFileName )

        except Exception, ex:
            import traceback
            logging.error( "Exception raised: " + str(ex) )
            logging.error( str(traceback.format_exc()) )
        mySession.bossLiteDB.close()
        del mySession


    ##########################################################################
    # utilities
    ##########################################################################

    def convertStatus( self, status ):
        """
        _convertStatus_
        U  : undefined
        C  : created
        S  : submitted
	SR : enqueued by the scheduler
	R  : running
	A  : Aborted
	D  : Done
	K  : killed
	E  : erased from the scheduler queue (also disappeared...)
	DA : finished but with some failures (aka Done Failed in GLite or Held for condor)
        UE : user ended (retrieved by th user)
        """
        stateConverting = { \
                    'R': 'Running', 'SD': 'Done', 'DA': 'Done (Failed)', \
                    'E': 'Done', 'SR': 'Ready', 'A': 'Aborted', \
                    'SS': 'Scheduled', 'U': 'Unknown', 'SW': 'Waiting', \
                    'K': 'Killed', 'S': 'Submitted', 'SU': 'Submitted', \
                    'NotSubmitted': 'NotSubmitted', 'C': 'Created', \
                    'UE': 'Cleared'
                          }
        if status in stateConverting:
            return stateConverting[status]
        return 'Unknown'


    def prepareReport( self, taskName, uuid, eMail, thresholdLevel, percentage, dictReportTot, nJobs, flag ):
        """
        _prepareReport_
        """
        pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + self.workAdd + "/"

        if os.path.exists( pathToWrite ):
            ###  get user name & original task name  ###
            obj = UtilSubject(self.args['dropBoxPath'], taskName, uuid)
            origTaskName, userName = obj.getInfos()
            del obj
            ###  preparing xml report  ###
            c = CreateXmlJobReport()
            eMaiList = self.getMoreMails( eMail )
            if len(eMaiList) < 1:
                c.initialize( origTaskName, "", userName, percentage, thresholdLevel, nJobs)
            else:
                for index in xrange(len(eMaiList)):
                    if index != 0:
                        c.addEmailAddress( eMaiList[index] )
                    else:
                        c.initialize( origTaskName, eMaiList[0], userName, percentage, thresholdLevel, nJobs)

            for singleJob in dictReportTot:
                J = Job()   ##    id             status                        eec
                J.initialize( singleJob, dictReportTot[singleJob][0], dictReportTot[singleJob][2], \
                            ##         jes                       clear                       Resub
                              dictReportTot[singleJob][1], dictReportTot[singleJob][3], self.getListEl(dictReportTot[singleJob], 4), \
                            ##         site                                               sched_status
                              self.getListEl(dictReportTot[singleJob], 5),  self.getListEl(dictReportTot[singleJob], 6), \
                            ##        sId
                              self.getListEl(dictReportTot[singleJob],9) )
                c.addJob( J )
            c.toXml()
            c.toFile ( pathToWrite + self.tempxmlReportFile )
            if not flag:
                self.undiscoverXmlFile( pathToWrite, taskName, self.tempxmlReportFile, self.xmlReportFileName )

    def getListEl(self, lista, el):
        try:
            return lista[el]
        except Exception, ex:
            logging.debug(" problems reading destination site info.")
            return None

    def getMoreMails ( self, eMail ):
        """
        _getMoreMails_

        prepares a list of eMails from str "eMail"
        """

        eMaiList2 = []
        if eMail != None:
            eMaiList = eMail.split(";")
            for index in xrange(len(eMaiList)):
                temp = eMaiList[index].replace(" ", "")
                if self.checkEmail( temp ):
                    eMaiList2.append( temp )

        return eMaiList2


    def checkEmail ( self, eMail ):
        """
        _checkEmail_
        
        check the eMail with a regular expression
        """

        reg = re.compile('^[\w\.-_]+@(?:[\w-]+\.)+[\w]{2,4}$', re.IGNORECASE)
        if not reg.match( eMail ):
            errmsg = "Error parsing e-mail address; address ["+eMail+"] has "
            errmsg += "an invalid format;"
            logging.debug("WARNING: " + errmsg)
            logging.debug("         this e-mail address will be ignored.")
            return False
        return True


    def undiscoverXmlFile (self, path, taskName, fromFileName, toFileName):
        if os.path.exists(path + fromFileName):
            infile = file(path + fromFileName , 'r').read()
            file(path + toFileName , 'w').write(infile)


    ##########################################################################
    # publishing messages
    ##########################################################################

    def taskEnded( self, taskName ):
        """
        _taskEnded_
        
        Starting managing by TaskLifeManager component
        """
        logBuf = ""
        pathToWrite = self.args['dropBoxPath'] + '/' + taskName
        if os.path.exists( pathToWrite ):
            try:
                ms_sem.acquire()
                self.msThread.publish("TaskTracking:TaskEnded", taskName)
                self.msThread.commit()
            finally:
                ms_sem.release()

            logBuf = self.__log__(logBuf, "--> [TaskEnded] %s" % taskName)
        logging.info(logBuf)


    def taskSuccess( self, taskPath, taskName ):
        """
        _taskSuccess_
        
        Trasmit the "TaskSuccess" event to the prodAgent
        """
        logBuf = ""
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskSuccess", taskPath)
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__log__(logBuf, "--> [TaskSuccess] %s" % taskPath)
        logging.info(logBuf)


    def taskFailed( self, taskName, eMaiList, userName ):
        """
        _taskFailed_

        Trasmit the "TaskFailed" event to the prodAgent

        """
        logBuf = ""
	if userName == "" or userName == None:
	    userName = "Unknown"
        payload = taskName + ":" + userName + ":" + eMaiList
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskFailed", payload)
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__log__(logBuf, "--> [TaskFailed] %s" % payload)
        self.taskEnded(taskName)
        logging.info(logBuf)


    def taskNotSubmitted( self, taskPath, taskName ):
        """
        _taskNotSubmitted_
        """
        logBuf = ""
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskNotSubmitted", taskPath) 
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__log__(logBuf, "==> [NotSubmitted] %s" % taskPath)
        self.taskEnded(taskName)
        logging.info(logBuf)

    def __log__(self, buf, toadd):
        """
        __logToBug__
        input:
        - buf: the string buffer
        - toadd: the string to appent to the buffer
        output:
        - the buffer with a new row like [datetime] - [str]
        this is an helper method for poolTasks one.
        """
        #avoid to log empty message
        if toadd == None or toadd == "":
            bufRet = str(buf)
        else:
            bufRet = str(buf) + "\n" + str(datetime.datetime.now()) + \
                     " - " + str(toadd)
        return bufRet


    ##########################################################################
    # polling js_taskInstance and BOSS DB
    ##########################################################################


    def computeJobStatus(self, taskName, mySession, taskObj, dictStateTot, \
                               dictReportTot, countNotSubmitted):
        """
        _computeJobStatus_
        """
        ttdb = TaskStateAPI()

        for jobbe in taskObj.jobs:
            try:
                mySession.getRunningInstance(jobbe)
	    except JobError, ex:
	        logging.error('Problem loading job running info')
	        break
	    job   = jobbe.runningJob['jobId']
  	    stato = jobbe.runningJob['status']
	    sId   = jobbe.runningJob['schedulerId']
	    jec   = str( jobbe.runningJob['wrapperReturnCode'] )
	    eec   = str( jobbe.runningJob['applicationReturnCode'] )
	    joboff = str( jobbe.runningJob['closed'] )
	    site  = ""
	    if jobbe.runningJob['destination'] != None and \
               jobbe.runningJob['destination'] != '':
	        site  = jobbe.runningJob['destination'].split(":")[0]
	    del jobbe
 
	    resubmitting, MaxResub, Resub, internalstatus = \
                        ttdb.checkNSubmit(mySession.bossLiteDB, taskName, job)
                    
            vect = []
            if eec == "NULL" and jec == "NULL":
                vect = [ self.convertStatus(stato), "", "", 0, Resub, site, \
                         stato, joboff, resubmitting, sId]
            else:
                vect = [ self.convertStatus(stato), eec, jec, 0, Resub, site, \
                         stato, joboff, resubmitting, sId]
            dictStateTot.setdefault(job, vect)

            if stato == "E":
                if (eec == "0" or eec == "" or eec == "NULL") and jec == "0":
                    dictReportTot['JobSuccess'] += 1
                    dictStateTot[job][3] = 1
                elif not resubmitting:
                    dictReportTot['JobFailed'] += 1
                    dictStateTot[job][0] = "Done (Failed)"
                    dictStateTot[job][3] = 1
                else:
                    dictReportTot['JobInProgress'] += 1
            elif stato == "A" or stato == "Done (Failed)" or stato == "K":
                if not resubmitting:
                    dictReportTot['JobFailed'] += 1
                else:
                    dictReportTot['JobInProgress'] += 1
            elif not resubmitting and joboff == 'Y':
                dictReportTot['JobFailed'] += 1
                dictStateTot[job][3] = 1
            elif stato == "C":
                if (internalstatus in ["failed", "finished"] and not resubmitting) \
                  or internalstatus == "reallyFinished":
                   countNotSubmitted += 1
                   dictReportTot['JobFailed'] += 1
                   dictStateTot[job][0] = "NotSubmitted"
                else:
                   countNotSubmitted += 1
                   dictReportTot['JobInProgress'] += 1
            elif not resubmitting:
                dictReportTot['JobInProgress'] += 1
            else:
                dictReportTot['JobInProgress'] += 1

        #rev_items = [(v, int(k)) for k, v in dictStateTot.items()]
        #rev_items.sort()
        #dictStateTot = {}
        #for valu3, k3y in rev_items:
        #    dictStateTot.setdefault( k3y, valu3 )
        #del rev_items

        return dictStateTot, dictReportTot, countNotSubmitted

    def pollTasks(self, threadName):
        """
        _pollTasks_

        Poll the task database
        @note: function utility for a cross-thread-readable logging 
        """
        logBuf = ""

        task = None
        taskName = ""
        numJobs = 0
        taskObj = None
        mySession = None
        ttdb = TaskStateAPI()

        ## bossLite session
        try:
            mySession = BossLiteAPI("MySQL", pool=self.sessionPool)
        except Exception, ex:
            logging.info(str(ex))
            return 0
        try:
            ## loading task from DB
            task = ttdb.getNLockFirstNotFinished(mySession.bossLiteDB)
            _loginfo = {} 
            try:
                taskId = 0
                if task == None or len(task) <= 0:
                    ttdb.resetControlledTasks(mySession.bossLiteDB)
                else:
                    taskId = task[0][0]
                    taskName = task[0][1]
		    eMail = task[0][2]
		    notified = int(task[0][4])
		    thresholdLevel = task[0][3]
		    endedLevel = task[0][5]
		    status = task[0][6]
		    uuid = task[0][7]

                    if status == self.taskState[2] and notified < 2:
                        ######### Taskfailed is prepared now
                        self.prepareTaskFailed( taskName, uuid, eMail, status)
                    else:
                        ## lite task load in memory
                        try:
                            taskObj = mySession.loadTaskByName( taskName )
                        except TaskError, te:
                            taskObj = None
                            pass 
                        if taskObj is None:
                            logBuf = self.__log__(logBuf, "Unable to load " + \
                                                  "task [%s]." % (taskName))
                        else:
                            logBuf = self.__log__(logBuf, " - - - - - - - ")
                            logBuf = self.__log__(logBuf, " [" + str(taskObj['id']) + "] *" + taskName + "*:")

			    pathToWrite = ""
			    dictReportTot = {'JobSuccess': 0, 'JobFailed': 0, 'JobInProgress': 0}
			    countNotSubmitted = 0 
			    dictStateTot = {}
                            numJobs = len(taskObj.jobs)
                            dictStateTot, dictReportTot, countNotSubmitted = \
                                 self.computeJobStatus( taskName, mySession, \
                                                        taskObj, dictStateTot, \
                                                        dictReportTot, \
                                                        countNotSubmitted )
			    for state in dictReportTot:
                                logBuf = self.__log__(logBuf, state + " : " + \
                                                      str(dictReportTot[state]))
			    if countNotSubmitted > 0:
                                logBuf = self.__log__(logBuf, " -not submitted: "\
                                                      + str(countNotSubmitted))

			    endedJob = dictReportTot['JobSuccess'] + \
                                       dictReportTot['JobFailed']
			    try:
			        percentage = (100 * endedJob) / numJobs
			        pathToWrite = os.path.join( str(self.args['dropBoxPath']), \
                                                            str(taskName+self.workAdd)
                                                          )

                                if os.path.exists( pathToWrite ):
                                    self.prepareReport( taskName, uuid, eMail, \
                                                        thresholdLevel, percentage, \
                                                        dictStateTot, numJobs, 1 )
                                else:
                                    logBuf = self.__log__(logBuf, "Error: The path " + pathToWrite + " does not exist!\n")

                                succexo = 0
			        if percentage != endedLevel or \
			            (percentage == 0 and status == self.taskState[3] ) or \
			            (percentage == 0 and status == self.taskState[1] ) or \
			            (notified < 2 and endedLevel == 100):

		 	            ###  updating endedLevel  ###
				    if endedLevel == 100:
                                        msg = ttdb.updatingEndedPA( mySession.bossLiteDB, \
                                                                    taskName, str(percentage), \
                                                                    self.taskState[5])
                                        logBuf = self.__log__(logBuf, msg)
                                        if notified != 2:
                                            self.taskEnded(taskName)
                                            notified = 2
                                            succexo = 1
				    elif percentage != endedLevel:
				        msg = ttdb.updatingEndedPA( mySession.bossLiteDB, \
                                                                    taskName, str(percentage), \
                                                                    status)
                                        logBuf = self.__log__(logBuf, msg)
                                        if percentage >= thresholdLevel:
					    if percentage == 100:
                                                succexo = 1
                                                self.taskEnded(taskName)
					        notified = 2
					    elif notified <= 0:
                                                succexo = 1
					        notified = 1
			        elif status == '':
                                    msg = ""
			            if numJobs > countNotSubmitted:
				        msg = ttdb.updateTaskStatus(taskName, self.taskState[3])
				    else:
				        msg = ttdb.updateTaskStatus(taskName, self.taskState[2])
                                
                                    logBuf = self.__log__(logBuf, msg)

                                self.undiscoverXmlFile(pathToWrite, taskName, \
                                                       self.tempxmlReportFile, \
                                                       self.xmlReportFileName )
                                if succexo:
                                    self.taskSuccess( os.path.join(pathToWrite, self.xmlReportFileName), taskName )
                                    _loginfo.setdefault('txt', "publishing task success (sending e-mail to %s)"%(str(eMail)))
                                    msg = ttdb.updatingNotifiedPA( taskName, notified )
                                    logBuf = self.__log__(logBuf, msg)
 			    except ZeroDivisionError, detail:
                                logBuf = self.__log__(logBuf, "WARNING: No jobs in the task " + taskName)
                                logBuf = self.__log__(logBuf, "         deatil: " + str(detail))
                                logBuf = self.__log__(logBuf, " ")
                           
            finally:
                #case with a task taken
                if task != None and len(task)>0:
                    ttdb.setTaskControlled(mySession.bossLiteDB, taskId)

                ## clean task from memory
                del task
                del taskObj

                if len(_loginfo) > 0:
                    self.__appendDbgInfo__(taskName, _loginfo)


        except Exception, ex:
            logBuf = self.__log__(logBuf, "ERROR: " + str(traceback.format_exc()))

        logging.info(logBuf)

        try:
            if not mySession.bossLiteDB is None:
                mySession.bossLiteDB.close()
            del mySession
        except:
            logging.info("not closed..")
            logging.error("ERROR: " + str(traceback.format_exc()))
            pass
        time.sleep(float(self.args['PollInterval']))

    ##########################################################################
    # start component execution
    ##########################################################################

    def startComponent(self):
        """
        _startComponent_

        Fire up the two main threads
        
        Arguments:
          none
          
        Return:
          none

        """
        # create message service instances
        self.ms = MessageService()
        self.msThread = MessageService()

        # register
        self.ms.registerAs("TaskTracking")
        self.msThread.registerAs("TaskTrackingThread")

        # subscribe to messages
        self.ms.subscribeTo("TaskTracking:StartDebug")
        self.ms.subscribeTo("TaskTracking:EndDebug")
        self.ms.subscribeTo("TaskKilled")
        self.ms.subscribeTo("TaskKilledFailed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformedPartial")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkFailed")
        self.ms.subscribeTo("TaskRegister:TaskArrival")
        self.ms.subscribeTo("CrabServerWorkerComponent:SubmitNotSucceeded")
        self.ms.subscribeTo("CrabServerWorkerComponent:TaskNotSubmitted")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewTask")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:GetOutputNotification")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:MailReference")
        ## new for logging task info
        self.ms.subscribeTo("CrabServerWorkerComponent:FatWorkerResult")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewCommand")
        self.ms.subscribeTo("KillTask")


        #reset all work_status
        ttdb = TaskStateAPI()
        ttdb.resetAllWorkStatus()

        nMaxThreads = int(self.args['Thread']) + 1
        # start polling threads
	for i in xrange(1, nMaxThreads):
	    pollingThread = PollThread(self.pollTasks, "pollingThread_" + str(i))
            pollingThread.start()

        # start message thread
        msgThread = MsgQueueExecuterThread(self.__executeQueuedMessages__)
        msgThread.start()

        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            logBuf = ""
            logBuf = self.__log__(logBuf, "GOT MESSAGE: " + str(messageType))
            logBuf = self.__log__(logBuf, " ")
            logging.info(logBuf)
            logBuf = ""

            #queue the message, instead of exetute it
            #then i commit it, then try to execute
            self.__addToQueue__(messageType, payload)
            #self.__call__(messageType, payload)
            self.ms.commit()


    def __addToQueue__(self, messageType, payload):
        semMsgQueue.acquire()
        try:
            try:
                fileName = self.__queueFileName__()
                q = PersistentQueue()
                q.loadState(fileName)
                q.put_nowait([messageType, payload])
                q.saveState(fileName)
            except Exception, ex:
                logging.error("ERROR: " + str(traceback.format_exc()))
        finally:
            semMsgQueue.release()


    def __queueFileName__(self):
        return os.path.join(self.args['ComponentDir'], "queueSerializationFile")


    def __executeQueuedMessages__(self):
        ttdb = TaskStateAPI()
        logBuf = ""
        semMsgQueue.acquire()
        try:
            logBuf = self.__log__(logBuf, "Entering in __executeQueuedMessages__ method...")
            lockedTasks = []
            fileName = self.__queueFileName__()
            q = PersistentQueue()
            q.loadState(fileName)
            #count & size are need cause the message re-queue
            count = 0
            size = q.qsize()
            while not q.empty() and count < size:
                count += 1
                item = q.get()
                logBuf = self.__log__(logBuf, "Got message: " + str(item))
                messageType = item[0]
                taskName = item[1]#payload
                #try except for catching error
                try:
                    if taskName in lockedTasks:#task was locked in before messages
                        q.put_nowait(item)
                    else:
                        #try to lock the task
                        recordAffected = ttdb.lockUnlockedTaskByTaskName(taskName)
                        if recordAffected == 0:#task is locked
                            lockedTasks.append(taskName)
                            q.put_nowait(item)
                            logBuf = self.__log__(logBuf, "Message with locked task: " + str(item))
                        else:
                            if recordAffected == 1:
                                logBuf = self.__log__(logBuf, "Locked task " + str(taskName) + " for message processing")
                            #try /finally for task unlocking
                            try:
                                self.__call__(messageType, taskName)
                                logBuf = self.__log__(logBuf, "Message executed: " + str(item))
                            finally:
                                if recordAffected == 1:
                                    ris = ttdb.unlockTaskByTaskName(taskName)
                                    if ris == 1:
                                        logBuf = self.__log__(logBuf, "Unlocked task " + str(taskName) + " for message processing")
                                    if ris != 1:
                                        logBuf = self.__log__(logBuf, "Unexcepted return value for unlock(..)=" + str(ris))
                except Exception, ex:
                    logBuf = self.__log__(logBuf, str("ERROR: " + str(traceback.format_exc())))

            logBuf = self.__log__(logBuf, "... __executeQueuedMessages__ method done.")
            #save queue state
            q.saveState(fileName)
        finally:
            semMsgQueue.release()
            logging.info(logBuf)


##############################################################################
# PollDBS class
##############################################################################
class PollThread(Thread):
    """
    Thread that performs task polling 
    """
    ##########################################################################
    # thread initialization
    ##########################################################################
    #added name parameter for self.name field class
    #that specific a thread name
    def __init__(self, poll, name):
        """
        __init__
        Initialize thread and set polling callback
        Arguments:
          poll -- the task polling function
        """
        Thread.__init__(self)
        self.poll = poll
        self.name = name

    ##########################################################################
    # thread main body
    ##########################################################################
    def run(self):
        """
        __run__
        Performs polling on task database
        Arguments:
          none
        Return:
          none
        """
        # at most three consecutive failed attempts to run the polling thread
        failedAttempts = 0
        # performs DBS polling indefinitely
        while True:
            # perform dataset polling
            try:
                #pass thread name to poll method ^^
                self.poll(self.name)
            except Exception, ex:
                # log error message
                logging.error("ERROR in polling thread " + self.name + "\n" + str(traceback.format_exc()))
                # try at most 3 times
                if failedAttempts == 3:
                    logging.error("Cannot restart polling. Aborting")
                    logging.error("\nWarning: Polling thread is not running!")
                    sys.exit(1)
                # increment failure counter
                failedAttempts += 1

                logging.error("Trying to restart polling... " + \
                              "(attempt %s)" % failedAttempts)

            # no errors, reset failure counter
            else:
                failedAttempts = 0

##########################################
#### serializable queue class
##########################################
class PersistentQueue(Queue.Queue):
    def __init__(self, maxsize=0):
        Queue.Queue.__init__(self, maxsize)
      
    def saveState(self, filePath):
        f = open(filePath, 'w')
        l = []
        while not self.empty():
            l.append(self.get())
        cPickle.dump(l, f)
        f.close()

    def loadState(self, filePath):
        if os.path.exists( filePath ):
            f = open(filePath)
            l = cPickle.load(f)
            f.close()
            for i in l:
                self.put(i)


##########################################
#### message queue executer thread class
##########################################
class MsgQueueExecuterThread(Thread):
    """
    Thread that performs the message queue execute 
    """
    ##########################################################################
    # thread initialization
    ##########################################################################
    def __init__(self, msgExecuterMethod):
        """i
        __init__
        Initialize thread and set polling callback
        Arguments:
          msgExecuterMethod: method that do the message execution from the persistent queue
        """
        Thread.__init__(self)
        self.msgExecuterMethod = msgExecuterMethod

    ##########################################################################
    # thread main body
    ##########################################################################
    def run(self):
        """
        __run__
        Arguments:
          none
        Return:
          none
        """
        while True:
            try:
                self.msgExecuterMethod()
                time.sleep(5)
            except Exception, ex:
                logging.error("ERROR in MsgQueueExecuterThread \n" + str(traceback.format_exc()))

