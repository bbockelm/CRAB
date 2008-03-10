#!/usr/bin/env python
"""
_TaskTracking_

"""

__revision__ = "$Id: TaskTrackingComponent.py,v 1.54 2008/01/10 09:10:15 mcinquil Exp $"
__version__ = "$Revision: 1.54 $"

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
import  ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Core.ProdException import ProdException
from ProdAgentCore.Configuration import ProdAgentConfiguration

# DB PA
import TaskStateAPI

# XML
from CrabServer.CreateXmlJobReport import * 

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
semvar = BoundedSemaphore(maxnum) #for synchronisation between thread 
semfile = BoundedSemaphore(maxnum) #for synchronisation between thread 
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
        self.crabcfg = "crab.cfg"
	self.resSubDir = "" #"res/"
        self.workAdd = "_spec/"
        self.xmlReportFileName = "xmlReportFile.xml"
        self.tempxmlReportFile = ".tempxmlReportFileName"
        self.mutex = Condition()

	#
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

    ##########################################################################
    # handle events
    ##########################################################################
    
    def __appendDbgInfo__( self, taskName, message ):
        """
        _appendDbgInfo_
        
        update debug informations about task processing
        """
        #"""
        from InternalLoggingInfo import InternalLoggingInfo

        dbgInfo = InternalLoggingInfo()
        path2Wr = str(self.args['dropBoxPath']) + "/" + taskName + self.workAdd + "/" + self.resSubDir
        #logging.info("Appending: \n\n" + message + "\n\n")
        dbgInfo.appendLoggingInfo( path2Wr, "***"+str(time.asctime())+"***\n" + message )
        del dbgInfo
        #"""
        #pass


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

        # new task to insert
	#if event == "DropBoxGuardianComponent:NewFile":
        if event == "CRAB_Cmd_Mgr:NewTask":
            logging.info( event )
            logging.info( payload )
	    if payload != None or payload != "" or len(payload) > 0:
                #taskName = payload.split("::")[2]
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, "NewTask: %s" % taskName)
		logBuf = self.__logToBuf__(logBuf, taskName)
		self.insertNewTask( taskName )
                logBuf = self.__logToBuf__(logBuf, "               new task inserted.")
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: wrong payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
            logging.info(logBuf)
            return None
	    
	if event == "CrabServerWorkerComponent:TaskArrival":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                ## start dbg info ##
                OK += "  Task ["+ taskName +"] ready to be submitted and already in queue.\n"
                ## end dbg info ##
                logBuf = self.__logToBuf__(logBuf, "Submitting Task: %s" % str(taskName) )
		self.updateTaskStatus( taskName, self.taskState[1] )
                logBuf = self.__logToBuf__(logBuf, "              task updated.")
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+taskName+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:CrabWorkPerformed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, "CrabWorkPerformed: %s" % payload)
                ## start dbg info ##
                OK += "  Task ["+ taskName +"] succesfully submitted to the grid.\n"
                ## end dbg info ##
		self.updateTaskStatus( taskName, self.taskState[3])
                logBuf = self.__logToBuf__(logBuf, "               task updated.")
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+taskName+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:CrabWorkFailed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, "CrabWorkFailed: %s" % payload)
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                ## start dbg info ##
                OK += "  Task ["+taskName+"] not submitted to the grid (devel-note: be more verbose).\n"
                ## end dbg info ##
		self.updateTaskStatus(taskName, self.taskState[2])
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:SubmitNotSucceeded" or \
            event == "CrabServerWorkerComponent:TaskNotSubmitted":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, str(event.split(":")[1]) + ": %s" % payload)
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                taskName, taskStatus, reason = payload.split("::")
                ## start dbg info ##
                OK += "  Task ["+taskName+"] not submitted to the grid\n"+ \
                      "        status = " + str(taskStatus) + \
                      "        reason = " + str(reason)
                ## end dbg info ##
                self.updateTaskStatus(taskName, self.taskState[2])
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)


        if event == "CrabServerWorkerComponent:CrabWorkPerformedPartial":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, event + ": %s" % payload)
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
#                taskName = str(payload.split("::", 1)[0])
                ## start dbg info ##
                OK += "  Task ["+taskName+"] submitted to the grid.\n"
#                OK += "    -> WARNING: couldn't submit jobs "+str(eval(payload.split("::")[2]))+".\n"
                ## end dbg info ##
                #self.preUpdatePartialTask(payload, self.taskState[7])
                self.updateTaskStatus(taskName, self.taskState[7])
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:FastKill":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, event + ": " + str(payload) )
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                ## start dbg info ##
                OK += "  FastKill: task ["+payload+"] killed before the submission to the grid.\n"
                ## end dbg info ##
                self.updateTaskStatus(payload, self.taskState[4])
#                self.updateTaskStatus(payload, self.taskState[5])
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "TaskKilled":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                rangeKillJobs = "all"
                if payload.find("::") != -1:
                    taskName, rangeKillJobs = payload.split("::")
                logBuf = self.__logToBuf__(logBuf, "   Killed task: %s" % taskName)
                if rangeKillJobs == "all":
                    self.updateTaskKilled( taskName, self.taskState[4] )
                else:
                    self.updateTaskKilled( taskName, self.taskState[8] )
                ## start dbg info ##
                OK += "  Task ["+str(payload.split("::")[0])+"] killed (jobs killed: "+str(rangeKillJobs)+").\n"
                ## end dbg info ##
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "TaskKilledFailed":
            if payload != None or payload != "" or len(payload) > 0: 
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                rangeKillJobs = "all"
                if payload.find("::") != -1:
                    taskName, rangeKillJobs = payload.split("::")
                logBuf = self.__logToBuf__(logBuf, "   Error killing task: %s" % taskName)
                if rangeKillJobs == "all":
                    self.killTaskFailed( taskName )
                else:
                    self.killTaskFailed( taskName )
                ## start dbg info ##
                OK += "  WARNING: task ["+str(payload.split("::")[0])+"] failed to kill (jobs to be killed: "+str(rangeKillJobs)+").\n"
                ## end dbg info ##
                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf, " ")
                logBuf = self.__logToBuf__(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf, " ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        
        # start debug event
        if event == "TaskTracking:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return None

        # stop debug event
        if event == "TaskTracking:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return None

        # wrong event
        logBuf = self.__logToBuf__(logBuf, " ")
        logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
        logBuf = self.__logToBuf__(logBuf, "Unexpected event %s, ignored" % event)
        logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
        logBuf = self.__logToBuf__(logBuf, " ")
        logging.info(logBuf)

        return None

    ##########################################################################
    # read task config file
    ##########################################################################


    def readInfoCfg(self, path):
        """
        _readInfoCfg_

        read informations from the config files
        """
        #pathFile = path + "/share/" + self.crabcfg
        eMail = None
        thresholdLevel = None
        #if os.path.exists( pathFile ):
        #    eMail = self.parseCrabCfg(open(pathFile).read(), "USER", "eMail")
        #    thresholdLevel = self.parseCrabCfg(open(pathFile).read(), "USER", "thresholdLevel")
        return eMail, thresholdLevel


    ##########################################################################
    # insert and update task in database
    ##########################################################################


    def insertNewTask( self, payload ):
        """
        _insertNewTask_
        """
        logBuf = ""
        try:
            TaskStateAPI.insertTaskPA( payload, self.taskState[0] )
        except Exception, ex:
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while inserting the task " + str(payload) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
        logging.info(logBuf)


    def updateInfoTask( self, payload ):
        """
        _updateInfoTask_

        updating a task that is just sumitted
        """
        logBuf = ""
        uuid, taskName, proxy = payload.split(":", 3)
	
        eMail, thresholdLevel = self.readInfoCfg( self.args['dropBoxPath'] + "/" + taskName )
        if eMail == None:
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR: missing 'eMail' from " + self.crabcfg + " for task: " + str(taskName) )
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            eMail = "mattia.cinquilli@pg.infn.it"  #### TEMPORARY SOLUTION!
        if thresholdLevel == None:
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "WARNING: missing 'thresholdLevel' from " + self.crabcfg + " for task: " + str(taskName) )
            logBuf = self.__logToBuf__(logBuf, "         using default value 'thresholdLevel = 100'")
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            thresholdLevel = 100
        elif int(thresholdLevel) < 0:
            thresholdLevel = 0
        elif int(thresholdLevel) > 100:
            thresholdLevel = 100
        dictionaryReport =  {"all": ["Submitting", "", "", 0]} 
        self.prepareReport( taskName, uuid, eMail, thresholdLevel, 0, dictionaryReport, 0, 0 )

        try:
            TaskStateAPI.updateNotSubmitted( taskName, eMail, thresholdLevel, proxy, uuid, self.taskState[6] )
        except Exception, ex:
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while updating the task " + str(taskName) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
        logging.info(logBuf)


    def preUpdatePartialTask( self, payload, status ):
        """
        _preUpdatePartialTask_

        split the payload-sendes email(updates status)
        """
        logBuf = ""

        fields = payload.split("::")
        taskName = fields[0]
        totJobs = int(fields[1])
        pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + self.workAdd + "/" + self.resSubDir
        jobList = []
        if status == self.taskState[7]:
            jobList = eval( fields[2] )
            self.addJobsToFile( pathToWrite, jobList )#, totJobs)

        ## call function that updates DB
        self.updateTaskStatus(taskName, status)
        
        uuid = ""
        eMail = ""
        valuess = TaskStateAPI.getStatusUUIDEmail( taskName )
        if valuess != None:
            #status = valuess[0]
            if len(valuess) > 1:
                uuid = valuess[1]
                if len(valuess) > 2:
                    eMail = valuess[2]
            ## XML report file
            dictionaryReport =  {}
            for jobId in range( 1, totJobs + 1 ):
                vect = ["Submitted", "", "", 0]
                dictionaryReport.setdefault(jobId, vect)
            try:
                for jobId in jobList:
                    dictionaryReport[jobId+1][0] = "NotSubmitted"
        #        self.addJobsToFile()
            except Exception, ex:
                logBuf = self.__logToBuf__(logBuf, str(ex) )
                logBuf = self.__logToBuf__(logBuf, str(ex.args) )
            #            logBuf = self.__logToBuf__(logBuf, str(dictionaryReport))
            self.prepareReport( taskName, uuid, eMail, 0, 0, dictionaryReport, 0, 0 )
            """
            ## MAIL report user
            self.prepareTaskFailed( payload, uuid, eMail, status )
            """
            logging.info(logBuf)


    def addJobsToFile(self, taskPath, jobList):#, totJobs):
        """
        _addJobToFile_
        """
        fileName = "/.notSubmitted.TT"
        file(taskPath + fileName, 'w').write(str(jobList))#+"::"+str(totJobs))

    def getJobFromFile(self, taskPath):
        """
        _getJobFromFile_
        """
        fileName = "/.notSubmitted.TT"
        if os.path.exists( taskPath + fileName ):
            return eval( file(taskPath + fileName, 'r').read() )#.split("::")
        return []

    
    def updateTaskStatus(self, payload, status):
        """
	_updateTaskStatus_

	 update the status of a task
        """
        logBuf = ""	
        try:
            TaskStateAPI.updateStatus( payload, status )
        except Exception, ex:
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while updating the task " + str(payload) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logging.info(logBuf)
            logBuf = ""

	eMail = ""
	uuid = ""

        try:
	    if status == self.taskState[2] or status == self.taskState[4]:
	        valuess = TaskStateAPI.getStatusUUIDEmail( payload )
		if valuess != None:
		    status = valuess[0]
		    if len(valuess) > 1:
		        uuid = valuess[1]
		        if len(valuess) > 2:
		    	    eMail = valuess[2]
                    if status == self.taskState[2]:
	                ## XML report file
                        dictionaryReport =  {"all": ["NotSubmitted", "", "", 0]}
                        self.prepareReport( payload, uuid, eMail, 0, 0, dictionaryReport, 0, 0 )
                        ## MAIL report user
                        #self.prepareTaskFailed( payload, uuid, eMail, status )
                    else:
                        ## XML report file
                        dictionaryReport =  {"all": ["Killed", "", "", 0]}
                        self.prepareReport( payload, uuid, eMail, 0, 0, dictionaryReport, 0, 0 )
		        ## MAIL report user
                        #self.taskFastKill( self.args['dropBoxPath'] + "/" + payload  + "/res/" + self.xmlReportFileName, payload )
        except Exception, ex:
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while reporting info about the task " + str(payload) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
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
	TaskStateAPI.updatingNotifiedPA( taskName, 2 )
        if status == self.taskState[2]:
            self.taskNotSubmitted( self.args['dropBoxPath'] + "/" + taskName + self.workAdd + self.resSubDir + self.xmlReportFileName, taskName )
        elif status == self.taskState[7]:
            self.taskIncompleteSubmission(origTaskName, strEmail[0:len(strEmail)-1], userName)
        else:
            self.taskFailed(origTaskName, strEmail[0:len(strEmail)-1], userName)
	 

    def updateTaskKilled ( self, taskName, status ):
        """
        _updateTaskKilled_
        """
        logBuf = ""
        try:
            TaskStateAPI.updateStatus( taskName, status )
        except Exception, ex:
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while updating the task " + str(taskName) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
            logging.info(logBuf)

    def killTaskFailed (self, taskName ):
        """
        _killTaskFailed_
        """
        logBuf = ""
        logBuf = self.__logToBuf__(logBuf, "Error killing task: " + taskName )
        logging.info(logBuf)


    ##########################################################################
    # utilities
    ##########################################################################

    def convertStatus( self, status ):
        """
        _convertStatus_
        """
        stateConverting = {'R': 'Running','SA': 'Aborted','SD': 'Done','SE': 'Done','E': 'Done','SK': 'Cancelled','SR': 'Ready','SU': 'Submitted','SS': 'Scheduled','UN': 'Unknown','SW': 'Waiting','W': 'Submitting', 'K': 'Killed', 'S': 'Submitted', 'DA': 'Done (Failed)', 'NotSubmitted': 'NotSubmitted'}
        if status in stateConverting:
            return stateConverting[status]
        return 'Unknown'


    def prepareReport( self, taskName, uuid, eMail, thresholdLevel, percentage, dictReportTot, nJobs, flag ):
        """
        _prepareReport_
        """
        pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + self.workAdd + "/" + self.resSubDir

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
                for index in range(len(eMaiList)):
                    if index != 0:
                        c.addEmailAddress( eMaiList[index] )
                    else:
                        c.initialize( origTaskName, eMaiList[0], userName, percentage, thresholdLevel, nJobs)

            for singleJob in dictReportTot:
                J = Job()   ##    id             status                        eec
                J.initialize( singleJob, dictReportTot[singleJob][0], dictReportTot[singleJob][2], \
                            ##         jes                       clear                       Resub
                              dictReportTot[singleJob][1], dictReportTot[singleJob][3], self.getListEl(dictReportTot[singleJob], 4), \
                            ##         site
                              self.getListEl(dictReportTot[singleJob], 5) )
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
            for index in range(len(eMaiList)):
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

            logBuf = self.__logToBuf__(logBuf, "-------> Published 'TaskEnded' message with payload: %s" % taskName)
        logging.info(logBuf)


    def taskSuccess( self, taskPath, taskName ):
        """
        _taskSuccess_
        
        Trasmit the "TaskSuccess" event to the prodAgent
       kEnded
        """
        logBuf = ""
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskSuccess", taskPath)
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-*")
        logBuf = self.__logToBuf__(logBuf, "-------> Published 'TaskSuccess' message with payload: %s" % taskPath)
        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-*")
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

        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-* ")
        logBuf = self.__logToBuf__(logBuf, "Published 'TaskFailed' message with payload: %s" % payload)
        self.taskEnded(taskName)
        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-* ")
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

        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-* ")
        logBuf = self.__logToBuf__(logBuf, "Published 'TaskNotSubmitted' message with payload: %s" % taskPath)
        self.taskEnded(taskName)
        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-* ")
        logging.info(logBuf)


    def taskFastKill( self, taskPath, taskName ):
        """
        _taskFastKill_
        """
        logBuf = ""
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskFastKill", taskPath)
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-* ")
        logBuf = self.__logToBuf__(logBuf, "Published 'TaskFastKill' message with payload: %s" % taskPath)
        self.taskEnded(taskName)
        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-* ")
        logging.info(logBuf)
  

    def taskIncompleteSubmission( self, taskName, eMaiList, userName ):
        """
        _taskIncompleteSubmission_
        """
        logBuf = ""
        if userName == "" or userName == None:
            userName = "Unknown"
        payload = taskName + ":" + userName + ":" + eMaiList
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskIncompleteSubmission", payload)
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-* ")
        logBuf = self.__logToBuf__(logBuf, "Published 'TaskIncompleteSubmission' message with payload: %s" % payload)
        logBuf = self.__logToBuf__(logBuf, "         *-*-*-*-* ")
        logging.info(logBuf)


    def __logToBuf__(self, buf, strToAppend):
        """
        __logToBug__
        input:
        - buf: the string buffer
        - strToAppend: the string to appent to the buffer
        output:
        - the buffer with a new row like [datetime] - [str]
        this is an helper method for poolTasks one.
        """
        #avoid to log empty message
        if strToAppend == None or strToAppend == "":
            bufRet = str(buf)
        else:
            bufRet = str(buf) + "\n" + str(datetime.datetime.now()) + " - " + str(strToAppend)
        #logging.info(strToAppend)
        return bufRet


    ##########################################################################
    # polling js_taskInstance and BOSS DB
    ##########################################################################

    def pollTasks(self, threadName):
        """
        _pollTasks_

        Poll the task database
        @note: user __logToBuf__ function utility for a cross-thread-readable logging ^^
         """
        logBuf = ""

        task = None
        try:
            ## bossLite session
            mySession = None
            try:
                mySession = BossLiteAPI("MySQL", self.bossCfgDB)
            except ProdException, ex:
                logging.info(str(ex))
                return 0

            #logging.info("connected...")
            ## task from DB
            task = TaskStateAPI.getNLockFirstNotFinished()
            try:
                taskId = 0
                if task == None or len(task) <= 0:
                    TaskStateAPI.resetControlledTasks()
                else:
                    taskId = task[0][0]
                    taskName = task[0][1]
		    eMail = task[0][2]
		    notified = int(task[0][4])
		    thresholdLevel = task[0][3]
		    endedLevel = task[0][5]
		    status = task[0][6]
		    uuid = task[0][7]

                    #logBuf = self.__logToBuf__(logBuf, "Got Task(id, name): (" + str(taskId) + ", " + str(taskName) + ")")

                    if status == self.taskState[2] and notified < 2:
                        ######### Taskfailed is prepared now
                        self.prepareTaskFailed( taskName, uuid, eMail, status)
		    else:
                        ## lite task load in memory
                        taskObj = None
                        try:
                            taskObj = mySession.loadTaskByName( taskName )
                        except TaskError, te:
                            taskObj = None
                            pass 
                        if taskObj is None:
                            logBuf = self.__logToBuf__(logBuf, "Unable to retrieve task [%s]. Causes: loadTaskByName"%(taskName))
                            logBuf = self.__logToBuf__(logBuf,"  Requested task [%s] does not exist."%(taskName) )
                        else:
                            logBuf = self.__logToBuf__(logBuf, " - - - - - - - ")
                            logBuf = self.__logToBuf__(logBuf, " *" + taskName + "*:")

			    pathToWrite = ""
			    dictReportTot = {'JobSuccess': 0, 'JobFailed': 0, 'JobInProgress': 0}
			    countNotSubmitted = 0 
			    dictStateTot = {}
                            numJobs = len(taskObj.jobs)
                            
                            for jobbe in taskObj.jobs:
                                try:
                                    mySession.getRunningInstance(jobbe)
                                except JobError, ex:
                                    logging.error('Problem loading job running info')
                                    break
                                job   = jobbe.runningJob['jobId']
                                stato = jobbe.runningJob['status']
                                sId   = jobbe.runningJob['schedulerId']
                                jec   = jobbe.runningJob['wrapperReturnCode']
                                eec   = jobbe.runningJob['applicationReturnCode']
                                site  = ""
                                if jobbe.runningJob['destination'] != None:
                                    site  = jobbe.runningJob['destination'].split("://")[1].split("/")[0]
 
                                try:
                                    self.mutex.acquire()
                                    resubmitting, MaxResub, Resub = TaskStateAPI.checkNSubmit( taskName, job )
                                finally:
                                    self.mutex.notifyAll()
                                    self.mutex.release()

			        vect = []
			        if eec == "NULL" and jec == "NULL":
   			            vect = [self.convertStatus(stato), "", "", 0, Resub, site]
			        else:
                                    vect = [self.convertStatus(stato), eec, jec, 0, Resub, site]
                                dictStateTot.setdefault(job, vect)
 
#                                jobPartList = self.getJobFromFile(str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir)

			        if stato == "SE" or stato == "E":
				    if (eec == "0" or eec == "" or eec == "NULL") and jec == "0":
				        dictReportTot['JobSuccess'] += 1
				        dictStateTot[job][3] = 1
				    elif not resubmitting:
				        dictReportTot['JobFailed'] += 1
                                        dictStateTot[job][0] = "Done (Failed)"
                                        dictStateTot[job][3] = 1
				    else:
				        dictReportTot['JobInProgress'] += 1
                                        dictStateTot[job][0] = "Resubmitting by server"
			        elif stato == "SA" or stato == "SK" or stato == "K":
				    if not resubmitting:
				        dictReportTot['JobFailed'] += 1
				    else:
				        dictReportTot['JobInProgress'] += 1
                                        dictStateTot[job][0] = "Resubmitting by server"
			        elif stato == "W":
                                    if not resubmitting:
   				        countNotSubmitted += 1 
				        dictReportTot['JobFailed'] += 1
                                        if status == self.taskState[4]:
                                            dictStateTot[job][0] = "Killed"
                                        else:
                                            dictStateTot[job][0] = "NotSubmitted"
#                                    elif int(job) in jobPartList:
#                                        countNotSubmitted += 1
#                                        dictReportTot['JobFailed'] += 1
##                                        if status == self.taskState[9]:
##                                            dictStateTot[job][0] = "Created"
##                                        else:
#                                        dictStateTot[job][0] = "NotSubmitted"
                                    elif status == self.taskState[4]:
                                        #countNotSubmitted += 1   
                                        dictReportTot['JobFailed'] += 1
                                        dictStateTot[job][0] = "Killed"
                                    elif status == self.taskState[9]:
                                        dictStateTot[job][0] = "Created"
                                        countNotSubmitted += 1
                                        dictReportTot['JobInProgress'] += 1
                                    else:
                                        countNotSubmitted += 1
                                        dictReportTot['JobInProgress'] += 1
                                elif not resubmitting:
                                    if status == self.taskState[4]:
                                        dictReportTot['JobInProgress'] += 1
                                    else:
                                        dictReportTot['JobInProgress'] += 1
                               #elif stato != "K": ## ridondante
                                    #dictReportTot['JobFailed'] += 1
                                    #else:
                                    #    dictReportTot['JobFailed'] += 1
                                else:
                                    dictReportTot['JobInProgress'] += 1

			    rev_items = [(v, int(k)) for k, v in dictStateTot.items()]
			    rev_items.sort()
			    dictStateTot = {}
			    for valu3, k3y in rev_items:
			        dictStateTot.setdefault( k3y, valu3 )
                            del rev_items

			    for state in dictReportTot:
                                logBuf = self.__logToBuf__(logBuf, " Job " + state + ": " + str(dictReportTot[state]))
			    if countNotSubmitted > 0:
                                logBuf = self.__logToBuf__(logBuf, "    -> of which not yet submitted: " + str(countNotSubmitted))

			    endedJob = dictReportTot['JobSuccess'] + dictReportTot['JobFailed']

			    try:
			        percentage = (100 * endedJob) / numJobs
			        pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + self.workAdd + "/" + self.resSubDir

                                if os.path.exists( pathToWrite ):
                                    self.prepareReport( taskName, uuid, eMail, thresholdLevel, percentage, dictStateTot, numJobs, 1 )
                                else:
                                    logBuf = self.__logToBuf__(logBuf, "Error: the path " + pathToWrite + " does not exist!\n")
                                succexo = 0
			        if percentage != endedLevel or \
			            (percentage == 0 and status == self.taskState[3] ) or \
			            (percentage == 0 and status == self.taskState[1] ) or \
			            (notified < 2 and endedLevel == 100):

		 	            ###  updating endedLevel  ###
				    if endedLevel == 100:
                                        msg = TaskStateAPI.updatingEndedPA( taskName, str(percentage), self.taskState[5])
                                        logBuf = self.__logToBuf__(logBuf, msg)
                                        if notified != 2:
                                            self.taskEnded(taskName)
                                            notified = 2
                                            succexo = 1
                                            logBuf = self.__logToBuf__(logBuf, msg)
				    elif percentage != endedLevel:
				        msg = TaskStateAPI.updatingEndedPA( taskName, str(percentage), status)
                                        logBuf = self.__logToBuf__(logBuf, msg)
                                        if percentage >= thresholdLevel:
					    if percentage == 100:
                                                succexo = 1
                                                self.taskEnded(taskName)
					        notified = 2
                                                logBuf = self.__logToBuf__(logBuf, msg)
					    elif notified <= 0:
                                                succexo = 1
					        notified = 1
                                                logBuf = self.__logToBuf__(logBuf, msg)
			        elif status == '':
                                    msg = ""
			            if dictReportTot['JobSuccess'] + dictReportTot['JobFailed'] + dictReportTot['JobInProgress'] > countNotSubmitted:
				        msg = TaskStateAPI.updateTaskStatus( taskName, self.taskState[3] )
				    else:
				        msg = TaskStateAPI.updateTaskStatus( taskName, self.taskState[2] )
                                
                                    logBuf = self.__logToBuf__(logBuf, msg)

                                self.undiscoverXmlFile( pathToWrite, taskName, self.tempxmlReportFile, self.xmlReportFileName )
                                if succexo:
                                    self.taskSuccess( pathToWrite + self.xmlReportFileName, taskName )
                                    msg = TaskStateAPI.updatingNotifiedPA( taskName, notified )
                                    logBuf = self.__logToBuf__(logBuf, msg)
 			    except ZeroDivisionError, detail:
                                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                                logBuf = self.__logToBuf__(logBuf, "WARNING: No jobs in the task " + taskName)
                                logBuf = self.__logToBuf__(logBuf, "         deatil: " + str(detail))
                                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                           
                        ##clear tasks from memory
                        del mySession
            finally:
                #case with a task taken
                if task != None and len(task)>0:
                    TaskStateAPI.setTaskControlled(taskId)
                    #logBuf = self.__logToBuf__(logBuf, "Task(id, name): (" + str(taskId) + ", " + str(taskName) + ") set to Controlled.")

        except Exception, ex:
            logBuf = self.__logToBuf__(logBuf, "ERROR: " + str(traceback.format_exc()))

        logging.info(logBuf)

        time.sleep(float(self.args['PollInterval']))


    def printDictKey( self, diction, value ):
        """
        _printDictKey_
        """
        msg = "Ended jobs: * "
        try:
            for valu3, k3y in diction.iteritems():
#                logging.info( str(valu3) + " " + str(k3y) )
                if str(valu3) == int(value) or\
                   int(valu3) == int(value) or\
                   valu3 == value:
                    msg += str(k3y) + " * "
        except Exception, ex:
            logging.error(str(ex))
        return msg
                

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
        self.ms.subscribeTo("SetThresholdLevel")
        #self.ms.subscribeTo("CrabServerWorkerComponent:FastKill")
        self.ms.subscribeTo("TaskKilled")
        self.ms.subscribeTo("TaskKilledFailed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformedPartial")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkFailed")
        self.ms.subscribeTo("CrabServerWorkerComponent:TaskArrival")
        self.ms.subscribeTo("CrabServerWorkerComponent:SubmitNotSucceeded")
        self.ms.subscribeTo("CrabServerWorkerComponent:TaskNotSubmitted")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewTask")

        #reset all work_status
        TaskStateAPI.resetAllWorkStatus()

        nMaxThreads = int(self.args['Thread']) + 1
        # start polling threads
	for i in range(1, nMaxThreads):
	    pollingThread = PollThread(self.pollTasks, "pollingThread_" + str(i))
            pollingThread.start()

        # start message thread
        msgThread = MsgQueueExecuterThread(self.__executeQueuedMessages__)
        msgThread.start()

        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            logBuf = ""
            logBuf = self.__logToBuf__(logBuf, "\n")
            logBuf = self.__logToBuf__(logBuf, "GOT MESSAGE: " + str(messageType))
            logBuf = self.__logToBuf__(logBuf, "\n")
            logging.info(logBuf)
            logBuf = ""

            #queue the message, instead of exetute it
            #then i commit it, then try to execut

            #queue the message, instead of exetute it
            #then i commit it, then try to executee
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
        logBuf = ""
        semMsgQueue.acquire()
        try:
            logBuf = self.__logToBuf__(logBuf, "Entering in __executeQueuedMessages__ method...")
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
                logBuf = self.__logToBuf__(logBuf, "Got message: " + str(item))
                messageType = item[0]
                taskName = item[1]#payload
                #try except for catching error
                try:
                    if taskName in lockedTasks:#task was locked in before messages
                        q.put_nowait(item)
                    else:
                        #try to lock the task
                        recordAffected = TaskStateAPI.lockUnlockedTaskByTaskName(taskName)
                        if recordAffected == 0:#task is locked
                            lockedTasks.append(taskName)
                            q.put_nowait(item)
                            logBuf = self.__logToBuf__(logBuf, "Message with locked task: " + str(item))
                        else:
                            if recordAffected == 1:
                                logBuf = self.__logToBuf__(logBuf, "Locked task " + str(taskName) + " for message processing")
                            #try /finally for task unlocking
                            try:
                                self.__call__(messageType, taskName)
                                logBuf = self.__logToBuf__(logBuf, "Message executed: " + str(item))
                            finally:
                                if recordAffected == 1:
                                    ris = TaskStateAPI.unlockTaskByTaskName(taskName)
                                    if ris == 1:
                                        logBuf = self.__logToBuf__(logBuf, "Unlocked task " + str(taskName) + " for message processing")
                                    if ris != 1:
                                        logBuf = self.__logToBuf__(logBuf, "Unexcepted return value for unlock(..)=" + str(ris))
                except Exception, ex:
                    logBuf = self.__logToBuf__(logBuf, str("ERROR: " + str(traceback.format_exc())))

            logBuf = self.__logToBuf__(logBuf, "... __executeQueuedMessages__ method done.")
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
                time.sleep(7)
            except Exception, ex:
                # log error message
                # new exception(detailed) logging
                logging.error("ERROR in MsgQueueExecuterThread \n" + str(traceback.format_exc()))

