#!/usr/bin/env python
"""
_TaskTracking_

"""

__revision__ = "$Id: TaskTrackingComponent.py,v 1.35 2007/07/25 15:42:44 mcinquil Exp $"
__version__ = "$Revision: 1.35 $"

import os
import time
import datetime
import sys
import re

# BOSS API import
from BossSession import *

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
from ProdAgentCore.Configuration import ProdAgentConfiguration

# DB PA
import TaskStateAPI

# XML
from CrabServer.CreateXmlJobReport import * 

from Outputting import *

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
newstate={} #communication between thread
semvar = BoundedSemaphore(maxnum) #for synchronisation between thread 
semfile = BoundedSemaphore(maxnum) #for synchronisation between thread 
semMsgQueue = BoundedSemaphore(maxnum) #for synchronisation between thread for the msgQueue 
####hassen will move all var to init for avoiding changement of var value by instance of this class  

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
        self.args.setdefault("PollInterval", 35 )
        self.args.setdefault("Logfile", None)
        self.args.setdefault("bossClads", None)
        self.args.setdefault("dropBoxPath", None)
        self.args.setdefault("Thread", 5)
	self.args.setdefault("jobDetail", "nop")
        
        # update parameters
        self.args.update(args)
        logging.info("Using "+str(self.args['dropBoxPath'])+" as DropBox")
        logging.info("Using "+str(self.args['bossClads'])+" for BOSS configuration")

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
	self.resSubDir = "res/"
        self.xmlReportFileName = "xmlReportFile.xml"
        self.tempxmlReportFile = ".tempxmlReportFileName"

	#
	self.taskState = [\
                          "arrived", "submitting", "not submitted", "submitted",\
                          "killed", "ended", "unpacked", "partially submitted",\
                          "partially killed", "range submitted"\
                         ]

        self.mutex = Condition()

        #self.maxnum = 1
        #self.newstate={}communication between thread
        #self.semvar = BoundedSemaphore(maxnum)for synchronisation between thread
        #self.semfile = BoundedSemaphore(maxnum)for synchronisation between thread
        #self.semMsgQueue = BoundedSemaphore(maxnum)for synchronisation between thread for the msgQueue
 
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
        path2Wr = str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir
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
	if event == "DropBoxGuardianComponent:NewFile":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf,"NewTask: %s" % payload)
		logBuf = self.__logToBuf__(logBuf,taskName)
		self.insertNewTask( taskName )
                logBuf = self.__logToBuf__(logBuf,"               new task inserted.")
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
            logging.info(logBuf)
            return None
	    
	if event == "ProxyTarballAssociatorComponent:WorkDone":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf,"Task Ready to be submitted: %s" % payload)
                taskName = str(payload.split(":")[1])
                ## start dbg info ##
                OK += "  Task ["+taskName+"] succesfully unpacked and associated with the right proxy.\n"
                ## end dbg info ##
		self.updateInfoTask( payload )
                logBuf = self.__logToBuf__(logBuf,"              task updated.")
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

	if event == "ProxyTarballAssociatorComponent:UnableToManage":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf,"Problem with the project: %s" % payload)
                ## start dbg info ##
                OK += "  Problems unpacking (or associating to a proxy) the task ["+payload+"].\n"
                ## end dbg info ##
		try:
		    TaskStateAPI.updateNotSubmitted( payload, "", "", "", "", self.taskState[2] )
		except Exception, ex:
		    logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
		    logBuf = self.__logToBuf__(logBuf,"ERROR while updating the task " + str(payload) )
		    logBuf = self.__logToBuf__(logBuf,"      "+str(ex))
		    logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf,"              task updated.")
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                ## start dbg info ##
                OK += "  The task ["+payload+"] has been archived on the server db.\n"
                ## end dbg info ##
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

	if event == "CrabServerWorkerComponent:TaskArrival":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                ## start dbg info ##
                OK += "  Task ["+payload.split(":", 1)[0]+"] ready to be submitted and already in queue.\n"
                ## end dbg info ##
                taskName = str(payload.split(":", 1)[0])
                logBuf = self.__logToBuf__(logBuf,"Submitting Task: %s" % str(taskName) )
		self.updateTaskStatus( taskName, self.taskState[1] )
                logBuf = self.__logToBuf__(logBuf,"              task updated.")
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+tName+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)


        if event == "CrabServerWorkerComponent:CrabWorkPerformed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf,"CrabWorkPerformed: %s" % payload)
                ## start dbg info ##
                OK += "  Task ["+payload+"] succesfully submitted to the grid.\n"
                ## end dbg info ##
		self.updateTaskStatus(payload, self.taskState[3])
                logBuf = self.__logToBuf__(logBuf,"               task updated.")
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:CrabWorkFailed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf,"CrabWorkFailed: %s" % payload)
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                ## start dbg info ##
                OK += "  Task ["+payload+"] not submitted to the grid (devel-note: be more verbose).\n"
                ## end dbg info ##
		self.updateTaskStatus(payload, self.taskState[2])

                try:
		    semvar.acquire()
                    newstate[payload]="fail"
                finally:
                    semvar.release()
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:CrabWorkPerformedPartial":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, event + ": %s" % payload)
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                taskName = str(payload.split("::", 1)[0])
                ## start dbg info ##
                OK += "  Task ["+taskName+"] submitted to the grid.\n"
                OK += "    -> WARNING: couldn't submit jobs "+str(eval(payload.split("::")[2]))+".\n"
                ## end dbg info ##
                self.preUpdatePartialTask(payload, self.taskState[7])

#                semvar.acquire()
#                newstate[payload]="fail"
#                semvar.release()
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:FastKill":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, event + ": " + str(payload) )
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                ## start dbg info ##
                OK += "  FastKill: task ["+payload+"] killed before the submission to the grid.\n"
                ## end dbg info ##
                self.updateTaskStatus(payload, self.taskState[4])
#                self.updateTaskStatus(payload, self.taskState[5])
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "CrabServerWorkerComponent:CrabWorkRangeSubmitPerformed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                logBuf = self.__logToBuf__(logBuf, event + ": %s " % payload )
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                taskName = str(payload.split("::")[0])
                ## start dbg info ##
                OK += "  Task ["+taskName+"] submitted to the grid.\n"
                OK += "    -> submitted range of jobs: "+str(eval(payload.split("::")[2]))+".\n"
                ## end dbg info ##
                self.preUpdatePartialTask(payload, self.taskState[9])
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from '"+str(event)+"'!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "TaskKilled":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                rangeKillJobs = "all"
                if payload.find("::") != -1:
                    taskName, rangeKillJobs = payload.split("::")
                logBuf = self.__logToBuf__(logBuf,"   Killed task: %s" % taskName)
                if rangeKillJobs == "all":
                    self.updateTaskKilled( taskName, self.taskState[4] )
                else:
                    self.updateTaskKilled( taskName, self.taskState[8] )
                ## start dbg info ##
                OK += "  Task ["+str(payload.split("::")[0])+"] killed (jobs killed: "+str(rangeKillJobs)+").\n"
                ## end dbg info ##
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
                ## start dbg info ##
                ERROR += "  ERROR: problems managing task ["+payload+"] for the event [" +event+ "]!\n"
                ## end dbg info ##
            logging.info(logBuf)
            return taskName, str(OK + "\n" + ERROR)

        if event == "TaskKilledFailed":
            if payload != None or payload != "" or len(payload) > 0: 
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
                rangeKillJobs = "all"
                if payload.find("::") != -1:
                    taskName, rangeKillJobs = payload.split("::")
                logBuf = self.__logToBuf__(logBuf,"   Error killing task: %s" % taskName)
                if rangeKillJobs == "all":
                    self.killTaskFailed( taskName )
                else:
                    self.killTaskFailed( taskName )
                ## start dbg info ##
                OK += "  WARNING: task ["+str(payload.split("::")[0])+"] failed to kill (jobs to be killed: "+str(rangeKillJobs)+").\n"
                ## end dbg info ##
                logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            else:
                logBuf = self.__logToBuf__(logBuf," ")
                logBuf = self.__logToBuf__(logBuf,"ERROR: empty payload from [" +event+ "]!!!!")
                logBuf = self.__logToBuf__(logBuf," ")
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
        logBuf = self.__logToBuf__(logBuf," ")
        logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
        logBuf = self.__logToBuf__(logBuf,"Unexpected event %s, ignored" % event)
        logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
        logBuf = self.__logToBuf__(logBuf," ")
        logging.info(logBuf)

        return None

    ##########################################################################
    # read task config file
    ##########################################################################

    def parseCrabCfg( self, text, section, var ):
        """
        _parseCrabCfg_
        """
        sectionFound = False
        for line in text.split("\n"):
            line = line.strip()
            comment = line.find("#")
            if comment > -1:
                line = line[:comment]
            if line:
                if not sectionFound:
                    if line == "["+section+"]":
                        sectionFound = True
                        continue
                else:
                    if line[0] == "[" and line[-1] == "]":
                        sectionFound = False # Found a new section. Current correct section finished.
                    else:
                        line = line.split("=") # Splitting variable name from its value.
                        if len(line) == 2 and line[0].strip() == var:
                            return line[1].strip()
        return None


    def readInfoCfg(self, path):
        """
        _readInfoCfg_

        read informations from the config files
        """
        pathFile = path + "/share/" + self.crabcfg
        eMail = None
        thresholdLevel = None
        if os.path.exists( pathFile ):
            eMail = self.parseCrabCfg(open(pathFile).read(), "USER", "eMail")
            thresholdLevel = self.parseCrabCfg(open(pathFile).read(), "USER", "thresholdLevel")
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
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while inserting the task " + str(payload) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
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
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR: missing 'eMail' from " + self.crabcfg + " for task: " + str(taskName) )
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            eMail = "mattia.cinquilli@pg.infn.it"  #### TEMPORARY SOLUTION!
        if thresholdLevel == None:
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "WARNING: missing 'thresholdLevel' from " + self.crabcfg + " for task: " + str(taskName) )
            logBuf = self.__logToBuf__(logBuf, "         using default value 'thresholdLevel = 100'")
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
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
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while updating the task " + str(taskName) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
        logging.info(logBuf)


    def preUpdatePartialTask( self, payload, status ):
        """
        _preUpdatePartialTask_

        split the payload-sends email(updates status)
        """
        logBuf = ""

        fields = payload.split("::")
        taskName = fields[0]
        totJobs = int(fields[1])
        pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir
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
            for jobId in range(1,totJobs+1):
                vect = ["Submitted", "", "", 0]
                dictionaryReport.setdefault(jobId, vect)
            try:
                for jobId in jobList:
                    dictionaryReport[jobId+1][0] = "NotSubmitted"
        #        self.addJobsToFile()
            except Exception, ex:
                logBuf = self.__logToBuf__(logBuf, str(ex) )
                logBuf = self.__logToBuf__(logBuf, str(ex.args) )
            #            logBuf = self.__logToBuf__(logBuf,str(dictionaryReport))
            self.prepareReport( taskName, uuid, eMail, 0, 0, dictionaryReport, 0,0 )
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
        outfile = file(taskPath + fileName, 'w').write(str(jobList))#+"::"+str(totJobs))

    def getJobFromFile(self, taskPath):
        """
        _getJobFromFile_
        """
        fileName = "/.notSubmitted.TT"
        if os.path.exists( taskPath + fileName ):
            return eval( file(taskPath + fileName, 'r').read() )#.split("::")
        return []

    def addFailedJobToFile (self, taskPath, jobList):
        fileName = "/.failedResubmitting.TT"
        outfile = file(taskPath + fileName, 'w').write(str(jobList))

    def getFailedJobFromFile(self, taskPath):
        fileName = "/.failedResubmitting.TT"
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
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while updating the task " + str(payload) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
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
                        self.prepareReport( payload, uuid, eMail, 0, 0, dictionaryReport, 0,0 )
                        ## DONE tgz file
                        pathToWrite = str(self.args['dropBoxPath']) + "/" + payload + "/" + self.resSubDir
                        self.prepareTarball( pathToWrite, payload )
                        ## MAIL report user
                        #self.prepareTaskFailed( payload, uuid, eMail, status )
                    else:
                        ## XML report file
                        dictionaryReport =  {"all": ["Killed", "", "", 0]}
                        self.prepareReport( payload, uuid, eMail, 0, 0, dictionaryReport, 0,0 )
		        ## MAIL report user
                        #self.taskFastKill( self.args['dropBoxPath'] + "/" + payload  + "/res/" + self.xmlReportFileName, payload )
        except Exception, ex:
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while reporting info about the task " + str(payload) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
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
            self.taskNotSubmitted( self.args['dropBoxPath'] + "/" + taskName  + "/res/" + self.xmlReportFileName, taskName )
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
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            logBuf = self.__logToBuf__(logBuf, "ERROR while updating the task " + str(taskName) )
            logBuf = self.__logToBuf__(logBuf, "      "+str(ex))
            logBuf = self.__logToBuf__(logBuf,"  <-- - -- - -->")
            logging.info(logBuf)

        """
        eMail = ""
        uuid = ""

        try:
            valuess = TaskStateAPI.getStatusUUIDEmail( taskName )
            if valuess != None:
                stato = valuess[0]
                if stato != self.taskState[2]:
                    if len(valuess) > 1:
                        uuid = valuess[1]
                        if len(valuess) > 2:
                            eMail = valuess[2]
        #            dictionaryReport =  {"all": ["Killed", "", "", 0]}
        #            self.prepareReport( taskName, uuid, eMail, 0, 0, dictionaryReport, 0, 0 )
        #            self.prepareTaskFailed( taskName, uuid, eMail, status )
        except Exception, ex:
            logging.error("  <-- - -- - -->")
            logging.error( "ERROR while reporting info about the task " + str(taskName) )
            logging.error( "      "+str(ex))
            logging.error("  <-- - -- - -->")
        """
        #TaskStateAPI.updatingStatus( taskName, self.taskState[4], 2 )

    def killTaskFailed (self, taskName ):
        """
        _killTaskFailed_
        """
        logBuf = ""
        logBuf = self.__logToBuf__(logBuf,"Error killing task: " + taskName )
        logging.info(logBuf)

        ### DO SOMETHING LIKE FLAG THE TASK ###


    ##########################################################################
    # utilities
    ##########################################################################

    def convertStatus( self, status ):
        """
        _convertStatus_
        """
        stateConverting = {'R': 'Running','SA': 'Aborted','SD': 'Done','SE': 'Done','E': 'Done','SK': 'Cancelled','SR': 'Ready','SU': 'Submitted','SS': 'Scheduled','UN': 'Unknown','SW': 'Waiting','W': 'Submitting','K': 'Killed', 'S': 'Submitted', 'DA': 'Done (Failed)', 'NotSubmitted': 'NotSubmitted'}
        if status in stateConverting:
            return stateConverting[status]
        return 'Unknown'


    def prepareReport( self, taskName, uuid, eMail, thresholdLevel, percentage, dictReportTot, nJobs, flag ):
        """
        _prepareReport_
        """
        pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir

        if os.path.exists( pathToWrite ):
           ###  get user name & original task name  ###
            obj = UtilSubject(self.args['dropBoxPath'], taskName, uuid)
            origTaskName, userName = obj.getInfos()
            del obj
           ###  preparing xml report  ###
            c = CreateXmlJobReport()
            eMaiList = self.getMoreMails( eMail )
            if len(eMaiList) < 1:
                c.initialize( origTaskName, "ASdevel@cern.ch", userName, percentage, thresholdLevel, nJobs)
            else:
                for index in range(len(eMaiList)):
                    if index != 0:
                        c.addEmailAddress( eMaiList[index] )
                    else:
                        c.initialize( origTaskName, eMaiList[0], userName, percentage, thresholdLevel, nJobs)

            for singleJob in dictReportTot:
                J = Job()   ##    id             status                        eec
                J.initialize( singleJob, dictReportTot[singleJob][0], dictReportTot[singleJob][2],\
                            ##         jes                       clear                       Resub
                              dictReportTot[singleJob][1], dictReportTot[singleJob][3], self.getListEl(dictReportTot[singleJob], 4),\
                            ##         site
                              self.getListEl(dictReportTot[singleJob], 5) )
                c.addJob( J )
            c.toXml()
            c.toFile ( pathToWrite + self.tempxmlReportFile )
            if not flag:
                self.undiscoverXmlFile( pathToWrite, taskName, self.tempxmlReportFile, self.xmlReportFileName )

    def getListEl(self, list, el):
        try:
            return list[el]
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

        reg = re.compile('^[\w\.-_]+@(?:[\w-]+\.)+[\w]{2,4}$',re.IGNORECASE)
        if not reg.match( eMail ):
            errmsg = "Error parsing e-mail address; address ["+eMail+"] has "
            errmsg += "an invalid format;"
            logging.debug("WARNING: " + errmsg)
            logging.debug("         this e-mail address will be ignored.")
            return False
        return True


    def prepareTarball( self, path, taskName ):
        """
        _prepareTarball_

        """
        work_dir = os.getcwd()
        os.chdir( path )
        logging.debug("path: " + str(path) + "/done.tar.gz")
	cmd = 'tar --create -z --file='+path+'/.temp_done.tar.gz * --exclude done.tar.gz; '
        cmd += 'mv '+path+'/.temp_done.tar.gz '+path+'/done.tar.gz'
        os.system( cmd )
			
        os.chdir( work_dir )


    def prepareTarballFailed( self, path, taskName, nJob, flag, listaFailed ):
        """
        _prepareTarballFailed_
        """
        work_dir = os.getcwd()
        os.chdir( path )
        cmd = 'mkdir .tmpFailed;'
        lista =  range(1, nJob+1)
        if flag != 0:
            lista = listaFailed
        for i in lista:
            ## Add parametric indexes for failed and successful jobs 
            jtResDir = 'job'+str(i)+'/JobTracking'
            ## Get the most recent failure and copy that to tmp 
            failIndex = 4 
            if os.path.exists('./'+jtResDir+'/Failed/'):
                if len(os.listdir('./'+jtResDir+'/Failed/')) > 0:
                    try:
                        failIndex = max( [ int(s.split('Submission_')[-1]) for s in os.listdir('./'+jtResDir+'/Failed/') ] )
                    except Exception, ex:
                        logBuf = ""
                        logBuf = self.__logToBuf__(logBuf,str(ex) + " " + str(failIndex))
                        logging.info(logBuf)
                        #for s in os.listdir('./'+jtResDir+'/Failed/'):
                        #    logging.error( str(int(s.split('Submission_')[-1])) )
                cmd += 'cp '+self.tempxmlReportFile+' .tmpDone/'+self.xmlReportFileName+' ;'
                cmd += 'cp '+ jtResDir +'/Failed/Submission_'+str(failIndex)+'/log/edgLoggingInfo.log .tmpFailed/edgLoggingInfo_'+str(i)+'.log;'
                #logging.info('cp '+ jtResDir +'/Failed/Submission_'+str(failIndex)+'/log/edgLoggingInfo.log .tmpFailed/edgLoggingInfo_'+str(i)+'.log;')
                if flag != 0:
                     cmd += 'cp '+ jtResDir +'/Failed/Submission_1/std*/* .tmpFailed/;'
                     cmd += 'cp '+ jtResDir +'/Failed/Submission_1/root/* .tmpFailed/;'
        if flag == 0:
            cmd += 'tar --create -z --file='+path+'/.temp_failed.tgz .tmpFailed/edgLoggingInfo_*.log;' 
            cmd += 'rm -drf .tmpFailed;'
            cmd += 'mv '+path+'/.temp_failed.tgz '+path+'/failed.tgz;'
            os.system( cmd )
            os.chdir( work_dir )
            pass
        else:
            cmd += 'tar --create -z --file='+path+'/.temp_failed.tgz .tmpFailed/*;'
            logging.info('tar --create -z --file='+path+'/.temp_failed.tgz .tmpFailed/*;')
            cmd += 'mv '+path+'/.temp_failed.tgz '+path+'/IntermedFailed.tgz;'
            cmd += 'rm -drf .tmpFailed;'
            os.system( cmd )
            os.chdir( work_dir )
            pass


    def undiscoverXmlFile (self, path, taskName, fromFileName, toFileName):
        if os.path.exists(path + fromFileName):
            infile = file(path + fromFileName , 'r').read()
            outfile = file(path + toFileName , 'w').write(infile)
            #infile.close()
            #outfile.close()
 

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
           ###  get user name & original task name  ###
#            obj = UtilSubject(self.args['dropBoxPath'], taskName, uuid)
#            origTaskName, userName = obj.getInfos()
#            del obj
            self.msThread.publish("TaskTracking:TaskEnded", taskName)
            logBuf = self.__logToBuf__(logBuf,"-------> Published 'TaskEnded' message with payload: %s" % taskName)
            self.msThread.commit()
        logging.info(logBuf)


    def taskSuccess( self, taskPath, taskName ):
        """
        _taskSuccess_
        
        Trasmit the "TaskSuccess" event to the prodAgent
       kEnded
        """
        logBuf = ""
        self.msThread.publish("TaskSuccess", taskPath)
        self.msThread.commit()

        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-*")
        logBuf = self.__logToBuf__(logBuf,"-------> Published 'TaskSuccess' message with payload: %s" % taskPath)
        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-*")
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
        self.msThread.publish("TaskFailed", payload)
        self.msThread.commit()

        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-* ")
        logBuf = self.__logToBuf__(logBuf,"Published 'TaskFailed' message with payload: %s" % payload)
#        self.taskEnded(taskName)
        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-* ")
        logging.info(logBuf)


    def taskNotSubmitted( self, taskPath, taskName ): #Name, eMaiList, userName ):
        """
        _taskNotSubmitted_
        """
        logBuf = ""
#        if userName == "" or userName == None:
#            userName = "Unknown"
#        payload = taskName + ":" + userName + ":" + eMaiList
        self.msThread.publish("TaskNotSubmitted", taskPath) # payload)
        self.msThread.commit()

        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-* ")
        logBuf = self.__logToBuf__(logBuf,"Published 'TaskNotSubmitted' message with payload: %s" % taskPath) #payload)
#        self.taskEnded(taskName)
        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-* ")
        logging.info(logBuf)


    def taskFastKill( self, taskPath, taskName ):
        """
        _taskFastKill_
        """
        logBuf = ""
        self.msThread.publish("TaskFastKill", taskPath) # payload) 
        self.msThread.commit()

        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-* ")
        logBuf = self.__logToBuf__(logBuf,"Published 'TaskFastKill' message with payload: %s" % taskPath) #payload)
#        self.taskEnded(taskName)
        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-* ")
        logging.info(logBuf)
  

    def taskIncompleteSubmission( self, taskName, eMaiList, userName ):
        """
        _taskIncompleteSubmission_
        """
        logBuf = ""
        if userName == "" or userName == None:
            userName = "Unknown"
        payload = taskName + ":" + userName + ":" + eMaiList
        self.msThread.publish("TaskIncompleteSubmission", payload)
        self.msThread.commit()

        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-* ")
        logBuf = self.__logToBuf__(logBuf,"Published 'TaskIncompleteSubmission' message with payload: %s" % payload)
        logBuf = self.__logToBuf__(logBuf,"         *-*-*-*-* ")
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
#        logBuf = self.__logToBuf__(logBuf, "*** Polling task database for thread " + threadName + " ***")

        task = None
        try:
            ## starting BOSS session
            mySession = BossSession( self.args['bossClads'] )

            #task2Check = TaskStateAPI.getAllNotFinished()
	    #for task in task2Check:
            task = TaskStateAPI.getNLockFirstNotFinished()
            try:
                taskId = 0
                if task == None or len(task) <= 0:
                    TaskStateAPI.resetControlledTasks()
                else:#if task != None and len(task)>0:
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
                        try:
                            semvar.acquire()
                            if newstate.has_key(taskName):
                                del newstate[taskName]
                        finally:
                            semvar.release()
                        
                        ######### Taskfailed is prepared now
                        self.prepareTaskFailed( taskName, uuid, eMail, status)
  
                    elif newstate.has_key( taskName ) and status == self.taskState[1]:
                        try:
                            semvar.acquire()
                            del newstate[taskName]
                        finally:
                            semvar.release()

		    else:
    	                taskDict = mySession.loadByName( taskName )
		        if len(taskDict) > 0:
                            logBuf = self.__logToBuf__(logBuf, " - - - - - - - ")
                            logBuf = self.__logToBuf__(logBuf, taskName + " ["+str(status)+"] - num.: " + str(len(taskDict)))

			    pathToWrite = ""
			    dictReportTot = {'JobSuccess': 0, 'JobFailed': 0, 'JobInProgress': 0}
			    countNotSubmitted = 0 
			    dictStateTot = {}
                            dictFinishedJobs = {}
			    v = taskDict.values()[0]
			    statusJobsTask = v.jobStates()
#                            logBuf = self.__logToBuf__(logBuf," --> %s <--> %s <--"%(taskName, str(len(statusJobsTask))) )
                            listFailedJobs = self.getFailedJobFromFile( os.path.join(str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir))
                            #logBuf = self.__logToBuf__(logBuf, " -"+str(listFailedJobs)+"- ")
                            listFailed2Prepare = []
			    for job,stato in statusJobsTask.iteritems():
                                jobInfo = v.Job(job)

                                try:
                                   self.mutex.acquire()
                                   resubmitting, MaxResub, Resub = TaskStateAPI.checkNSubmit( taskName, job )
                                finally:
                                   self.mutex.notifyAll()
                                   self.mutex.release()

                                runInfoJob =  jobInfo.specific("1")
                                site = "None"
                                sId = "None"
                                if stato != "W" and (status == self.taskState[3] or status == self.taskState[4] or\
                                                 status == self.taskState[7] or status == self.taskState[1] ):
                                    infoRunningJobs = jobInfo.runningInfo()
                                    if str(infoRunningJobs) != "Job not existing or not loaded":
                                        site = infoRunningJobs.get( 'DEST_CE',  None )
                                        sId = infoRunningJobs.get( 'SCHED_ID', None )
                                    del infoRunningJobs

			    #if lower(self.args['jobDetail']) == "yes":
                                #logging.info("STATUS = " + str(stato) + " - EXE_EXIT_CODE = "+ str(runInfoJob['EXE_EXIT_CODE']) + " - JOB_EXIT_STATUS = " + str(runInfoJob['JOB_EXIT_STATUS']) + " - resubmitting, MaxResub, Resub = " + str(TaskStateAPI.checkNSubmit( taskName, job )))
			        vect = []
			        if runInfoJob['EXE_EXIT_CODE'] == "NULL" and runInfoJob['JOB_EXIT_STATUS'] == "NULL":
   			            vect = [self.convertStatus(stato), "", "", 0, Resub, site]
			        else:
                                    vect = [self.convertStatus(stato), runInfoJob['EXE_EXIT_CODE'], runInfoJob['JOB_EXIT_STATUS'], 0, Resub, site]
                                dictStateTot.setdefault(job, vect)
 
                                jobPartList = self.getJobFromFile(str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir)

			        if stato == "SE" or stato == "E":
				    if runInfoJob['EXE_EXIT_CODE'] == "0" and runInfoJob['JOB_EXIT_STATUS'] == "0":
				        dictReportTot['JobSuccess'] += 1
				        dictStateTot[job][3] = 1
                                        dictFinishedJobs.setdefault(job, 1)
				    elif not resubmitting:
				        dictReportTot['JobFailed'] += 1
                                        dictStateTot[job][0] = "Done (Failed)"
                                        dictStateTot[job][3] = 1
                                        dictFinishedJobs.setdefault(job, 1)
				    else:
				        dictReportTot['JobInProgress'] += 1
                                        dictStateTot[job][0] = "Resubmitting by server"#"Managing by server"
                                        dictFinishedJobs.setdefault(job, 0)
                                        if resubmitting:
                                            if job not in listFailedJobs:
                                   #             logBuf = self.__logToBuf__(logBuf, " -adding JOB: "+str(job))
                                                listFailed2Prepare.append( job )
			        elif stato == "SA" or stato == "SK" or stato == "K":
				    if not resubmitting:
				        dictReportTot['JobFailed'] += 1
                                        dictFinishedJobs.setdefault(job, 1)
				    else:
				        dictReportTot['JobInProgress'] += 1
                                        dictStateTot[job][0] = "Resubmitting by server"#"Managing by server"
                                        dictFinishedJobs.setdefault(job, 0)
                                        if job not in listFailedJobs:
                                   #         logBuf = self.__logToBuf__(logBuf, " -adding JOB: "+str(job))
                                            listFailed2Prepare.append( job )
			        elif stato == "W":
                                    if not resubmitting:
   				        countNotSubmitted += 1 
				        dictReportTot['JobFailed'] += 1
                                        dictFinishedJobs.setdefault(job, 1)
                                        if status == self.taskState[4]:
                                            dictStateTot[job][0] = "Killed"
                                        else:
                                            dictStateTot[job][0] = "NotSubmitted"
                                    elif int(job) in jobPartList:
                                        countNotSubmitted += 1
                                        dictReportTot['JobFailed'] += 1
                                        dictFinishedJobs.setdefault(job, 0)
#                                        if status == self.taskState[9]:
#                                            dictStateTot[job][0] = "Created"
                                        #else:
                                        dictStateTot[job][0] = "NotSubmitted"
                                    elif status == self.taskState[4]:
                                        #countNotSubmitted += 1   
                                        dictReportTot['JobFailed'] += 1
                                        dictFinishedJobs.setdefault(job, 0)
                                        dictStateTot[job][0] = "Killed"
                                    elif status == self.taskState[9]:
                                        dictStateTot[job][0] = "Created"
                                        countNotSubmitted += 1
                                        dictFinishedJobs.setdefault(job, 0)
                                        dictReportTot['JobInProgress'] += 1
                                    else:
                                        countNotSubmitted += 1
                                        dictReportTot['JobInProgress'] += 1
                                        dictFinishedJobs.setdefault(job, 0)
                                elif not resubmitting:
                                    if status == self.taskState[4]:
                                        dictReportTot['JobInProgress'] += 1
                                        dictFinishedJobs.setdefault(job, 0)
                                    else:
                                       file("JobStupidi", 'a').write("\ntask name: " + str(taskName) +\
                                                                     " - resubm: " + str(resubmitting) +\
                                                                     " - stato: " + str(stato) +\
                                                                    " - status: " + str(status) )
                                       logging.debug("resubm: " +str(resubmitting)+ " - stato: " +str(stato)+ " - status: " +str(status))
                               #elif stato != "K": ## ridondante
                                    #dictReportTot['JobFailed'] += 1
                                    #dictFinishedJobs.setdefault(job, 0)
                                    #else:
                                    #    dictReportTot['JobFailed'] += 1
                                    #    dictFinishedJobs.setdefault(job, 0)
                                else:
                                    dictReportTot['JobInProgress'] += 1
                                    dictFinishedJobs.setdefault(job, 0)
                            ### prototype: preparing intermediate output 4 failed jobs ###
                            """
                            logBuf = self.__logToBuf__(logBuf, " adding: "+str(listFailed2Prepare) )
                            if len (listFailed2Prepare) > 0:
                                self.prepareTarballFailed( os.path.join(str(self.args['dropBoxPath']), taskName, self.resSubDir), taskName, len(statusJobsTask), 1, listFailed2Prepare)
                            self.addFailedJobToFile(str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir, listFailed2Prepare)*/
                            """

			    rev_items = [(v, int(k)) for k, v in dictStateTot.items()]
			    rev_items.sort()
			    dictStateTot = {}
			    for valu3, k3y in rev_items:
			        dictStateTot.setdefault( k3y, valu3 )

			    for state in dictReportTot:
                                logBuf = self.__logToBuf__(logBuf, " Job " + state + ": " + str(dictReportTot[state]))
			    if countNotSubmitted > 0:
                                logBuf = self.__logToBuf__(logBuf, "    -> of which not yet submitted: " + str(countNotSubmitted))

			    endedJob = dictReportTot['JobSuccess'] + dictReportTot['JobFailed']

			    try:
			        percentage = (100 * endedJob) / len(statusJobsTask)
			        pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir

                                if os.path.exists( pathToWrite ):
                                    if status == self.taskState[1]:
                                        if newstate.has_key(taskName):
                                            try:
                                                semvar.acquire()
                                                del newstate[taskName]
                                            finally:
                                                semvar.release()
                                        else:
                                            ## avoiding simultaneous access ##
                                            try:
                                                semfile.acquire()
                                                self.prepareReport( taskName, uuid, eMail, thresholdLevel, percentage, dictStateTot, len(statusJobsTask),1 )
                                            finally:
                                                semfile.release()
                                    else:
                                        self.prepareReport( taskName, uuid, eMail, thresholdLevel, percentage, dictStateTot, len(statusJobsTask),1 )
                                else:
                                    logBuf = self.__logToBuf__(logBuf, "Error: the path " + pathToWrite + " does not exist!\n")
                                #self.prepareTarballFailed(pathToWrite, taskName, len(statusJobsTask) )
			        if percentage != endedLevel or \
			            (percentage == 0 and status == self.taskState[3] ) or \
			            (percentage == 0 and status == self.taskState[1] ) or \
			            (notified < 2 and endedLevel == 100):

		 	            ###  updating endedLevel  ###
				    if endedLevel == 100:
                                        msg = TaskStateAPI.updatingEndedPA( taskName, str(percentage), self.taskState[5])
                                        logBuf = self.__logToBuf__(logBuf, msg)
                                        if notified != 2:
                                            self.taskSuccess( pathToWrite + self.xmlReportFileName, taskName )
                                        #self.taskEnded(taskName)
                                            notified = 2
                                            msg = TaskStateAPI.updatingNotifiedPA( taskName, notified )
                                            logBuf = self.__logToBuf__(logBuf, msg)
				    elif percentage != endedLevel:
				        msg = TaskStateAPI.updatingEndedPA( taskName, str(percentage), status)
                                        logBuf = self.__logToBuf__(logBuf, msg)
				        ### prepare tarball & send eMail ###
                                        if percentage != endedLevel:
                                            obj = Outputting( self.xmlReportFileName, self.tempxmlReportFile )
                                            logBuf = self.__logToBuf__(logBuf, "**** ** **** ** ****")
                                            logBuf = self.__logToBuf__(logBuf, "  preparing OUTPUT")

                                            obj.prepare( pathToWrite, taskName, len(statusJobsTask), dictFinishedJobs)# ,"Done")

                                            if os.path.exists( pathToWrite+"/done.tar.gz" ):
                                                logBuf = self.__logToBuf__(logBuf, "  preparing OUTPUT finished")
                                            else:
                                                logBuf = self.__logToBuf__(logBuf, "  preparing OUTPUT FAILED")
                                            #obj.prepare( pathToWrite, taskName, len(statusJobsTask), dictFinishedJobs )
                                            logBuf = self.__logToBuf__(logBuf, "**** ** **** ** ****")
                                            #obj.prepare( pathToWrite, taskName, len(statusJobsTask), dictFinishedJobs )
                                        if percentage >= thresholdLevel:
                                            if dictReportTot['JobFailed'] > 0:
                                                ##hassen added and commented
                                                obj = Outputting( self.xmlReportFileName, self.tempxmlReportFile )
                                                logBuf = self.__logToBuf__(logBuf, "**** ** **** ** ****")
                                                logBuf = self.__logToBuf__(logBuf, "  preparing OUTPUT FAILED")
                                                #obj.prepare( pathToWrite, taskName, len(statusJobsTask), dictFinishedJobs )
                                                obj.prepare( pathToWrite, taskName, len(statusJobsTask), dictFinishedJobs,"Failed" )
                                                #obj.prepare( pathToWrite, taskName, len(statusJobsTask), dictFinishedJobs )
                                                logBuf = self.__logToBuf__(logBuf, str(dictFinishedJobs))
                                                if os.path.exists( pathToWrite+"/failed.tar.gz" ):
                                                    logBuf = self.__logToBuf__(logBuf, "  preparing OUTPUT failed finished")

                                                else:
                                                    logBuf = self.__logToBuf__(logBuf, "  preparing OUTPUT FAILED")

                                                self.prepareTarballFailed(pathToWrite, taskName, len(statusJobsTask),0 )

					    if percentage == 100:
					        self.taskSuccess( pathToWrite + self.xmlReportFileName, taskName )
#                                                self.taskEnded(taskName)
					        notified = 2
					        msg = TaskStateAPI.updatingNotifiedPA( taskName, notified )
                                                logBuf = self.__logToBuf__(logBuf, msg)
                                                obj.deleteTempTar( pathToWrite )
					    elif notified <= 0:
					        self.taskSuccess( pathToWrite + self.xmlReportFileName, taskName )
					        notified = 1
					        msg = TaskStateAPI.updatingNotifiedPA( taskName, notified )
                                                logBuf = self.__logToBuf__(logBuf, msg)
			        elif status == '':
                                    msg = ""
			            if dictReportTot['JobSuccess'] + dictReportTot['JobFailed'] + dictReportTot['JobInProgress'] > countNotSubmitted:
				        msg = TaskStateAPI.updateTaskStatus( taskName, self.taskState[3] )
				    else:
				        msg = TaskStateAPI.updateTaskStatus( taskName, self.taskState[2] )
                                
                                    logBuf = self.__logToBuf__(logBuf, msg)
                                self.undiscoverXmlFile( pathToWrite, taskName, self.tempxmlReportFile, self.xmlReportFileName )
 
			    except ZeroDivisionError, detail:
                                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
                                logBuf = self.__logToBuf__(logBuf, "WARNING: No jobs in the task " + taskName)
                                logBuf = self.__logToBuf__(logBuf, "         deatil: " + str(detail))
                                logBuf = self.__logToBuf__(logBuf, "  <-- - -- - -->")
		        else:
			    #logging.debug("-----")
                            logBuf = self.__logToBuf__(logBuf, "-----")
			    #logging.debug( "Skipping task "+ taskName )
                            logBuf = self.__logToBuf__(logBuf, "Skipping task "+ taskName)

                        ##clear tasks from memory
                        mySession.clear()
            finally:
                #case with a task taken
                if task != None and len(task)>0:
                    TaskStateAPI.setTaskControlled(taskId)
                    logBuf = self.__logToBuf__(logBuf, "Task(id, name): (" + str(taskId) + ", " + str(taskName) + ") set to Controlled.")

        except Exception, ex:
            #logging.error("ERROR: " + str(traceback.format_exc()))
            logBuf = self.__logToBuf__(logBuf, "ERROR: " + str(traceback.format_exc()))

#        logBuf = self.__logToBuf__(logBuf, "Pool ended for thread " + threadName)
        logging.info(logBuf)

        time.sleep(float(self.args['PollInterval']))


    def printDictKey( self, dict, value ):
        """
        _printDictKey_
        """
        msg = "Ended jobs: * "
        try:
            for valu3, k3y in dict.iteritems():
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
        self.ms.subscribeTo("CrabServerWorkerComponent:TaskArrival")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkFailed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformedPartial")
        self.ms.subscribeTo("CrabServerWorkerComponent:FastKill")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkRangeSubmitPerformed")
	self.ms.subscribeTo("DropBoxGuardianComponent:NewFile")
	self.ms.subscribeTo("ProxyTarballAssociatorComponent:WorkDone")
	self.ms.subscribeTo("ProxyTarballAssociatorComponent:UnableToManage")
        self.ms.subscribeTo("TaskKilled")
        self.ms.subscribeTo("TaskKilledFailed")

        #reset all work_status
        TaskStateAPI.resetAllWorkStatus()

        nMaxThreads = int(self.args['Thread']) + 1
        # start polling threads
	for i in range(1,nMaxThreads):
	   pollingThread = PollThread(self.pollTasks, "pollingThread_" + str(i))
           pollingThread.start()

        # start message thread
        msgThread = MsgQueueExecuterThread(self.__executeQueuedMessages__)
        msgThread.start()

        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            logBuf = ""
            logBuf = self.__logToBuf__(logBuf,"\n")
            logBuf = self.__logToBuf__(logBuf,"GOT MESSAGE: " + str(messageType))
            logBuf = self.__logToBuf__(logBuf,"\n")
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
        Queue.Queue.__init__(self,maxsize)
      
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
                time.sleep(3)
            except Exception, ex:
                # log error message
                # new exception(detailed) logging
                logging.error("ERROR in MsgQueueExecuterThread \n" + str(traceback.format_exc()))

