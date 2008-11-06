#!/usr/bin/env python
"""
_TaskTracking_

"""

__revision__ = "$Id: TaskTrackingComponent.py,v 1.123 2008/10/29 22:38:27 mcinquil Exp $"
__version__ = "$Revision: 1.123 $"

import os
import time
import datetime
import sys

# Blite API import
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import TaskError, JobError

# Message service import
from MessageService.MessageService import MessageService

# threads
from threading import Thread, BoundedSemaphore

# logging
import logging
from logging.handlers import RotatingFileHandler
import ProdAgentCore.LoggingUtils as LoggingUtils

from ProdAgentCore.Configuration import ProdAgentConfiguration

# DB PA
from TaskStateAPI import TaskStateAPI

# XML
from CrabServer.CreateXmlJobReport import * 

# TT utility
from TaskTrackingUtil import *

import traceback
import Queue
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
        self.args.setdefault("allow_anonymous", "0")
        
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
                           "partially submitted", \
                           "partially killed"
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
    
    def __appendDbgInfo( self, taskName, message, jobid = "-1" ):
        """
        _appendDbgInfo_
        
        update debug informations about task processing
        """
        from types import StringType
        if type(message) != StringType:
            message.setdefault('date', str(time.time()))

        #path of the logging info file
        filepath = None
        if jobid == "-1":
            filepath = os.path.join( self.args['dropBoxPath'], \
                                     taskName+self.workAdd,    \
                                     'internalog.xml'          \
                                   )
        else:
            filepath = os.path.join( self.args['dropBoxPath'], \
                                     taskName+self.workAdd,    \
                                     'internalog_'+jobid+'.xml'\
                                   )

        from InternalLoggingInfo import InternalLoggingInfo
        xmlogger = InternalLoggingInfo( filepath )
        if not os.path.exists( filepath ):
            xmlogger.createLogger( message )
        else:
            xmlogger.addEntry( message )
        

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

        taskName = payload
        logBuf = ""
        _loginfo = {}
        _loginfo.setdefault('ev', str(event))

        # new task to insert
        if event == "CRAB_Cmd_Mgr:NewTask":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log(logBuf, "NewTask: %s" % taskName)
                logBuf = self.__log(logBuf, taskName)
                taskName, totjobs, templistsubmit = payload.split("::")
                self.insertNewTask( taskName )
                listsubmit = eval(templistsubmit)
                self.fastReport( taskName, totjobs, listsubmit )
                logBuf = self.__log(logBuf, "               new task inserted.")
                _loginfo.setdefault('txt', str("Arrived task: " + str(taskName)))
            else:
                logBuf = self.__log(logBuf, "ERROR: wrong payload from [" +event+ "]!!!!")
            logging.info(logBuf)
            self.__appendDbgInfo(taskName, _loginfo)
            return

        # e-mail & treshold for arrived task
        if event == "CRAB_Cmd_Mgr:MailReference":
            if payload != None or payload != "" or len(payload) > 0:
                taskName, eMail, threshold = payload.split("::")
                logBuf = self.__log(logBuf, "E-mail %s and threshold %s arrived for task %s" %(str(eMail),str(threshold),taskName))
                self.updateEmailThresh(taskName, eMail, threshold)
	        logBuf = self.__log(logBuf, "     db updated.")
            else:
                logBuf = self.__log(logBuf, "ERROR: wrong payload from [" +event+ "]!!!!")
            logging.info(logBuf)
            return

        # task registered in the db
        if event == "TaskRegisterComponent:NewTaskRegistered":
	    if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log(logBuf, "Submitting Task: %s" % str(taskName) )
                taskname, maxtry, listjob = payload.split("::")
                self.updateTaskStatus( taskname, self.taskState[1] )
                self.processSubmitted(taskname)
                logBuf = self.__log(logBuf, "              task updated.")
                _loginfo.setdefault('range', str(listjob))
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
            logging.info(logBuf)
            self.__appendDbgInfo(taskname, _loginfo)
            return

        # submission performed
        if event == "CrabServerWorkerComponent:CrabWorkPerformed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log(logBuf, "CrabWorkPerformed: %s" % payload)
                taskName, textreason = payload.split("::")
		self.updateTaskStatus( taskName, self.taskState[3])
                self.updateProxyName(taskName)
                self.processSubmitted(taskName)
                logBuf = self.__log(logBuf, "               task updated.")
                _loginfo.setdefault('txt', textreason)
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
            self.__appendDbgInfo(taskName, _loginfo)
            logging.info(logBuf)
            return

        # submission failed
        if event == "CrabServerWorkerComponent:CrabWorkFailed":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log(logBuf, "CrabWorkFailed: %s" % payload)
                self.updateProxyName(taskName)
		self.updateTaskStatus(taskName, self.taskState[2])
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
            logging.info(logBuf)
            return

        # submission failed
        if event == "CrabServerWorkerComponent:SubmitNotSucceeded":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log(logBuf, str(event.split(":")[1]) + ": %s" % payload)
                taskName, taskStatus, reason, jobid = payload.split("::")
	        _loginfo.setdefault('txt', str(reason)) 	 
	        _loginfo.setdefault('code', str(taskStatus))
                self.__appendDbgInfo(taskName, _loginfo) #, jobid)
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
            logging.info(logBuf)
            return

        # fast kill performed
        if event == "CrabServerWorkerComponent:FastKill":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log(logBuf, event + ": " + str(payload) )
                self.updateTaskStatus(payload, self.taskState[4])
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
            logging.info(logBuf)
            return

        # new command
        if event == "CRAB_Cmd_Mgr:NewCommand":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log(logBuf, event + ": " + str(payload) )
                taskName, count, listjob, cmnd = payload.split("::")
                _loginfo.setdefault('txt', "New command %s for task %s"%(str(cmnd), str(taskName)) )
                _loginfo.setdefault('count', str(count))
                _loginfo.setdefault('range', str(listjob))
                if str(cmnd) == "kill":
                    self.transientTaskJob(taskName, listjob, "Killing")
                elif str(cmnd) == "submit":
                    self.transientTaskJob(taskName, listjob, "Submitting")
                elif str(cmnd) == "outputRetrieved":
                    self.setCleared(taskName, eval(listjob))
                else:
                    _loginfo.setdefault('exc', "Unknown operation [%s]"%str(cmnd) )
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
            self.__appendDbgInfo(taskName, _loginfo)
            logging.info(logBuf)
            return

        # kill asked
        if event == "KillTask":
            if payload != None or payload != "" or len(payload) > 0:
                logBuf = self.__log(logBuf, event + ": " + str(payload) )
                taskName, fake_proxy, range = payload.split(":")
                _loginfo.setdefault('range', str(range))
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from '"+str(event)+"'!!!!")
            self.__appendDbgInfo(taskName, _loginfo)
            logging.info(logBuf)
            return

        # successfully killed
        if event == "TaskKilled":
            if payload != None or payload != "" or len(payload) > 0:
                rangeKillJobs = "all"
                if payload.find("::") != -1:
                    taskName, rangeKillJobs = payload.split("::")
                logBuf = self.__log(logBuf, "   Killed task: %s" % taskName)
                if rangeKillJobs == "all":
                    self.updateTaskKilled( taskName, self.taskState[4] )
                else:
                    self.updateTaskKilled( taskName, self.taskState[7] )
                self.transientTaskJob(taskName, rangeKillJobs, "", True)
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
            logging.info(logBuf)
            return

        # problems killing
        if event == "TaskKilledFailed":
            if payload != None or payload != "" or len(payload) > 0: 
                rangeKillJobs = "all"
                if payload.find("::") != -1:
                    taskName, rangeKillJobs = payload.split("::")
                logBuf = self.__log(logBuf, "   Error killing task: %s" % taskName)
                self.transientTaskJob(taskName, rangeKillJobs, "", True)
                _loginfo.setdefault('range', str(rangeKillJobs))
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
            logging.info(logBuf)
            self.__appendDbgInfo(taskName, _loginfo)
            return

        # get output performed
        if event == "CRAB_Cmd_Mgr:GetOutputNotification":
            if payload != "" and payload != None:
                taskName, jobstr = payload.split('::')
                logBuf = self.__log(logBuf, "Cleared jobs: " + str(jobstr) + \
                                            " for task " + str(taskName) )
                try:
                    self.setCleared(taskName, eval(jobstr))
                except Exception, ex:
                    logBuf = self.__log(logBuf, "Exception raised: " + str(ex) )
                    logBuf = self.__log(logBuf, str(traceback.format_exc()) )
                _loginfo.setdefault('range', str(jobstr))
            else:
                logBuf = self.__log(logBuf, "No task specified for " + str(event) )
            self.__appendDbgInfo(taskName, _loginfo)
            return

        # registering logging information
        if event == "TTXmlLogging":
            if payload != "" and payload != None:
                taskName, jobid, pklpath = payload.split("::")
                logging.info(payload)
                self.__appendDbgInfo(taskName, pklpath, jobid)
            else:
                logBuf = self.__log(logBuf, "ERROR: empty payload from [" +event+ "]!!!!")
            logging.info(logBuf)
            return

        
        # start debug event
        if event == "TaskTracking:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return

        # stop debug event
        if event == "TaskTracking:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        # wrong event
        logBuf = self.__log(logBuf, "Unexpected event %s, ignored" % event)
        logging.info(logBuf)

        return

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
            logBuf = self.__log(logBuf, "ERROR while inserting the task " + str(payload) )
            logBuf = self.__log(logBuf, "      "+str(ex))
        logging.info(logBuf)

    def updateEmailThresh( self, taskname, email, threshold ):
        """
        _updateEmailThresh_
        """
        logBuf = ""

        if email == None or len(email) == 0:
            logBuf = self.__log(logBuf, "ERROR: missing 'eMail' for task: " + str(taskname) )
        if threshold == None:
            threshold = "100"
        try:
            ttdb = TaskStateAPI()
            ttdb.updateEmailThresh( taskname, str(email), str(threshold) )
        except Exception, ex:
            logBuf = self.__log(logBuf, "ERROR while updating the 'eMail' field for task: " + str(taskname) )

    def updateProxyName(self, taskName):
        """
        _updateProxyName_
        
        duplicate proxy from blite + archive name
        (to be executed once per task)
        """
        logBuf = ""
        mySession = BossLiteAPI("MySQL", pool=self.sessionPool)
        taskObj = None
        ttdb = TaskStateAPI()
        ttutil = TaskTrackingUtil(self.args['allow_anonymous']) 

        try:
            taskObj = mySession.loadTaskByName( taskName )
        except TaskError, te:
            logBuf = self.__log(logBuf,"  Requested task [%s] does not exist."%(taskName) )
	    logBuf = self.__log(logBuf,"  %s"%(str(te)))
	if not taskObj is None:
	    proxy = taskObj['user_proxy']
            userName = ""
            try:
	        userName = ttutil.cnSplitter(ttutil.getNameFromProxy(proxy))
            except Exception, ex:
                userName = taskName.split("_")[0]
	    try:
                if len(userName) == 1:
                    ttdb.updateProxyUname(mySession.bossLiteDB, taskName, \
                                          proxy, userName[0])
                else:
                    ttdb.updateProxyUname(mySession.bossLiteDB, taskName, \
                                          proxy, userName[-1])
            except Exception, ex:
                logBuf = self.__log(logBuf, "ERROR while updating the task " + str(taskName) )
                logBuf = self.__log(logBuf, "      "+str(ex))
        logging.info(logBuf)
        mySession.bossLiteDB.close()
        del taskObj, mySession

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
            logBuf = self.__log(logBuf, "ERROR while updating the task " + str(payload) )
            logBuf = self.__log(logBuf, "      "+str(ex))
            logging.info(logBuf)
            logBuf = ""

        eMail = ""
        uuid = ""
        taskObj = None
        try:
            if status == self.taskState[2] or status == self.taskState[4]:
                valuess = ttdb.getStatusUUIDEmail( payload )
                if valuess != None:
                    status = valuess[0]
		    uuid = valuess[1]
                    eMail = valuess[2]
                    ## XML report file
                    if status == self.taskState[2]:
                        dictionaryReport =  {"all": ["NotSubmitted", "", "", 0, '', 'C']}
                        self.prepareReport( payload, uuid, eMail, valuess[3], 0, 0, dictionaryReport, 0, 0 )
                    elif status == self.taskState[4]:
                        dictionaryReport =  {"all": ["Killed", "", "", 0, '', 'C']}
                        self.prepareReport( payload, uuid, eMail, valuess[3], 0, 0, dictionaryReport, 0, 0 )
        except Exception, ex:
            logBuf = self.__log(logBuf, "ERROR while reporting info about the task " + str(payload) )
            logBuf = self.__log(logBuf, "      "+str(ex))
            logging.info(logBuf)

    def transientTaskJob(self, taskName, range, status, reverse = False):
        """
        _transientTaskJob_
        """
        mySession = None
        taskObj = None
        joblist = eval(range)

        ## bossLite session
        try:
            mySession = BossLiteAPI("MySQL", pool=self.sessionPool)
        except Exception, ex:
            logging.info(str(ex))
            return 0

        ## loading blite task, jobs, runnning jobs
        try:
            try:
                taskObj = mySession.loadTaskByName( taskName )
            except TaskError, te:
                taskObj = None
            if taskObj is None:
                raise Exception("Unable to load task [%s]."%(taskName))
            else:
                for jobbe in taskObj.jobs:
                    try:
                        mySession.getRunningInstance(jobbe)
                    except JobError, ex:
                        logging.error('Problem loading job running info')
                        continue
        except Exception, ex:
            import traceback
            logging.error( "Exception raised: " + str(ex) )
            logging.error( str(traceback.format_exc()) )

        ## updating internal server status for job in joblist
        try:
            if reverse:
                status = "inProgress"
            ttdb = TaskStateAPI()
            for jobid in joblist:
                ttdb.updateStatusServer(mySession.bossLiteDB, taskName, jobid, status)
            if taskObj is not None:
                self.singleTaskPoll(taskObj, ttdb, taskName, mySession)
        except Exception, ex:
            import traceback
            logging.error( "Exception raised: " + str(ex) )
            logging.error( str(traceback.format_exc()) )
        mySession.bossLiteDB.close()
        del taskObj
        del mySession


    def prepareTaskFailed( self, taskName, uuid, eMail, status, userName ):
        """
	_prepareTaskFailed_
	
	"""
        ttutil = TaskTrackingUtil(self.args['allow_anonymous'])
	origTaskName = ttutil.getOriginalTaskName(taskName, uuid)
	eMaiList = ttutil.getMoreMails(eMail)
	strEmail = ""
	for mail in eMaiList:
	    strEmail += str(mail) + ","
        ttdb = TaskStateAPI()
	ttdb.updatingNotifiedPA( taskName, 2 )
        if status == self.taskState[2]:
            self.taskNotSubmitted( os.path.join( self.args['dropBoxPath'], \
                                                 (taskName + self.workAdd), \
                                                 self.xmlReportFileName), \
                                   taskName )
        else:
            self.taskFailed(origTaskName, strEmail[0:len(strEmail)-1], userName, taskName)
	 

    def updateTaskKilled ( self, taskName, status ):
        """
        _updateTaskKilled_
        """
        logBuf = ""
        try:
            ttdb = TaskStateAPI()
            ttdb.updateStatus( taskName, status )
        except Exception, ex:
            logBuf = self.__log(logBuf, "ERROR while updating the task " + str(taskName) )
            logBuf = self.__log(logBuf, "      "+str(ex))
            logging.info(logBuf)


    def processSubmitted(self, taskName):
        """
        _processSubmitted_
        """
        taskObj = None
        mySession = None

        ## bossLite session
        try:
            mySession = BossLiteAPI("MySQL", pool=self.sessionPool)
        except Exception, ex:
            logging.info(str(ex))
            return 0
        try:
        ## lite task load in memory
            try:
                taskObj = mySession.loadTaskByName( taskName )
            except TaskError, te:
                taskObj = None
            if taskObj is None:
                logging.info("Unable to load task [%s]."%(taskName))
            else:
                self.singleTaskPoll(taskObj, TaskStateAPI(), taskName, mySession)
        except Exception, ex:
            logging.error( "Exception raised: " + str(ex) )
            logging.error( str(traceback.format_exc()) )
        mySession.bossLiteDB.close()
        del mySession


    def setCleared (self, taskName, jobList):
        """
        _setCleared_
        """
        taskObj = None
        mySession = None

        ## bossLite session
        try:
            mySession = BossLiteAPI("MySQL", pool=self.sessionPool)
        except Exception, ex:
            logging.info(str(ex))
            return 0
        try:
        ## lite task load in memory
            try:
                taskObj = mySession.loadTaskByName( taskName )
            except TaskError, te:
                taskObj = None
            if taskObj is None:
                logging.info("Unable to load task [%s]."%(taskName))
            else:
                for jobbe in taskObj.jobs:
                    try:
                        mySession.getRunningInstance(jobbe)
                    except JobError, ex:
                        logging.error('Problem loading job running info')
                    if jobbe['jobId'] in jobList:
                        if jobbe.runningJob['status'] in ["D","E", "DA", "SD"]:
                            jobbe.runningJob['status'] = "UE"
                            if jobbe.runningJob['processStatus'] in ["handled", "not handled"]:
                                jobbe.runningJob['processStatus'] = "output_requested"
                mySession.updateDB(taskObj)
                self.singleTaskPoll(taskObj, TaskStateAPI(), taskName, mySession)
        except Exception, ex:
            logging.error( "Exception raised: " + str(ex) )
            logging.error( str(traceback.format_exc()) )
        mySession.bossLiteDB.close()
        del mySession


    ##########################################################################
    # utilities
    ##########################################################################

    def fastReport(self, taskName, totjobs, listsubmit, taskstatus = "Processing"):
        """
        _fastReport_
        """
        dictionaryReport =  {}
        for job in xrange(1, int(totjobs)+1):
            if job in listsubmit:
                dictionaryReport.setdefault(job, ["Submitting", "", "", 0, 0, '', 'CS'])
            else:
                dictionaryReport.setdefault(job, ["Created", "", "", 0, 0, '', 'C'])
        self.prepareReport( taskName, "", "", "", 0, 0, dictionaryReport, int(totjobs), 0, taskstatus )

    def singleTaskPoll(self, taskObj, ttdb, taskName, mySession):
        """
        _singleTaskPoll_
        
        update the xml info with real time updated status
        """
        try:
            ## update xml -> W: duplicated code - need to clean
            dictReportTot = {'JobSuccess': 0, 'JobFailed': 0, 'JobInProgress': 0}
            dictStateTot = {}
            #numJobs = ttdb.countServerJob(mySession.bossLiteDB, taskName)
            numJobs = len(taskObj.jobs)
            status, uuid, email, user_name = ttdb.getStatusUUIDEmail( taskName, mySession.bossLiteDB )
            dictStateTot, dictReportTot, countNotSubmitted = self.computeJobStatus(taskName, mySession, taskObj, dictStateTot, dictReportTot)
            pathToWrite = os.path.join(str(self.args['dropBoxPath']), (taskName + self.workAdd))
            if os.path.exists( pathToWrite ):
                self.prepareReport( taskName, uuid, email, user_name, 0, 0, dictStateTot, numJobs, 1 )
                self.undiscoverXmlFile( pathToWrite, self.tempxmlReportFile, self.xmlReportFileName )
        except Exception, ex:
            logging.error("Problem updating job status to xml: " + str(ex))


    def prepareReport( self, taskName, uuid, eMail, userName, thresholdLevel, percentage, dictReportTot, nJobs, flag, taskstatus = "Processed" ):
        """
        _prepareReport_
        """
        ttutil = TaskTrackingUtil(self.args['allow_anonymous'])
        pathToWrite = os.path.join( self.args['dropBoxPath'], \
                                    (taskName + self.workAdd) )

        if os.path.exists( pathToWrite ):
            ###  get user name & original task name  ###
            origTaskName = ttutil.getOriginalTaskName(taskName, uuid)
            ###  preparing xml report  ###
            c = CreateXmlJobReport()
            eMaiList = ttutil.getMoreMails( eMail )
            if len(eMaiList) < 1:
                c.initialize( origTaskName, "", str(userName), percentage, thresholdLevel, nJobs, taskstatus)
            else:
                for index in xrange(len(eMaiList)):
                    if index != 0:
                        c.addEmailAddress( eMaiList[index] )
                    else:
                        c.initialize( origTaskName, eMaiList[0], str(userName), percentage, thresholdLevel, nJobs, taskstatus)

            for singleJob in dictReportTot:
                st  = dictReportTot[singleJob][0]
                eec = dictReportTot[singleJob][2]
                jec = dictReportTot[singleJob][1]
                cle = dictReportTot[singleJob][3]
                res = ttutil.getListEl(dictReportTot[singleJob], 4)
                sit = ttutil.getListEl(dictReportTot[singleJob], 5)
                sst = ttutil.getListEl(dictReportTot[singleJob], 6)
                sid = ttutil.getListEl(dictReportTot[singleJob], 9)
                J = JobXml()
                J.initialize( singleJob, st, eec, jec, cle, res, sit, sst, sid)
                c.addJob( J )

            c.toXml()
            c.toFile ( os.path.join(pathToWrite, self.tempxmlReportFile) )
            if not flag:
                self.undiscoverXmlFile( pathToWrite, self.tempxmlReportFile, self.xmlReportFileName )

    def undiscoverXmlFile (self, path, fromFileName, toFileName):
        if os.path.exists(os.path.join(path, fromFileName)):
            infile = file(os.path.join(path, fromFileName) , 'r').read()
            file(os.path.join(path, toFileName) , 'w').write(infile)


    ##########################################################################
    # publishing messages
    ##########################################################################

    def taskEnded( self, taskName ):
        """
        _taskEnded_
        
        Starting managing by TaskLifeManager component
        """
        logBuf = ""
        pathToWrite = os.path.join(self.args['dropBoxPath'], taskName)
        if os.path.exists( pathToWrite ):
            try:
                ms_sem.acquire()
                self.msThread.publish("TaskTracking:TaskEnded", taskName)
                self.msThread.commit()
            finally:
                ms_sem.release()

            logBuf = self.__log(logBuf, "--> [TaskEnded] %s" % taskName)
        logging.info(logBuf)


    def taskSuccess( self, taskPath, taskName ):
        """
        _taskSuccess_
        
        Trasmit the "TaskSuccess" event to the prodAgent
        """
        logBuf = ""
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskSuccess", taskPath+"::"+taskName)
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__log(logBuf, "--> [TaskSuccess] %s" % taskPath)
        logging.info(logBuf)


    def taskFailed( self, taskName, eMaiList, userName, fullname ):
        """
        _taskFailed_

        Trasmit the "TaskFailed" event to the prodAgent

        """
        logBuf = ""
        payload = taskName + ":" + userName + ":" + eMaiList + ":" + fullname
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskFailed", payload)
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__log(logBuf, "--> [TaskFailed] %s" % payload)
        self.taskEnded(taskName)
        logging.info(logBuf)


    def taskNotSubmitted( self, taskPath, taskName ):
        """
        _taskNotSubmitted_
        """
        logBuf = ""
        try:
            ms_sem.acquire()
            self.msThread.publish("TaskNotSubmitted", taskPath+"::"+taskName) 
            self.msThread.commit()
        finally:
            ms_sem.release()

        logBuf = self.__log(logBuf, "==> [NotSubmitted] %s" % taskPath)
        self.taskEnded(taskName)
        logging.info(logBuf)

    def __log(self, buf, toadd):
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
                               dictReportTot):
        """
        _computeJobStatus_
        """
        ttdb = TaskStateAPI()
        ttutil = TaskTrackingUtil(self.args['allow_anonymous'])

        countNotSubmitted = 0

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
                vect = [ ttutil.convertStatus(stato), "", "", 0, Resub, site, \
                         stato, joboff, resubmitting, sId]
            else:
                vect = [ ttutil.convertStatus(stato), eec, jec, 0, Resub, site, \
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
            elif stato == "Done (Failed)" or stato == "K":
                if not resubmitting:
                    dictReportTot['JobFailed'] += 1
                else:
                    dictReportTot['JobInProgress'] += 1
            elif stato == "A":
                    dictReportTot['JobFailed'] += 1
            elif (not resubmitting) and joboff == 'Y' and internalstatus != "Killing":
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
                   range = []
            else:
                dictReportTot['JobInProgress'] += 1

            ## case for killing jobs:
            if (not stato in ['K', 'A']) and internalstatus == "Killing":
                dictStateTot[job][0] = "Killing"
                dictStateTot[job][6] = "KK"
            ## case for submitting jobs
            if stato in ['C'] and internalstatus == "Submitting":
                dictStateTot[job][0] = "Submitting"
                dictStateTot[job][6] = "CS"

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
                    userName = task[0][8]

                    if status == self.taskState[2] and notified < 2:
                        ######### Taskfailed is prepared now
                        self.prepareTaskFailed( taskName, uuid, eMail, status, userName)
                    else:
                        ## lite task load in memory
                        try:
                            taskObj = mySession.loadTaskByName( taskName )
                        except TaskError, te:
                            taskObj = None
                            pass 
                        if taskObj is None:
                            logBuf = self.__log(logBuf, "Unable to load " + \
                                                  "task [%s]." % (taskName))
                        else:
                            logBuf = self.__log(logBuf, " - - - - - - - ")
                            logBuf = self.__log(logBuf, " [" + str(taskObj['id']) + "] *" + taskName + "*:")

			    pathToWrite = ""
			    dictReportTot = {'JobSuccess': 0, 'JobFailed': 0, 'JobInProgress': 0}
			    dictStateTot = {}
                            numJobs = len(taskObj.jobs)
                            dictStateTot, dictReportTot, countNotSubmitted = \
                                 self.computeJobStatus( taskName, mySession, \
                                                        taskObj, dictStateTot, \
                                                        dictReportTot )
			    for state in dictReportTot:
                                logBuf = self.__log(logBuf, state + " : " + \
                                                      str(dictReportTot[state]))
			    if countNotSubmitted > 0:
                                logBuf = self.__log(logBuf, " -not submitted: "\
                                                      + str(countNotSubmitted))

			    endedJob = dictReportTot['JobSuccess'] + \
                                       dictReportTot['JobFailed']
			    try:
			        percentage = (100 * endedJob) / numJobs
			        pathToWrite = os.path.join( str(self.args['dropBoxPath']), \
                                                            str(taskName+self.workAdd)
                                                          )

                                if os.path.exists( pathToWrite ):
                                    self.prepareReport( taskName, uuid, eMail, userName, \
                                                        thresholdLevel, percentage, \
                                                        dictStateTot, numJobs, 1 )
                                else:
                                    logBuf = self.__log(logBuf, "Error: The path " + pathToWrite + " does not exist!\n")

                                succexo = 0
			        if percentage != endedLevel or \
			           (percentage == 0 and status == self.taskState[3] ) or \
			           (percentage == 0 and status == self.taskState[1] ) or \
			           (notified < 2 and endedLevel == 100):

		 	            ###  updating endedLevel  ###
				    if percentage == 100:
                                        msg = ttdb.updatingEndedPA( mySession.bossLiteDB, \
                                                                    taskName, str(percentage), \
                                                                    self.taskState[5])
                                        logBuf = self.__log(logBuf, msg)
                                        if notified != 2:
                                            self.taskEnded(taskName)
                                            notified = 2
                                            succexo = 1
				    elif percentage != endedLevel:
				        msg = ttdb.updatingEndedPA( mySession.bossLiteDB, \
                                                                    taskName, str(percentage), \
                                                                    status)
                                        logBuf = self.__log(logBuf, msg)
                                        if percentage >= thresholdLevel:
					    if percentage == 100:
                                                succexo = 1
                                                self.taskEnded(taskName)
					        notified = 2
					    elif notified <= 0:
                                                succexo = 1
					        notified = 1

                                self.undiscoverXmlFile(pathToWrite,\
                                                       self.tempxmlReportFile, \
                                                       self.xmlReportFileName )
                                if succexo:
                                    self.taskSuccess( os.path.join(pathToWrite, self.xmlReportFileName), taskName )
                                    _loginfo.setdefault('ev', "Reached %s"%(str(percentage)) )
                                    _loginfo.setdefault('txt', "publishing task success (sending e-mail to %s)"%(str(eMail)))
                                    msg = ttdb.updatingNotifiedPA( taskName, notified )
                                    logBuf = self.__log(logBuf, msg)
 			    except ZeroDivisionError, detail:
                                logBuf = self.__log(logBuf, "WARNING: No jobs in the task " + taskName)
                                logBuf = self.__log(logBuf, "         deatil: " + str(detail))
                           
            finally:
                #case with a task taken
                if task != None and len(task)>0:
                    ttdb.setTaskControlled(mySession.bossLiteDB, taskId)

                ## clean task from memory
                del task
                del taskObj

                if len(_loginfo) > 0:
                    self.__appendDbgInfo(taskName, _loginfo)

        except Exception, ex:
            logBuf = self.__log(logBuf, "ERROR: " + str(traceback.format_exc()))

        logging.info(logBuf)

        try:
            if not mySession.bossLiteDB is None:
                mySession.bossLiteDB.close()
            del mySession
        except:
            logging.info("not closed..")
            logging.error("ERROR: " + str(traceback.format_exc()))

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
        try:
            self.ms.registerAs("TaskTracking")
            self.msThread.registerAs("TaskTrackingThread")
        except Exception, ex:
            logging.error("Problem registering component\n [%s]"%str(ex))
            logging.error(str(traceback.format_exc()))
            exit

        # subscribe to messages
        self.ms.subscribeTo("TaskTracking:StartDebug")
        self.ms.subscribeTo("TaskTracking:EndDebug")
        self.ms.subscribeTo("TaskKilled")
        self.ms.subscribeTo("TaskKilledFailed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkFailed")
        self.ms.subscribeTo("TaskRegisterComponent:NewTaskRegistered")
        self.ms.subscribeTo("CrabServerWorkerComponent:SubmitNotSucceeded")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewTask")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:GetOutputNotification")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:MailReference")
        ## new for logging task info
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewCommand")
        self.ms.subscribeTo("KillTask")
        self.ms.subscribeTo("TTXmlLogging")


        #reset all work_status
        ttdb = TaskStateAPI()
        ttdb.resetAllWorkStatus()

        nMaxThreads = int(self.args['Thread']) + 1
        # start polling threads
	for i in xrange(1, nMaxThreads):
	    pollingThread = PollThread(self.pollTasks, "pollingThread_" + str(i))
            pollingThread.start()

        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            logging.info("GOT MESSAGE: [%s]" %(messageType))
            try:
                self.__call__(messageType, payload)
            except Exception, ex:
                logging.error("Exception [%s] managing message "%(str(ex)))
                logging.error("Unattended message formatting")
                logging.error("\t-type: %s"%(str(messageType)))
                logging.error("\t-payl: %s"%(str(payload)))

            self.ms.commit()


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

