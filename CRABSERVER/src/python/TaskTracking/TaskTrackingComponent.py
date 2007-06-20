#!/usr/bin/env python
"""
_TaskTracking_

"""

__revision__ = "$Id: TaskTrackingComponent.py,v 1.23 2007/06/18 08:16:04 mcinquil Exp $"
__version__ = "$Revision: 1.23 $"

import os
import time
import sys
import re

# BOSS API import
from BossSession import *

# Message service import
from MessageService.MessageService import MessageService

# threads
from threading import Thread

# logging
import logging
from logging.handlers import RotatingFileHandler
import  ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentCore.ProdAgentException import ProdAgentException


# DB PA
import TaskStateAPI

# XML
#from CrabServer.CreateXmlJobReport import *
from CreateXmlJobReport import *

from Outputting import *

# subject & original name
from UtilSubject import *

##############################################################################
# TaskTrackingComponent class
##############################################################################

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
        self.args.setdefault("bossClads", "/home/ASdevel/test/BOSS/config/")
        self.args.setdefault("dropBoxPath", "/flatfiles/cms/")
	self.args.setdefault("jobDetail", "nop")

        # update parameters
        self.args.update(args)

        # define log file
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        # create log handler
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)

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
	self.taskState = ["arrived", "submitting", "not submitted", "submitted", "killed", "ended", "unpacked", "partially submitted"]

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

        logging.debug("Received Event: %s" % event)
        logging.debug("Payload: %s" % payload)

        # new task to insert
	if event == "DropBoxGuardianComponent:NewFile":
	    if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("NewTask: %s" % payload)
		tName = payload
		logging.info(tName)
		self.insertNewTask( tName )
                logging.info("               new task inserted.")
                logging.info("  <-- - -- - -->")
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from [" +event+ "]!!!!")
                logging.error(" ")
            return
	    
	if event == "ProxyTarballAssociatorComponent:WorkDone":
	    if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("Task Ready to be submitted: %s" % payload)
		self.updateInfoTask( payload )
                logging.info("              task updated.")
                logging.info("  <-- - -- - -->")
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from [" +event+ "]!!!!")
                logging.error(" ")
            return

	if event == "ProxyTarballAssociatorComponent:UnableToManage":
	    if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("Problem with the project: %s" % payload)
		try:
		    TaskStateAPI.updateNotSubmitted( payload, "", "", "", "", self.taskState[2] )
		except Exception, ex:
		    logging.error("  <-- - -- - -->")
		    logging.error( "ERROR while updating the task " + str(taskName) )
		    logging.error( "      "+str(ex))
		    logging.error("  <-- - -- - -->")
                logging.info("              task updated.")
                logging.info("  <-- - -- - -->")
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from [" +event+ "]!!!!")
                logging.error(" ")
            return


	if event == "CrabServerWorkerComponent:TaskArrival":
	    if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("Submitting Task: %s" % str(payload.split(":",1)[0]) )
		self.updateTaskStatus( str(payload.split(":",1)[0]), self.taskState[1] )
                logging.info("              task updated.")
                logging.info("  <-- - -- - -->")
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from [" +event+ "]!!!!")
                logging.error(" ")
            return


        if event == "CrabServerWorkerComponent:CrabWorkPerformed":
            if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("CrabWorkPerformed: %s" % payload)
		self.updateTaskStatus(payload, self.taskState[3])
                logging.info("               task updated.")
                logging.info("  <-- - -- - -->")
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from '"+str(event)+"'!!!!")
                logging.error(" ")
            return

        if event == "CrabServerWorkerComponent:CrabWorkFailed":
            if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("CrabWorkFailed: %s" % payload)
                logging.info("  <-- - -- - -->")
		self.updateTaskStatus(payload, self.taskState[2])
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from '"+str(event)+"'!!!!")
                logging.error(" ")
            return

        if event == "CrabServerWorkerComponent:CrabWorkPerformedPartial":
            if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("CrabWorkPerformedPartial: %s" % payload)
                logging.info("  <-- - -- - -->")
                self.preUpdatePartialTask(payload, self.taskState[7])
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from '"+str(event)+"'!!!!")
                logging.error(" ")
            return

        if event == "TaskKilled":
            if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("   Killed task: %s" % payload)
                self.updateTaskKilled( payload, self.taskState[4] )
                logging.info("  <-- - -- - -->")
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from [" +event+ "]!!!!")
                logging.error(" ")
            return

        if event == "TaskKilledFailed":
            if payload != None:
                logging.info("  <-- - -- - -->")
                logging.info("   Killed task: %s" % payload)
                self.killTaskFailed( payload )
                logging.info("  <-- - -- - -->")
            else:
                logging.error(" ")
                logging.error("ERROR: empty payload from [" +event+ "]!!!!")
                logging.error(" ")
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
        logging.debug("  <-- - -- - -->")
        logging.debug("Unexpected event %s, ignored" % event)
        logging.debug("  <-- - -- - -->")

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

        try:
            TaskStateAPI.insertTaskPA( payload, self.taskState[0] )
        except Exception, ex:
            logging.error("  <-- - -- - -->")
            logging.error( "ERROR while inserting the task " + str(payload) )
            logging.error( "      "+str(ex))
            logging.error("  <-- - -- - -->")


    def updateInfoTask( self, payload ):
        """
        _updateInfoTask_

        updating a task that is just sumitted
        """

        uuid, taskName, proxy = payload.split(":",3)
	
        eMail, thresholdLevel = self.readInfoCfg( self.args['dropBoxPath'] + "/" + taskName )
        if eMail == None:
            logging.error("  <-- - -- - -->")
            logging.error( "ERROR: missing 'eMail' from " + self.crabcfg + " for task: " + str(taskName) )
            logging.error("  <-- - -- - -->")
            eMail = "mattia.cinquilli@pg.infn.it"  #### TEMPORARY SOLUTION!
        if thresholdLevel == None:
            logging.error("  <-- - -- - -->")
            logging.error( "WARNING: missing 'thresholdLevel' from " + self.crabcfg + " for task: " + str(taskName) )
            logging.error( "         using default value 'thresholdLevel = 100'")
            logging.error("  <-- - -- - -->")
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
            logging.error("  <-- - -- - -->")
            logging.error( "ERROR while updating the task " + str(taskName) )
	    logging.error( "      "+str(ex))
            logging.error("  <-- - -- - -->")


    def preUpdatePartialTask( self, payload, status ):
        """
        _preUpdatePartialTask_

        split the payload-sends email(updates status)
        """

        fields = payload.split("::")
        taskName = fields[0]
        totJobs = int(fields[1])
        jobList = eval( fields[2] )

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
            except Exception, ex:
                logging.error( str(ex) )
                logging.error( str(ex.args) )
            #logging.info(str(dictionaryReport))
            self.prepareReport( taskName, uuid, eMail, 0, 0, dictionaryReport, 0,0 )
            ## MAIL report user
            self.prepareTaskFailed( payload, uuid, eMail, status )

    
    def updateTaskStatus(self, payload, status):
        """
	_updateTaskStatus_

	 update the status of a task
        """
	
        try:
            TaskStateAPI.updateStatus( payload, status )
        except Exception, ex:
            logging.error("  <-- - -- - -->")
            logging.error( "ERROR while updating the task " + str(payload) )
	    logging.error( "      "+str(ex))
            logging.error("  <-- - -- - -->")

	eMail = ""
	uuid = ""

        try:
#        if 1:
	    if status == self.taskState[2]:
	        valuess = TaskStateAPI.getStatusUUIDEmail( payload )
		if valuess != None:
		    status = valuess[0]
		    if len(valuess) > 1:
		        uuid = valuess[1]
		        if len(valuess) > 2:
		    	    eMail = valuess[2]
	            ## XML report file
		    dictionaryReport =  {"all": ["NotSubmitted", "", "", 0]}
	            self.prepareReport( payload, uuid, eMail, 0, 0, dictionaryReport, 0,0 )
		    ## DONE tgz file
		    pathToWrite = str(self.args['dropBoxPath']) + "/" + payload + "/" + self.resSubDir
		    self.prepareTarball( pathToWrite, payload )
		    ## MAIL report user
		    self.prepareTaskFailed( payload, uuid, eMail, status )
        except Exception, ex:
	    logging.error("  <-- - -- - -->")
            logging.error( "ERROR while reporting info about the task " + str(payload) )
            logging.error( "      "+str(ex))
            logging.error("  <-- - -- - -->")

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
            self.taskNotSubmitted( self.args['dropBoxPath'] + "/" + taskName  + "/res/" + self.xmlReportFileName )
        elif status == self.taskState[7]:
            self.taskIncompleteSubmission(origTaskName, strEmail[0:len(strEmail)-1], userName)
        else:
            self.taskFailed(origTaskName, strEmail[0:len(strEmail)-1], userName)
	 

    def updateTaskKilled ( self, taskName, status ):
        """
        _updateTaskKilled_
        """
        try:
            TaskStateAPI.updateStatus( taskName, status )
        except Exception, ex:
            logging.error("  <-- - -- - -->")
            logging.error( "ERROR while updating the task " + str(taskName) )
            logging.error( "      "+str(ex))
            logging.error("  <-- - -- - -->")

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
                    dictionaryReport =  {"all": ["Killed", "", "", 0]}
                    self.prepareReport( taskName, uuid, eMail, 0, 0, dictionaryReport, 0, 0 )
                    self.prepareTaskFailed( taskName, uuid, eMail, status )
        except Exception, ex:
            logging.error("  <-- - -- - -->")
            logging.error( "ERROR while reporting info about the task " + str(taskName) )
            logging.error( "      "+str(ex))
            logging.error("  <-- - -- - -->")

        #TaskStateAPI.updatingStatus( taskName, self.taskState[4], 2 )

    def killTaskFailed (self, taskName ):
        """
        _killTaskFailed_
        """
        logging.error("Error killing task: " + taskName )
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
        pathToWrite = pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir

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
                J = Job()   ##    id             status                        eec                            jes                       clear
                J.initialize( singleJob, dictReportTot[singleJob][0], dictReportTot[singleJob][1], dictReportTot[singleJob][2],dictReportTot[singleJob][3] )
                c.addJob( J )

            c.toXml()
            c.toFile ( pathToWrite + self.tempxmlReportFile )
            if not flag:
                self.undiscoverXmlFile( pathToWrite, taskName, self.tempxmlReportFile, self.xmlReportFileName )


    def getMoreMails ( self, eMail ):
        """
        _getMoreMails_

        prepares a list of eMails from str "eMail"
        """

        eMaiList2 = []
        if eMail != None:
            eMaiList = eMail.split(";")
            for index in range(len(eMaiList)):
                temp = eMaiList[index].replace(" ","")
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
        logging.debug("path: " + str(path) + "/done.tgz")
	cmd = 'tar --create -z --file='+path+'/.temp_done.tgz * --exclude done.tgz; '
        cmd += 'mv '+path+'/.temp_done.tgz '+path+'/done.tgz'
        os.system( cmd )
			
        os.chdir( work_dir )


    def prepareTarballDone( self, path, taskName, nJob ):
        """
        _prepareTarballDone_
        """
        work_dir = os.getcwd()
        os.chdir( path )
        cmd = 'mkdir .tmpDone;'
        for i in range(1, nJob+1):
            ## Add parametric indexes for failed and successful jobs # Fabio
            jtResDir = 'job'+str(i)+'/JobTracking'
            cmd += 'cp '+self.tempxmlReportFile+' .tmpDone/'+self.xmlReportFileName+' ;'  ## adde also the report file into the output package. DS 
            ## Get the most recent failure and copy that to tmp # Fabio
            failIndex = 4                # Default Value # Fabio
            if os.path.exists('./'+jtResDir+'/Failed/'):
                if len(os.listdir('./'+jtResDir+'/Failed/')) > 0:
                    failIndex = max( [ int(s.split('Submission_')[-1]) for s in os.listdir('./'+jtResDir+'/Failed/') ] )
                cmd += 'cp -r '+ jtResDir +'/Failed/Submission_'+str(failIndex)+'/*/* .tmpDone;' 
                                                                                        ## ----> Fixed # Fabio
                                                                                        ## Added also this stuff at done.tgz DS
                                                                                        ## This "4" MUST be parametric!!! otherwise 
                                                                                        ## In done.tgz I want ALL the stuff related
                                                                                        ## also to the jobs that finished even if failing.      
                                                                                        ## This means that done.tgz must conteins all the stuff
                                                                                        ## both for jobs exiting with "0" "0" and something !="0". DS 
            cmd += 'cp -r '+jtResDir+'/Success/Submission_*/*/* .tmpDone;'
        cmd += 'tar --create -z --file='+path+'/.temp_done.tgz .tmpDone/* --exclude done.tgz --exclude failed.tgz --exclude *BossChainer.log --exclude *BossProgram_1.log --exclude *edg_getoutput.log;'
        cmd += 'rm -drf .tmpDone/;'
        cmd += 'mv '+path+'/.temp_done.tgz '+path+'/done.tgz;'
        #logging.info('\n'+str(cmd)+'\n')
        os.system( cmd )
        os.chdir( work_dir )

    def prepareTarballFailed( self, path, taskName, nJob ):
        """
        _prepareTarballFailed_
        """
        work_dir = os.getcwd()
        os.chdir( path )
        cmd = 'mkdir .tmpFailed;'
        for i in range(1, nJob+1):
            ## Add parametric indexes for failed and successful jobs 
            jtResDir = 'job'+str(i)+'/JobTracking'
            ## Get the most recent failure and copy that to tmp 
            failIndex = 4 
            if os.path.exists('./'+jtResDir+'/Failed/'):
                if len(os.listdir('./'+jtResDir+'/Failed/')) > 0:
                    failIndex = max( [ int(s.split('Submission_')[-1]) for s in os.listdir('./'+jtResDir+'/Failed/') ] )
                cmd += 'cp '+self.tempxmlReportFile+' .tmpDone/'+self.xmlReportFileName+' ;'
                cmd += 'cp '+ jtResDir +'/Failed/Submission_'+str(failIndex)+'/log/edgLoggingInfo.log .tmpFailed/edgLoggingInfo_'+str(i)+'.log;'
                #logging.info('cp '+ jtResDir +'/Failed/Submission_'+str(failIndex)+'/log/edgLoggingInfo.log .tmpFailed/edgLoggingInfo_'+str(i)+'.log;')
        """
                cmd += 'cp -r '+ jtResDir +'/Failed/Submission_'+str(failIndex)+'/log/* .tmpFailed;' 
                                                                                          ## I didn' correct anything, but the failed must be preparesd 
                                                                                          ## only for the kill and the abort logginginfo file whose are in 
                                                                                          ## job1/JobTracking/Failed/Submission_*/log/   DS. 
        cmd += 'tar --create -z --file='+path+'/.temp_failed.tgz .tmpFailed/* --exclude failed.tgz --exclude done.tgz --exclude *BossChainer.log --exclude *BossProgram_1.log --exclude *edg_getoutput.log;';
        """
        cmd += 'tar --create -z --file='+path+'/.temp_failed.tgz .tmpFailed/edgLoggingInfo_*.log;' 
        cmd += 'rm -drf .tmpFailed;'
        cmd += 'mv '+path+'/.temp_failed.tgz '+path+'/failed.tgz;'
        os.system( cmd )
        os.chdir( work_dir )
 

    def undiscoverXmlFile (self, path, taskName, fromFileName, toFileName):
        if os.path.exists(path + fromFileName):
            infile = file(path + fromFileName , 'r').read()
            outfile = file(path + toFileName , 'w').write(infile)
            #infile.close()
            #outfile.close()
 

    ##########################################################################
    # publishing messages
    ##########################################################################

    def taskSuccess( self, taskPath ):
        """
        _taskSuccess_
        
        Trasmit the "TaskSuccess" event to the prodAgent
        
        """
        self.msThread.publish("TaskSuccess", taskPath)
        self.msThread.commit()

        logging.info("         *-*-*-*-*")
        logging.info("-------> Published 'TaskSuccess' message with payload: %s" % taskPath)
	logging.info("         *-*-*-*-*")

    def taskFailed( self, taskName, eMaiList, userName ):
        """
        _taskFailed_

        Trasmit the "TaskFailed" event to the prodAgent

        """
	if userName == "" or userName == None:
	    userName = "Unknown"
        payload = taskName + ":" + userName + ":" + eMaiList
        self.msThread.publish("TaskFailed", payload)
        self.msThread.commit()

        logging.info("         *-*-*-*-* ")
        logging.info("Published 'TaskFailed' message with payload: %s" % payload)
        logging.info("         *-*-*-*-* ")

    def taskNotSubmitted( self, taskPath ): #Name, eMaiList, userName ):
        """
        _taskNotSubmitted_
        """
#        if userName == "" or userName == None:
#            userName = "Unknown"
#        payload = taskName + ":" + userName + ":" + eMaiList
        self.msThread.publish("TaskNotSubmitted", taskPath) # payload)
        self.msThread.commit()

        logging.info("         *-*-*-*-* ")
        logging.info("Published 'TaskNotSubmitted' message with payload: %s" % taskPath) #payload)
        logging.info("         *-*-*-*-* ")

    def taskIncompleteSubmission( self, taskName, eMaiList, userName ):
        """
        _taskIncompleteSubmission_
        """
        if userName == "" or userName == None:
            userName = "Unknown"
        payload = taskName + ":" + userName + ":" + eMaiList
        self.msThread.publish("TaskIncompleteSubmission", payload)
        self.msThread.commit()

        logging.info("         *-*-*-*-* ")
        logging.info("Published 'TaskIncompleteSubmission' message with payload: %s" % payload)
        logging.info("         *-*-*-*-* ")


    ##########################################################################
    # polling js_taskInstance and BOSS DB
    ##########################################################################

    def pollTasks(self):
        """
        _pollTasks_

        Poll the task database
         """
        logging.info("- - - * * * * * - - -")
        logging.info("Polling task database")

        try:
            ## starting BOSS session
            mySession = BossSession( self.args['bossClads'] )

            task2Check = TaskStateAPI.getAllNotFinished()
	    for task in task2Check:
		taskName = task[0]
		eMail = task[1]
		notified = int(task[3])
		thresholdLevel = task[2]
		endedLevel = task[4]
		status = task[5]
		uuid = task[6]

                if status == self.taskState[2] and notified < 2:
		    self.prepareTaskFailed( taskName, uuid, eMail )
		else:
    	            taskDict = mySession.loadByName( taskName )
                   
		    if len(taskDict) > 0:

                        logging.info(" - - - - - - - - ")
			logging.info(taskName + " ["+str(status)+"] - num.: " + str(len(taskDict)))
			
			pathToWrite = ""
			dictReportTot = {'JobSuccess': 0, 'JobFailed': 0, 'JobInProgress': 0}
			countNotSubmitted = 0 
			dictStateTot = {}
                        dictFinishedJobs = {}
			v = taskDict.values()[0]
			statusJobsTask = v.jobStates()

			for job,stato in statusJobsTask.iteritems():
			
			    resubmitting = TaskStateAPI.checkNSubmit( taskName, job )
			    runInfoJob =  v.specific(job,"1")

			    if string.lower(self.args['jobDetail']) == "yes":
                                logging.info("STATUS = " + str(stato) + " - EXE_EXIT_CODE = "+ str(runInfoJob['EXE_EXIT_CODE']) + " - JOB_EXIT_STATUS = " + str(runInfoJob['JOB_EXIT_STATUS']))
			    vect = []
			    if runInfoJob['EXE_EXIT_CODE'] == "NULL" and runInfoJob['JOB_EXIT_STATUS'] == "NULL":
   			        vect = [self.convertStatus(stato), "", "", 0]
			    else:
                                vect = [self.convertStatus(stato), runInfoJob['EXE_EXIT_CODE'], runInfoJob['JOB_EXIT_STATUS'], 0]
                            dictStateTot.setdefault(job, vect)
			    
			    if stato == "SE" or stato == "E":
				if runInfoJob['EXE_EXIT_CODE'] == "0" and runInfoJob['JOB_EXIT_STATUS'] == "0":
				    dictReportTot['JobSuccess'] += 1
				    dictStateTot[job][3] = 1
                                    dictFinishedJobs.setdefault(job,1)
				elif not resubmitting:
				    dictReportTot['JobFailed'] += 1
                                    dictStateTot[job][0] = "Done (Failed)"
                                    dictStateTot[job][3] = 1
                                    dictFinishedJobs.setdefault(job,1)
				else:
				    dictReportTot['JobInProgress'] += 1
                                    dictStateTot[job][0] = "Resubmitting by server"#"Managing by server"
                                    dictFinishedJobs.setdefault(job,0)
			    elif stato == "SA" or stato == "SK" or stato == "K":
				if not resubmitting:
				    dictReportTot['JobFailed'] += 1
                                    dictFinishedJobs.setdefault(job,1)
				else:
				    dictReportTot['JobInProgress'] += 1
                                    dictStateTot[job][0] = "Resubmitting by server"#"Managing by server"
                                    dictFinishedJobs.setdefault(job,0)
			    elif stato == "W":
                                if not resubmitting:
   				    countNotSubmitted += 1 
				    dictReportTot['JobFailed'] += 1
                                    dictFinishedJobs.setdefault(job,1)
                                else:
                                    countNotSubmitted += 1
                                    dictReportTot['JobInProgress'] += 1
                                    dictFinishedJobs.setdefault(job,0)
			    elif not resubmitting:
   				dictReportTot['JobFailed'] += 1
                                dictFinishedJobs.setdefault(job,1)
                            else:
                                dictReportTot['JobInProgress'] += 1
                                dictFinishedJobs.setdefault(job,0)

                            ## TEMPORARY FIX
                            if status == "partially submitted" and dictStateTot[job][0] == "Submitting":
                                dictStateTot[job][0] = "NotSubmitted"
			
			rev_items = [(v, int(k)) for k, v in dictStateTot.items()]
			rev_items.sort()
			dictStateTot = {}
			for valu3, k3y in rev_items:
			    dictStateTot.setdefault( k3y, valu3 )
			for state in dictReportTot:
			    logging.info( " Job " + state + ": " + str(dictReportTot[state]) )
			if countNotSubmitted > 0:
			    logging.info( "    -> of which not yet submitted: " + str(countNotSubmitted) )

			endedJob = dictReportTot['JobSuccess'] + dictReportTot['JobFailed']

			try:
			    percentage = (100 * endedJob) / len(statusJobsTask)
			    pathToWrite = str(self.args['dropBoxPath']) + "/" + taskName + "/" + self.resSubDir

                            if os.path.exists( pathToWrite ):
                                self.prepareReport( taskName, uuid, eMail, thresholdLevel, percentage, dictStateTot, len(statusJobsTask),1 )
                            else:
                                logging.info("Error: the path " + pathToWrite + " does not exist!\n" )


			    if percentage != endedLevel or \
			       (percentage == 0 and status == self.taskState[3] ) or \
			       (percentage == 0 and status == self.taskState[1] ) or \
			       (notified < 2 and endedLevel == 100):

		 	        ###  updating endedLevel  ###
				if endedLevel == 100:
                                    TaskStateAPI.updatingEndedPA( taskName, str(percentage), self.taskState[5])
				elif percentage != endedLevel:
				    TaskStateAPI.updatingEndedPA( taskName, str(percentage), status)
				   ### prepare tarball & send eMail ###
                                    if percentage != endedLevel:
                                        obj = Outputting( self.xmlReportFileName, self.tempxmlReportFile )
                                        logging.info("**** ** **** ** ****")
                                        logging.info("TESTING: preparing output")
                                        obj.prepare( pathToWrite, taskName, len(statusJobsTask), dictFinishedJobs )
                                        if os.path.exists( pathToWrite+"/done.tar.gz" ):
                                            logging.info("TESTING: preparing output finished")
                                        else:
                                            logging.info("TESTING: preparing output FAILED")
                                        logging.info("**** ** **** ** ****")
                                #        self.prepareTarballDone(pathToWrite, taskName, len(statusJobsTask) )
                                    if percentage >= thresholdLevel:
                                        if dictReportTot['JobFailed'] > 0:
                                            self.prepareTarballFailed(pathToWrite, taskName, len(statusJobsTask) )
					if percentage == 100:
					    self.taskSuccess( pathToWrite + self.xmlReportFileName )
					    notified = 2
					    TaskStateAPI.updatingNotifiedPA( taskName, notified )
                                            obj.deleteTempTar( pathToWrite )
					elif notified <= 0:
					    self.taskSuccess( pathToWrite + self.xmlReportFileName )
					    notified = 1
					    TaskStateAPI.updatingNotifiedPA( taskName, notified )
			    elif status == '':
			        if dictReportTot['JobSuccess'] + dictReportTot['JobFailed'] + dictReportTot['JobInProgress'] > countNotSubmitted:
				    TaskStateAPI.updateTaskStatus( taskName, self.taskState[3] )
				else:
				    TaskStateAPI.updateTaskStatus( taskName, self.taskState[2] )
                            self.undiscoverXmlFile( pathToWrite, taskName, self.tempxmlReportFile, self.xmlReportFileName )
 
			except ZeroDivisionError, detail:
			    logging.info("  <-- - -- - -->")
			    logging.info("WARNING: No jobs in the task " + taskName )
			    logging.info("         deatil: " + str(detail) )
			    logging.info("  <-- - -- - -->")
		    else:
			logging.debug("-----")
			logging.debug( "Skipping task "+ taskName )

                    ##clear tasks from memory
                    mySession.clear()
            
        except BossError,e:
            logging.info( e.__str__() )

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
        self.ms.subscribeTo("SetThresholdLevel")
        self.ms.subscribeTo("CrabServerWorkerComponent:TaskArrival")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkFailed")
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformedPartial")
	self.ms.subscribeTo("DropBoxGuardianComponent:NewFile")
	self.ms.subscribeTo("ProxyTarballAssociatorComponent:WorkDone")
	self.ms.subscribeTo("ProxyTarballAssociatorComponent:UnableToManage")
        self.ms.subscribeTo("TaskKilled")
        self.ms.subscribeTo("TaskKilledFailed")


        # start polling thread
        pollingThread = PollThread(self.pollTasks)
        pollingThread.start()

        # wait for messages
        while True:
            messageType, payload = self.ms.get()
            self.ms.commit()
            self.__call__(messageType, payload)

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
    
    def __init__(self, poll):
        """
        __init__

        Initialize thread and set polling callback

        Arguments:
        
          poll -- the task polling function
        """

        Thread.__init__(self)
        self.poll = poll;

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
                self.poll()

            # error
            
            except Exception, ex:

                # log error message
                logging.error("Error in polling thread: " + str(ex))

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
            

