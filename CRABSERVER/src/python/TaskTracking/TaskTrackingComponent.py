#!/usr/bin/env python
"""
_TaskTracking_

"""

__revision__ = "$Id$"
__version__ = "$Revision$"

import os
import time
import sys

# BOSS API import
from ProdAgentBOSS import BOSSCommands
from BossSession import *

# Message service import
from MessageService.MessageService import MessageService

# threads
from threading import Thread

# logging
import logging
from logging.handlers import RotatingFileHandler

# DB PA
from JobState.Database.Api import JobStateInfoAPIMySQL
from JobState.Database.Api.RacerException import RacerException
from JobState.Database.Api.RetryException import RetryException
from JobState.Database.Api.RunException import RunException
from JobState.Database.Api.SubmitException import SubmitException
from JobState.Database.Api.TransitionException import TransitionException
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect

# XML
from CreateXmlJobReport import *

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
        self.args.setdefault("PollInterval", 60 )
        self.args.setdefault("Logfile", None)
        self.args.setdefault("bossClads", "/home/serverAdmin/CrabProdAgent/PRODAGENT/BOSS/config/")
        self.args.setdefault("dropBoxPath", "/data/SEDir/")

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
        if event == "CrabServerWorkerComponent:CrabWorkPerformed":
            if payload != None:
                logging.info("CrabWorkPerformed: %s" % payload)
                self.insertNewTask(payload)
            else:
                logging.error("ERROR: empty payload from 'CrabServerWorkerComponent:CrabWorkPerformed'!!!!")
            return

        # set threshold
        if event == "SetThresholdLevel":
            logging.info("SetThresholdLevel: %s" % payload)
            self.insertNewTask( payload )
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
        logging.debug("Unexpected event %s, ignored" % event)

    ##########################################################################
    # insert task in database
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


    def insertNewTask(self, payload):
        """
        _insertTaskPA_

        insert a new task, already submitted, in the PA's DB
        """

        eMail, thresholdLevel = self.readInfoCfg( self.args['dropBoxPath'] + "/" + payload )
        if eMail == None:
            logging.error( "ERROR: missing 'eMail' from " + self.crabcfg + " for task: " + str(payload) )
            eMail = "mattia.cinquilli@pg.infn.it"  #### TEMPORARY SOLUTION!
        if thresholdLevel == None:
            logging.error( "WARNING: missing 'thresholdLevel' from " + self.crabcfg + " for task: " + str(payload) )
            logging.error( "         using default value 'thresholdLevel = 100'")
            thresholdLevel = 100

        try:
            conn, dbCur = self.openConnPA()
            self.insertTaskPA( conn, dbCur, payload, eMail, thresholdLevel )
            ## closing connection with PA's DB
            self.closeConnPA( conn, dbCur )
        except:
            logging.error( "ERROR while inserting the task " + str(payload) )

        return


    ##########################################################################
    # poll task database
    ##########################################################################

 ### #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -# ###
 ### #- - -                      DB INTERACTION METHODS                       - - -# ###
 ### #-                                                                           -# ###

    
    def openConnPA(self):
        """
        _openConnPA_

        opening connection with the PA DB
        """
        try:
            conn=connect(False)
            dbCur=conn.cursor()
            logging.debug( "Conn opened\n" )
            return conn, dbCur
        except:
            logging.info("Error connnecting to DB!")
            self.closeConnPA(conn, dbCur)
            raise

    def closeConnPA(self, conn, dbCur):
        """
        _closeConnPA_

        closing connection with the PA DB
        """
        try:
            dbCur.close()
            conn.close()
            logging.debug("Conn closed\n")
        except:
            logging.info("Error closing connession with DB!")
            raise

    def checkNSubmit(self, conn, dbCur, taskName, idJob):
        try:
            dbCur.execute("START TRANSACTION")
            sqlStr='SELECT MaxRetries, Retries from js_JobSpec where JobSpecID="'+taskName+'_'+idJob+'" ;'
            dbCur.execute(sqlStr)
            rows = dbCur.fetchall()
            dbCur.execute("COMMIT")
            #logging.info("ROW = " + str(rows))
            if len(rows) == 1:
                if rows[0][0] == rows[0][1]:
                    return 0
            #logging.info("ROW = " + str(rows))
        except:
            dbCur.execute("ROLLBACK")
            logging.info( "Error quering PA DB!" )
            raise
        return 1

    def insertTaskPA( self, conn, dbCur, taskName, eMail, thresholdLevel ):
        """
        _insertTaskPA_
        """

        notificationSent = 0
        endedLevel = 0
        try:
            dbCur.execute("START TRANSACTION")
            sqlStr="INSERT INTO js_taskInstance VALUES('','"+taskName+"','"+eMail+"','"+str(thresholdLevel)+"','"+str(notificationSent)+"','"+str(endedLevel)+"');"
            dbCur.execute(sqlStr)
            logging.debug("Inserted task " + taskName)
            dbCur.execute("COMMIT")
            logging.info("New Task ("+taskName+") inserted in the PA's DB")
        except:
            dbCur.execute("ROLLBACK")
            logging.error( "Error inserting a new task ("+ taskName +") in the PA's DB!" )
            raise


        return

    def findTaskPA(self, conn, dbCur, taskName):
        """
        _findTaskPA_

        Query the PA database

        CREATE TABLE js_taskInstance (
           id int NOT NULL auto_increment,
           taskName varchar(255) NOT NULL default '',
           eMail varchar(255) NOT NULL default '',
           tresholdLevel int (3) UNSIGNED NOT NULL default '100',
           notificationSent int (1) NOT NULL default '0',
           primary key(id),
           unique(taskName),
           key(taskName)
        ) TYPE = InnoDB DEFAULT CHARSET=latin1;

        """

        try:
            dbCur.execute("START TRANSACTION")
            sqlStr='SELECT eMail,tresholdLevel,notificationSent,endedLevel from js_taskInstance WHERE taskName="'+taskName+'" AND endedLevel <> 100;'
            dbCur.execute(sqlStr)
            logging.debug("Query done")
            row = dbCur.fetchall()
            #for row in rows:
            #    print "%s, %s" % (row[0], row[1])
            dbCur.execute("COMMIT")
            logging.debug( "Query: " +sqlStr )
            logging.debug( "row in findTaskPA: " + str(row) )
            if len(row) > 0:
                return row[0]
            else:
                return None
        except:
            dbCur.execute("ROLLBACK")
            logging.info( "Error quering PA DB!" )
            raise


    def checkExistPA(self, conn, dbCur, taskName):
        """
        _checkExistPA_

        call this ONLY from inside a TRANSACTION
        """
        sqlStr='SELECT eMail from js_taskInstance WHERE taskName="'+taskName+'";'
        dbCur.execute(sqlStr)
        row = dbCur.fetchall()
        if len(row) == 1:
            return 1
        return 0


    def updatingEndedPA(self, conn, dbCur, taskName, newPercentage):
        """
        _updatingEndedPA_
        """
        logging.info( "   Updating the task table for task: " + taskName )
        logging.debug( "   Setting the field endedLevel at '" + newPercentage +"'")
        try:
            dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
                sqlStr='UPDATE js_taskInstance SET endedLevel="'+newPercentage+'"\
                        WHERE taskName="'+taskName+'";'
                rowModified=dbCur.execute(sqlStr)
                dbCur.execute("COMMIT")
        except:
            dbCur.execute("ROLLBACK")
            logging.info( "Error updating PA DB!" )
            raise

        
    def updatingNotifiedPA(self, conn, dbCur, taskName, sended):
        """
        _updatingNotified_
        """

        sendFlag = str(sended)
        logging.info( "   Updating the task table for task: " + taskName )
        logging.debug( "   Setting the field notificationSend at '" + sendFlag +"'")
        try:
            dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
                sqlStr='UPDATE js_taskInstance SET notificationSent="'+sendFlag+'"\
                        WHERE taskName="'+taskName+'";'
                rowModified=dbCur.execute(sqlStr)
                dbCur.execute("COMMIT")
#            else:
#                Excepiton
        except:
            dbCur.execute("ROLLBACK")
            logging.info( "Error updating PA DB!" )
            raise

    def cleaningTaskPA(self, conn, dbCur, taskName):
        """
        _cleaningTaskPA_

        cleaning task from PA's DB
        """

        logging.info( "   Cleaning the task table from task: " + taskName )
        try:
            dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
                sqlStr='DELETE from js_taskInstance WHERE taskName="'+taskName+'";'
                rowModified=dbCur.execute(sqlStr)
                dbCur.execute("COMMIT")
        except:
            dbCur.execute("ROLLBACK")
            logging.info( "Error updating PA DB!" )
            raise
 ### #- -                                                                       - -# ###
 ### #- - - - - - - -                                               - - - - - - - -# ###
 ### #- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -# ###

    def prepareTarball( self, path, taskName ):
        """
        _prepareTarball_

        """
        work_dir = os.getcwd()
        os.chdir( path )#+ '/../' )
        logging.debug("path: " + str(path))
        os.system('tar --create -z --file='+path+'/done.tgz job* --exclude done.tgz --exclude xmlReportFile.xml')
        os.chdir( work_dir )
        return

    def taskSuccess( self, taskPath ):
        """
        _taskSuccess_
        
        Trasmit the "TaskSuccess" event to the prodAgent
        
        """
        logging.info( "Publishing 'TaskSuccess' with payload=" + taskPath )
        self.msThread.publish("TaskSuccess", taskPath)
        self.msThread.commit()

        logging.info("published 'TaskSuccess' message with payload : %s" % taskPath)


    def pollTasks(self):
        """
        _pollTasks_

        Poll the task database
         """

        logging.info("Polling task database")

        xmlReportFileName = "xmlReportFile.xml"
        resSubDir = "res/"

        # add here polling code
        try:
            ## opening connection with PA's DB
            conn, dbCur = self.openConnPA()

            ## starting BOSS session
            mySession = BossSession( self.args['bossClads'] )

            ## query the BOSS' DB with the API
            taskDict = mySession.query(ALL, avoidCheck = 1)

            ## cycle for every task
            for k,v in taskDict.iteritems():
      ### - - - INIT TASK: *start* - - - ###
                task = v.jobsDict()
          #      logging.info( "-----" )
          #      logging.info("Task name: " + str(v.name()) )
                ## path where to write the report file
                pathToWrite = ""
                ## counter of the total number of jobs
                counterTot = 0

                ## allowed state in a dictionary for count how many jobs there are in each state
                dictReportTot = {'JobSuccess': 0, 'JobFailed': 0, 'JobInProgress': 0}
      ### - - - INIT TASK: *end* - - - ###

      ### - - - EVALUATING THE TASK: *start* - - - ###
                ## selecting info from task table
                info = self.findTaskPA( conn, dbCur, v.name() )
                if info != None:
                    logging.info( "-----" )
                    logging.info("Task name: " + str(v.name()) )
          ### - - - COUNTING EACH STATUS: *start* - - - ###
                    ## cycle for every job in the current task 'v'
                    for k1, v1 in task.iteritems():
                        counterTot += 1
                        ## here increment job status to report in the dictionary
                        resubmitting = self.checkNSubmit( conn, dbCur, v.name(), v1['CHAIN_ID'])
                        if v1['STATUS'] == "SA" or  v1['STATUS'] == "SK" or v1['STATUS'] == "K":
                            if not resubmitting:
                                dictReportTot['JobFailed'] += 1
                            else:
                                dictReportTot['JobInProgress'] += 1
                        elif v1['STATUS'] == "SD":
                            programs = v.jobPrograms( v1['CHAIN_ID'])
                            for k2,v2 in programs.iteritems():
                                logging.debug("  exe_exit_code = " + str(v2['EXE_EXIT_CODE']) )
                                logging.debug("  job_exit_status = " + str(v2['JOB_EXIT_STATUS']) )
                            if str(v2['EXE_EXIT_CODE']) == "0" and str(v2['JOB_EXIT_STATUS']) == "0":
                                dictReportTot['JobSuccess'] += 1
                            elif not resubmitting:
                                dictReportTot['JobFailed'] += 1
                            else:
                                dictReportTot['JobInProgress'] += 1
                        elif v1['STATUS'] == "E":
                            programs = v.jobPrograms( v1['CHAIN_ID'])
                            for k2,v2 in programs.iteritems():
                                logging.debug("  exe_exit_code = " + str(v2['EXE_EXIT_CODE']) )
                                logging.debug("  job_exit_status = " + str(v2['JOB_EXIT_STATUS']) )
                            if str(v2['EXE_EXIT_CODE']) == "0" and str(v2['JOB_EXIT_STATUS']) == "0":
                                dictReportTot['JobSuccess'] += 1
                            elif not resubmitting:
                                dictReportTot['JobFailed'] += 1
                            else:
                                dictReportTot['JobInProgress'] += 1
                        else:
                            dictReportTot['JobInProgress'] += 1
           ### - - - COUNTING EACH STATUS: *end* - - - ###

              ### - - - ADJUST INFO FROM PA's DB: *start* - - - ###
                    for state in dictReportTot:
                        logging.info( " Job " + state + ": " + str(dictReportTot[state]) )
                    logging.debug(str(info)) ## debug
                    try:
                        ## calculating the percentage
                        endedJob = dictReportTot['JobSuccess'] + dictReportTot['JobFailed']
                        percentage = (100 * endedJob) / counterTot
                        logging.info("PercentageEnded = " + str(percentage) + "  - JobEnded = " + str(endedJob) + " - TotalJob = " + str(counterTot) )
                        thresholdLevel = 100
                        eMail = info[0]
                        thresholdLevel = info[1]
                        notified = info[2]
                        endedLevel = info[3]
                        if eMail == None:
                            eMail = "no_eMail_registered"
              ### - - - ADJUST INFO FROM PA's DB: *end* - - - ###

                        pathToWrite = str(self.args['dropBoxPath']) + "/" + v.name() + "/" + resSubDir
                        if percentage != endedLevel or (percentage == 0 and (not os.path.exists(pathToWrite + xmlReportFileName)) ):
                  ### - - - UPDATING endedLevel: *start* - - - ###
                            self.updatingEndedPA( conn, dbCur, str(v.name()), str(percentage) )
                  ### - - - UPDATING endedLevel: *end* - - - ###

                  ### - - - GET USER NAME & ORIGINAL TASKNAME: *start* - - - ###
                            obj = UtilSubject(self.args['dropBoxPath'], str(v.name()))
                            par1, par2 = obj.getInfos()
                            origTaskName, userName = obj.getOriginalTaskName(par1, par2)
                  ### - - - GET USER NAME & ORIGINAL TASKNAME: *end* - - - ###

                  ### - - - XML REPORT: *start* - - - ###
                            c = CreateXmlJobReport()
                            c.initialize( origTaskName, eMail, userName, percentage, thresholdLevel) 
                            for state in dictReportTot:
                                c.addStatusCount( str(state), str(dictReportTot[state]) )
                            c.toXml()
#                            pathToWrite = str(self.args['dropBoxPath']) + "/" + v.name() + "/" + resSubDir
                            if os.path.exists( pathToWrite ):
                                c.toFile( pathToWrite + xmlReportFileName )
                            else:
                                logging.info("Error: the path " + pathToWrite + " does not exist!\n" )
                  ### - - - XML REPORT: *end* - - - ###

                  ### - - - SEND REPORT: *start* - - - ###
                            ## check if the level is reached
                            if percentage >= thresholdLevel and eMail != "no_eMail_registered":
                                if notified == 0:
                                    logging.info("Send report for task " + v.name() )
                                    if os.path.exists( pathToWrite ):
                                        ## preparing tarball
                                        logging.info("Preparing tarball...")
                                        self.prepareTarball( pathToWrite, str(v.name()) )
                                        ## activating component Notifica
                                        self.taskSuccess( pathToWrite + xmlReportFileName )
                                        if percentage == 100:
                                            notified = 2
                                            self.updatingNotifiedPA( conn, dbCur, str(v.name()), notified )
                                        else:
                                            notified = 1
                                            self.updatingNotifiedPA( conn, dbCur, str(v.name()), notified )
                                    else:
                                        logging.info("Error: the path " + pathToWrite + " does not exist!" )

                                if percentage == 100 and notified != 2:
                                    logging.info(" Task '" + str(v.name()) + "' completed!")
                                    logging.info("Updating tarball...")
                                    self.prepareTarball( pathToWrite, str(v.name()) )
                                    self.taskSuccess( pathToWrite + xmlReportFileName )
                                    notified = 2
                                    self.updatingNotifiedPA( conn, dbCur, str(v.name()), notified )
                                    ##self.cleaningTaskPA( conn, dbCur, str(v.name()) )
                                else:
                                    logging.info("Report for task " + str(v.name()) + " already sended" )
                  ### - - - SEND REPORT: *end* - - - ###
                    except ZeroDivisionError, detail:
                        logging.info("WARNING: No jobs in the task " + v.name() ) 
                        logging.info("         deatil: " + str(detail) )
                        #self.cleaningTaskPA( conn, dbCur, str(v.name()) )
                        logging.info( "-----" )
                else:
                    logging.debug( "Skipping task "+ str(v.name()) )
#                    logging.error( "-----")
      ### - - - EVALUATING THE TASK: *end* - - - ###

            #clear tasks from memory
            mySession.clear()
            
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )

        except BossError,e:
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
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
        self.ms.subscribeTo("CrabServerWorkerComponent:CrabWorkPerformed")

        # start polling thread
        pollingThread = PollThread(self.pollTasks)
        pollingThread.start()

#        conn, dbCur = self.openConnPA()
#        try:
#            dbCur.execute("START TRANSACTION")
#            sqlStr='SELECT MaxRetries Retries from js_JobSpec where JobSpecID="'+taskName+'" ;'
#            dbCur.execute(sqlStr)
#            rows = dbCur.fetchall()
#            dbCur.execute("COMMIT")
#        except:
#            dbCur.execute("ROLLBACK")
#            logging.info( "Error quering PA DB!" )
 #           raise
 #       self.closeConnPA( dbCur, conn )

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

