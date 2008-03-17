#!/usr/bin/env python
"""
_TaskLifeManager_

"""

__revision__ = "$Id: TaskLifeManagerComponent.py,v 1.4 2008/01/28 15:14:30 mcinquil Exp $"
__version__ = "$Revision: 1.4 $"

# Message service import
from MessageService.MessageService import MessageService

# logging
import logging
from logging.handlers import RotatingFileHandler

# module fro task and task queue management
from Task import *
from TaskQueue import *

# module from TaskTracking component
from TaskTracking.UtilSubject import UtilSubject
from TaskTracking.TaskStateAPI import getStatusUUIDEmail

# SE_API
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface
from ProdCommon.Storage.SEAPI.SElement import SElement

import os
import time
import pickle



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
        self.args.setdefault("dropBoxPath", None)
	self.args.setdefault("levelAvailable", 15)
        self.args.setdefault("taskLife", "360:00:00")
	self.args.setdefault("eMailAdmin", os.environ['USER'])
        self.args.setdefault("pollingTimeCheck", 10*60)
        self.args.setdefault("Protocol", "local")
        self.args.setdefault("SEHostname", "localhost")
        self.args.setdefault("SEPort", None)
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
                            self.args["SEHostname"],
                            self.args["Protocol"], \
                            self.args["SEPort"] \
                          )
        self.SeSbI = SBinterface(storage)

        #######################
        ## args constraints ##
        #####################
        # which dbox ?!?
        if self.args['dropBoxPath'] == None:
            self.args['dropBoxPath'] = self.args['ComponentDir']
        logging.info("Using " +str(self.args['dropBoxPath']))

        # minimum space available
        if int(self.args['levelAvailable']) < 5:
	    logging.info("Configuration porblem")
            logging.info("  Too high value for [levelAvailable];")
            logging.info("  setting to default (15%).")
            self.args['levelAvailable'] = 15

        logging.info(" Setting task life time at: "\
                          + self.args['taskLife'].split(":", 1)[0] + "hours "\
                          + self.args['taskLife'].split(":", 2)[1] + "mins "\
                          + self.args['taskLife'].split(":", 2)[2] + "secs" )
        # minimum time polling
        if int(self.args['pollingTimeCheck'] < 3):
            self.args['pollingTimeCheck'] = 3 * 60

        #####################
        ###    Queues    ###
        ###################
        ## contains all tasks arrived in the dbox
        self.taskQueue = TaskQueue()
            ## its pickle
        self.taskQueuePKL = "buildDropBox.pkl"
        ## groups tasks to be removed from the dbox
        self.taskDeleteQueue = TaskQueue()
            ## its pickle
        self.taskDeletePKL = "toDelete.pkl"
        ## groups tasks completely ended but yet "fresh"
        self.taskEndedQueue = TaskQueue()
            ## its pickle
        self.taskEndedPKL = "ended.pkl"

        ###################################
        ### checks dbox & fills queues ###
        #################################
        self.loadFromPickle()
        #self.buildDropBox()


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
            self.pollDropBox()
            return            #
        #######################

        #######################
        # New Task Arrival
        # inserting on task queue
        if event == "CRAB_Cmd_Mgr:NewTask" or \
               event == "TaskLifeManager::TaskToMange":
            try:
                if payload.split(".")[-1] != "xml" and payload.split(".")[1] != "tgz":
                    self.insertTaskWrp( payload )
            except:
                self.insertTaskWrp( payload )
            return            #
        #######################
         
        #######################
        # Task Finished
        # updating ended time on object
        if event == "TaskTracking:TaskEnded":
            self.insertEndedWrp( payload )
            return            #
        #######################

        if event == "TaskLifeManager::PrintTaskInfo":
            if payload != "" and payload != None:
                self.printTaskInfo( payload )
            else:
                logging.error("No task specified for " + str(event) )
            return

        #if event == "TaskLifeManager:OverAvailableSpace":
        #    return

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
    # utilty methods (user, time, convert)
    ##########################################################################

    def checkInfoUser( self, taskName):
        """
        _CheckInfoUser_
        """
        own = " "
        mail = " " 
        taskPath = self.args['dropBoxPath'] + "/" + taskName + "_spec"
        try:
            import xml.dom.minidom
            import xml.dom.ext
            from os.path import join
            if self.SeSbI.checkExists( join(taskPath, "xmlReportFile.xml")):
                fileXml = None
                try:
                    fileXml = open( join(taskPath, "xmlReportFile.xml"), "r")
                    doc  = xml.dom.minidom.parse( \
                                        join(taskPath, "xmlReportFile.xml") \
                                                 )
                    own  = doc.childNodes[0].childNodes[1].getAttribute("owner")
                    mail = doc.childNodes[0].childNodes[1].getAttribute("email")
                    fileXml.close()
                    return own, mail
                except Exception, ex:
                    import traceback
                    logging.error( "Exception raised: " + str(ex) ) 
                    logging.error( str(traceback.format_exc()) )
                    fileXml.close()
            else:
                return None, None

        except Exception, ex:
            import traceback
            logging.error( str(traceback.format_exc()) )

        return own, mail


    def setVal(self, val):
        """
        _setVal
        """
        if len(str(val)) == 1:
            return ("0" + str(val))
        return str(val)

    def calcFromSeconds(self, totSec):
        """
        _calcFromSeconds_
        """
         
        secondi = totSec % 60
        temp = int(totSec / 60)
        minuti = temp % 60
        ore = int(temp / 60)

        return ( self.setVal(ore) + ":" +\
                 self.setVal(minuti) + ":" +\
                 self.setVal(secondi) )

    def totSeconds(self, timeStr):
        """
        _totSeconds_
        """
        hh, mm, ss = timeStr.split(":")
        return ((int(hh)*60) + int(mm))*60 + int(ss)


    ##########################################################################
    # os level work
    ##########################################################################

    def getDirSpace( self, taskPath ):
        """
        _getDirSpace_
        """
        try:
            return self.SeSbI.getDirSpace(taskPath)
        except Exception, ex:
            import traceback
            logging.error( "Exception raised: " +str(ex) )
            logging.error( str(traceback.format_exc()) )
            return 0


    def checkGlobalSpace( self ):
        """
        _checkGlobalSpace_
        """
        out = self.SeSbI.getGlobalSpace(self.args['dropBoxPath'])
        logging.info ( str(out) )
        numberUsed = int(out[0].split("%")[0])
        spaceAvail = int(out[1])
        spaceUsed = int(out[2])
        tmpAvail = 100-numberUsed
        logging.info (" [Used Space is " + str(numberUsed) + "%]")
        logging.info (" [Avail Space is " + str(tmpAvail) + "%]")

        lev = int(self.args['levelAvailable'])

        toliberate = 0
        if  tmpAvail <= lev:
            percentagetoliber = lev - tmpAvail
            toliberate = (spaceAvail+spaceUsed)*percentagetoliber/100

            logging.info(" [Need to clean " + str(toliberate) + "]")
            logging.info("   [" + str(percentagetoliber) + "%]")
         
            return 1, numberUsed, toliberate
        else:
            return 0, numberUsed, toliberate


    def deleteTask( self, taskPath ):
        """
        _deleteTask_

        recurisvely delete all the files and dirs in [taskPath]
        """
        summ = 0
        if taskPath != "/" and taskPath != self.args['dropBoxPath']:
            try:
                summ = self.SeSbI.getDirSpace(taskPath)
                self.SeSbI.delete( taskPath )
            except Exception, ex: ### 2 IMPROVE!!!! ##
                import traceback
                logging.error( str(traceback.format_exc()) )
                logging.error("Error removing path: " + str(taskPath) )
                logging.error("                     " + str(ex) )
                return 0
        else:
            logging.error( "Task already removed or wrong task name" )
            logging.error( "  path: " +str(taskPath) )

        return summ


    def cleanTask( self, taskName ):
        """
        _cleanTask_
        """
        dBox = self.args['dropBoxPath']
        from os.path import join
        pathTask = join(dBox, taskName)
        if self.SeSbI.checkExists( pathTask ):
            logging.debug("removing task '" + pathTask + "' ...")
            try:
                summ = self.deleteTask( pathTask )
                if not self.SeSbI.checkExists( pathTask ):
                    logging.debug( "removed " + str(summ) + " bytes.")
                    return summ
                else:
                    logging.error( "could not remove task ["+taskName+"]!")
            except Exception, ex:
                import traceback
                logging.error( "Exception rasied: " +str(ex) )
                logging.error( str(traceback.format_exc()) )
                logging.error("Not able to delete the task [" + taskName +\
                              "] in the path [" + dBox +"]")
        else:
            logging.error("The pasth [" + pathTask +"] does not exists!")
        return 0

    ##########################################################################
    # task queue functionalities
    ##########################################################################

    def insertTaskWrp( self, taskName, endedTime = 0 ):
        """
        _insertTaskWrp_

        adding the new arrived task to the queue
        """
        ## checks if already is in the queue
        if not self.taskQueue.exists( taskName ):
            from os.path import isdir, join
            taskPath = join( self.args['dropBoxPath'], taskName )
            ## checks if is already unpacked
            if isdir( taskPath + "_spec" ):
                ## getting user info
                owner, mail = self.checkInfoUser( taskName )
                if owner is None and mail is None:
                    ############################
                    ### auto-registering msg ###
                    ##  to wait task unpack.  ##
                    #  self.unpackingTask( taskName )
                    ############################
                    pass
                ############################
                ### creating Task object ###
                ##  & inserting on queue  ##
                taskObj = Task(\
                                taskName, \
                                owner, \
                                mail, \
                                self.totSeconds(self.args['taskLife']), \
                                endedTime, \
                                str( os.stat( taskPath ).st_ctime ), \
                                self.getDirSpace(taskPath) \
                              )
                self.taskQueue.insert( taskObj )
                self.dumPickle( self.taskQueuePKL, self.taskQueue.getAll() )
                ############################
            else:
                ############################
                ### auto-registering msg ###
                ##  to wait task unpack.  ##
                self.unpackingTask( taskName )
                ############################

    def insertEndedWrp( self, taskName ):
        """
        _insertEndedWrp_

        updates the info for an ended task
        """
        ## checks if already is in the queue
        if not self.taskQueue.exists( taskName ):
            ## inserting with the ended time updated
            self.insertTaskWrp( taskName, time.time() )
        else:
            from os.path import isdir, join
            taskPath = join( self.args['dropBoxPath'], taskName + "_spec" )
            ## checks if is already unpacked
            if isdir( taskPath + "_spec" ):
                ## retrieving the object from the queue
                task = self.taskQueue.getbyName( taskName )
                ## updating the endedtime for the object
                task.updateEndedTime()
                ## updating pickle
                self.dumPickle( self.taskQueuePKL, self.taskQueue.getAll() )

    def printTaskInfo( self, taskName ):
        """
        _printTaskInfo_
        """
        taskObj = self.taskQueue.getbyName( taskName )
        logging.info("\n Task:    " + str(taskObj.getName()) + \
                     "\n user:    " + str(taskObj.getOwner()) + \
                     "\n size:    " + str(taskObj.getSize()) + \
                     "\n to live: " + str(taskObj.toLive()) + \
                     "\n e-mail:  " + str(taskObj.getOwnerMail()) \
                    )

    ##########################################################################
    # pickling functionalities
    ##########################################################################

    def dumPickle( self, workQueue, workData ):
        """
        _dumPickle_
        """
        from os.path import join
        try:
            workQueueTemp = workQueue + ".temp"
            output = open( workQueueTemp, 'w')
            pickle.dump( workData, output, -1 )
            output.close()
            cmd = "mv " + join( self.args['ComponentDir'], workQueueTemp )+ \
                   " " + join( self.args['ComponentDir'], workQueue )
            os.popen( cmd )
        except:
            workQueueTemp = workQueue + ".temp"
            output = open( workQueueTemp, 'w')
            pickle.dump( workData, output, -1 )
            output.close()
            cmd = "mv " + join( self.args['ComponentDir'], workQueueTemp )+ \
                   " " + join( self.args['ComponentDir'], workQueue )
            os.popen( cmd )


    def loadFromPickle( self ):
        """
        _loadFromPickle_

        loading the pickle dictionary
             recreating the Task objects
                   and putting on the queue
        """
        if os.path.exists(self.taskQueuePKL):
            queueTasks = {}
            try:
                inputF = open(self.taskQueuePKL, 'r')
                queueTasks = pickle.load(inputF)
                inputF.close()
                logging.info("status loaded from file")
            except IOError, ex:
                import traceback
                logging.error( "Exception raised: " + str(ex) )
                logging.error( str(traceback.format_exc()) )
                logging.info( "problems re-loading status..." )
                return None

            for valu3, k3y in queueTasks.iteritems():
                if not self.taskQueue.exists(k3y['taskName']):
                    notif = False
                    try:
                        notif = k3y['notified']
                    except:
                        pass
                    taskObj = Task(\
                                     k3y['taskName'], \
                                     k3y['owner'], \
                                     k3y['mail'], \
                                     k3y['lifetime'], \
                                     k3y['endedtime'], \
                                     k3y['heretime'], \
                                     k3y['size'], \
                                     notif \
                                 )
                    self.taskQueue.insert( taskObj )
        return None


    ##########################################################################
    # work on dropBox  disk space management
    ##########################################################################

    def buildDropBox( self ):
        """
        _buildDropBox_
        """
        tasks = self.SeSbI.getList(self.args['dropBoxPath'])
        logging.info( "   building the drop_box directory..." )
        if tasks != None and tasks != []:
            for task in tasks:
                if task.find("crab_") != -1:
                    self.insertTaskWrp( task )
            self.dumPickle( self.taskQueuePKL, self.taskQueue.getAll() )

    ##########################################################################
    # checks over the queues
    ##########################################################################

    def checkDelete( self ):
        sign = 0
        for index in range( self.taskQueue.getHowMany() ):
            task = self.taskQueue.getCurrentSwitch()
            if task.toLive() < (60*60*24): ## twentyfour hours
                if not task.getNotified():
                    self.notifyCleaning( task.getName(), task.toLive(), task.getOwner(), task.getOwnerMail() )
                    logging.info("       - notified: "+str(task.getNotified()))
                    task.notify()
                    logging.info("       - notified: "+str(task.getNotified()))
                    sign = 1
                if task.toLive() < (60*60*10): ## ten hours
                    if task.toLive() >= 0:
                        logging.info ( "task ("+str(index)+") ["+task.getName()+"] " + \
                                       "still living for: " + str(task.toLive()) )
                    else:
                        logging.info ( "task ("+str(index)+") ["+task.getName()+"] " + \
                                       "dead from: " + str(task.toLive()) )
                        self.taskDeleteQueue.insert(task)
        if sign == 1:
            self.dumPickle( self.taskQueuePKL, self.taskQueue.getAll() )
    def deleteTasks( self ):
        toDelete = self.taskDeleteQueue.getHowMany()
        totFreeSpace = 0
        for index in range( toDelete ):
            task = self.taskDeleteQueue.getCurrentSwitch()
            logging.info ( "task life expired " + task.getName() )
            summ = self.cleanTask( task.getName() )
            if summ > 0:
                logging.debug( "  -- deleted " + str(summ) + " bytes --" )
            self.taskDeleteQueue.remove( task )
            self.taskEndedQueue.remove( task )
            self.taskQueue.remove( task )
            self.dumPickle( self.taskQueuePKL, self.taskQueue.getAll() )
            totFreeSpace += summ
        if toDelete > 0:
            logging.info( str(toDelete) + " tasks removed" )
            logging.info( str(totFreeSpace/1024) + " KB cleaned")
            logging.info( "" )
            self.dumPickle( self.taskQueuePKL, self.taskQueue.getAll() )
 
    ##########################################################################
    # component publish
    ##########################################################################

    def spaceOverNotify( self, levelReached ):
        """
	_spaceOverNotify_

	Transmit the event "OverAvailableSpace" to the Notification Component
	
	"""

       	mexage = "TaskLifeManager:OverAvailableSpace"
	payload = str(levelReached) + "::" + self.args['eMailAdmin']

	logging.info(" Publishing ['"+ mexage +"']")
        logging.info("   payload = " + payload )
        self.ms.publish( mexage, payload )
        self.ms.commit()

    def unpackingTask( self, taskName ):
        """
        _unpackingTask_

        Trasmit the event "TaskLifeManager::TaskToMange" [to itself]
        """
        
        mexage = "TaskLifeManager::TaskToMange"
        payload = str(taskName)

        logging.info(" Publishing ['"+ mexage +"']")
        logging.info("   payload = " + payload )
        self.ms.publish( mexage, payload, "00:00:20" )
        self.ms.commit()

    def notifyCleaning( self, taskName, toLive, owner, mails ):
        """
        __

        Trasmit the event "TaskLifeManager:TaskNotifyLife"
        """
        #taskName lifetime userName email

        mexage = "TaskLifeManager:TaskNotifyLife"
        uuid = ""
        valuess = getStatusUUIDEmail( taskName )
        if len(valuess) > 1:
            uuid = valuess[1]
        obj = UtilSubject( self.args['dropBoxPath'], taskName, uuid )
        origTaskName, userName = obj.getInfos()

        if owner is None or mails is None:
            owner, mails = self.checkInfoUser(taskName)
        
        payload = origTaskName +"::"+ self.calcFromSeconds(toLive) +"::"+ str(owner) +"::"+ str(mails)

        logging.info(" Publishing ['"+ mexage +"']")
        logging.info("   payload = " + payload )
        self.ms.publish( mexage, payload )


    ##########################################################################
    # start component execution
    ##########################################################################

    def pollDropBox( self ):
        """
	_pollDropBox_

	"""
	value, numberUsed, spacetoAvail = self.checkGlobalSpace()
        if value:
            self.spaceOverNotify( numberUsed )

        self.checkDelete()
        self.deleteTasks()
            
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
        self.ms = MessageService()
        self.ms = MessageService()

        # register
        self.ms.registerAs("TaskLifeManager")

        # subscribe to messages
        self.ms.subscribeTo("TaskLifeManager:StartDebug")
        self.ms.subscribeTo("TaskLifeManager:EndDebug")
        self.ms.subscribeTo("TaskLifeManager::poll")
	self.ms.subscribeTo("TaskTracking:TaskEnded")
        self.ms.subscribeTo("CRAB_Cmd_Mgr:NewTask")
        self.ms.subscribeTo("TaskLifeManager::TaskToMange")
        self.ms.subscribeTo("TaskLifeManager::PrintTaskInfo")

        # load dBox tasks
        self.buildDropBox()

        # generate first polling cycle
        self.ms.remove("TaskLifeManager::poll")
        self.ms.publish("TaskLifeManager::poll", "")
        self.ms.commit()
        
        # wait for messages
        while True:
            msType, payload = self.ms.get()
	    if payload != None:
                logging.info( "Got ms: ['"+str(msType)+"', '"+str(payload)+"']")
                self.__call__(msType, payload)
                logging.info( "Ms exe: ['"+str(msType)+"', '"+str(payload)+"']")
	    else:
 	        logging.error(" ")
                logging.error("ERROR: empty payload - " + str(msType) )
                logging.error(" ")
            self.ms.commit()
	
