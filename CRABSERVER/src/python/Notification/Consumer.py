import time
from threading import Thread
import os
from ProdAgentCore.Configuration import ProdAgentConfiguration
import logging
import Mailer
import sys

#
################################################################################
#

class Consumer(Thread):

    #
    # ***************************************
    #
    def __init__(self):
        Thread.__init__(self)
	
        config = os.environ.get("PRODAGENT_CONFIG", None)
	
        if config == None:
            msg = "No ProdAgent Config file provided\n"
            msg += "set $PRODAGENT_CONFIG variable\n"
            logging.error(msg)
            sys.exit(1)
            
        okmsg = "Notification.Consumer.__init__: Configuration file is ["
        okmsg += config
        okmsg += "]"
        logging.info(okmsg)
                
        cfgObject = ProdAgentConfiguration()
        cfgObject.loadFromFile(config)
        notifCfg = cfgObject.getConfig("Notification")

        self.notif_delay = notifCfg.get("NotificationDelay")

        if self.notif_delay == None:
            self.notif_delay = 1800
            
        try:
            self.mailer = Mailer.Mailer(config)
        except RuntimeError, mex:
            logging.error( mex )
            sys.exit(1)

        okmsg = "Notification.Consumer.__init__: Notifying every " + str(self.notif_delay) + " seconds"
        logging.info(okmsg)

    #
    # ***************************************
    #
    
    def setParams(self, _jobList, _tasklist, per_job, per_task):
    	self.jobList    =  _jobList
	self.taskList   =  _tasklist
	self.per_job    =  per_job
	self.per_task   =  per_task
    
    def run(self):
        while(True):
		time.sleep( float(self.notif_delay) )
		
		if self.per_job:
			self.jobList.lock()
			self.processJobList()
			self.jobList.unlock()
			
		if self.per_task:
			self.taskList.lock()
			self.processTaskList()
			self.taskList.unlock()
		
            


    #
    # ***************************************
    #
    def processJobList(self):
    	localJobList = []
        
        if len( self.jobList.getJobList() ) == 0:
        	msg = "Notification.Consumer.processJobList: No job to notify"
                logging.info(msg)
                return
        for job in self.jobList.getJobList():
                msg = "Notification.Consumer.processJobList: Must notify job ["
                msg += str( job.getJobID() )
                msg += "] of the task [" + str( job.getTaskName() )+ "]"
		msg += " to the user ["
                msg += str( job.getOwner() )
                msg += "]"
                logging.info(msg)
                # HERE must send an email to job's owner
                # if mail sending is succesful remove job from self.jobList
                    
        self.NotifyJobs( self.jobList.getJobList() ) # Keep locked because Notify will remove the notified jobs

	
    #
    # ***************************************
    #
    def processTaskList(self):
    	localTaskList = []
        
        if len( self.taskList.getTaskList() ) == 0:
        	msg = "Notification.Consumer.processTaskList: No task to notify"
                logging.info(msg)
                return
        for task in self.taskList.getTaskList():
                msg = "Notification.Consumer.processTaskList: Must notify task ["
                msg += str( task.getTaskname() )
                msg += "] to the users ["
                msg += ",".join( task.getUserMail() )
                msg += "]"
                logging.info(msg)
                # HERE must send an email to job's owner
                # if mail sending is succesful remove job from self.jobList
                    
        self.NotifyTasks( self.taskList.getTaskList() ) # Keep locked because Notify will remove the notified jobs
        
    #
    # ***************************************
    #
    def NotifyJobs(self, joblist):

        ownerListMapInfo = {}
        
        for job in joblist:
            ownerListMapInfo[job.getOwner()] = []
            
        for job in joblist:
            ownerListMapInfo[job.getOwner()].append( job )

        for user in ownerListMapInfo.keys():

            message = "Dear user [" + user + "], the following job(s) are available for output sandbox retrieve:\n\n"
            listToRemove = []
            for aJob in ownerListMapInfo[user]:
                message += "JobID: [" +aJob.getJobID() + "] - TaskName: [" + aJob.getTaskName() + "]\n"
                listToRemove.append( aJob )
            
            toList = []
            toList.appen( user )

            completeMessage = "Subject:\"CRAB Server Notification: Job output available+\n\n" + message

            try:
                self.mailer.SendMail( toList, completeMessage )
                self.jobList.removeJobs(listToRemove)
            except RuntimeError, mess:
                logging.error(mess)

        
    #
    # ***************************************
    #
    def NotifyTasks(self, tasklist):
        
        listToRemove = [];
            
	for task in tasklist:
		message = task.getTaskReport() 

                emails = task.getUserMail()

		emailAddr = ",".join( emails )

                
                msg = "Notification.Consumer.Notify: Sending mail to [" + emailAddr + "]"
        	logging.info( msg )

                completeMessage = "Subject:\"CRAB Server Notification: The task ["+ task.getTaskname() + "] " + task.getTaskOutcome() + "\n\n" + message

                try:
                    self.mailer.SendMail( emails, completeMessage )
                    listToRemove.append( task )
                    self.taskList.removeTasks(listToRemove)
                except RuntimeError, mex:
                    logging.error(mex)
