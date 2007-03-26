import time
from threading import Thread
import os
from ProdAgentCore.Configuration import ProdAgentConfiguration
import logging

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

        #print "list config=[%s]" % cfgObject.listComponents()
        
        self.notif_delay = notifCfg.get("NotificationDelay")

        #print "Notifying every %s seconds" % self.notif_delay
        
        if self.notif_delay == None:
            self.notif_delay = 1800

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
        	msg = "Notification.Consumer.run: No job to notify"
                logging.info(msg)
                return
        for job in self.jobList.getJobList():
                msg = "Notification.Consumer.run: Must notify job ["
                msg += str( job.getJobID() )
                msg += "] of the task [" + str( job.getTaskName() )+ "]"
		msg += " to the user ["
                msg += str( job.getOwner() )
                msg += "]"
                logging.info(msg)
                # HERE must send an email to job's owner
                # if mail sending is succesful remove job from self.jobList
                #localJobList.append( self.jobList.getJobInfo(job) )
                    
        self.NotifyJobs( self.jobList.getJobList() ) # Keep locked because Notify will remove the notified jobs

	
    #
    # ***************************************
    #
    def processTaskList(self):
    	localTaskList = []
        
        if len( self.taskList.getTaskList() ) == 0:
        	msg = "Notification.Consumer.run: No task to notify"
                logging.info(msg)
                return
        for task in self.taskList.getTaskList():
                msg = "Notification.Consumer.run: Must notify task ["
                msg += str( task.getTaskname() )
                msg += "] to the users ["
                msg += str( task.getUserMail() )
                msg += "]"
                logging.info(msg)
                # HERE must send an email to job's owner
                # if mail sending is succesful remove job from self.jobList
                #localJobList.append( self.jobList.getJobInfo(job) )
                    
        self.NotifyTasks( self.taskList.getTaskList() ) # Keep locked because Notify will remove the notified jobs
        
    #
    # ***************************************
    #
    def NotifyJobs(self, joblist):
        #print "Notify: joblist=" + str( joblist )
        listFile = "/tmp/jobList." + str(time.time())
        ownerListMapInfo = {}
        
        for job in joblist:
            ownerListMapInfo[job.getOwner()] = []
            
        for job in joblist:
            ownerListMapInfo[job.getOwner()].append( job )

        for user in ownerListMapInfo.keys():
            #msg = "Switching to user [" + user + "]"
            #logging.info( msg )
            message = "Dear user [" + user + "], the following job(s) are available for output sandbox retrieve:\n\n"
            listToRemove = []
            for aJob in ownerListMapInfo[user]:
                message += "JobID: [" +aJob.getJobID() + "] - TaskName: [" + aJob.getTaskName() + "]\n"
                listToRemove.append( aJob )
            
            try:
                os.remove(listFile)
            except OSError:
                pass

            # before writing the file in /tmp, check for partition's free space
            
            FILE = open(listFile,"w")
            FILE.write(message)
            FILE.close()

            cmd = "mail -s \"CRAB Server Notification: Job Output available\" " +str(user) + "< " + listFile 
            msg = "Notification.Consumer.Notify: Sending mail to [" + str(user) + "]"
            logging.info( msg )
	    logging.info( cmd )
            retCode = os.system( cmd )

            if(retCode != 0):
                errmsg = "ERROR! Command ["+cmd+"] FAILED! Won't remove these jobs from queue, will retry later"
                logging.error(errmsg)
            else:
                self.jobList.removeJobs(listToRemove)
                
            try:
                os.remove(listFile)
            except OSError:
                pass

        
    #
    # ***************************************
    #
    def NotifyTasks(self, tasklist):
        #print "Notify: joblist=" + str( tasklist )
        infoFile = "/tmp/taskInfo." + str(time.time())
        
        listToRemove = [];
            
	for task in tasklist:
		try:
        		os.remove(infoFile)
	        except OSError:
        	        pass
 		
		message = task.getTaskReport() 
	        FILE = open(infoFile,"w")
        	FILE.write(message)
        	FILE.close()

                emails = task.getUserMail()
                mainAddr = emails.pop(0)
                
		emailAddr = ",".join( emails )
		#mainAddr = task.getUserMail()[0]
		if(len(task.getUserMail())>1):
        		cmd = "mail -s \"CRAB Server Notification: Task Report available\" " +mainAddr + " -c " + emailAddr +" < " + infoFile 
		else:
			cmd = "mail -s \"CRAB Server Notification: Task Report available\" " +mainAddr + " <"+ infoFile 
			
	        msg = "Notification.Consumer.Notify: Sending mail to [" + emailAddr + "]"
        	logging.info( msg )
		logging.info( cmd )
	        retCode = os.system( cmd )

        	if(retCode != 0):
			errmsg = "ERROR! Command ["+cmd+"] FAILED! Won't remove these jobs from queue, will retry later"
	                logging.error(errmsg)
        	else:
			listToRemove.append( task )
	                self.taskList.removeTasks(listToRemove)
                
        	try:
                	os.remove(infoFile)
	        except OSError:
        	        pass
