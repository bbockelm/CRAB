#!/usr/bin/env python

## CONFIG PARAMS

#<ConfigBlock Name="Notification">
#<Parameter Name="ComponentDir" Value="/data/dorigoa/PA_workdir/Notification"/>
#<Parameter Name="Author" Value="Alvise"/>
#<Parameter Name="NotificationDelay" Value="10"/>
#<Parameter Name="Notification_per_job" Value="false"/>
#<Parameter Name="Notification_per_task" Value="true"/>
#<Parameter Name="Notification_SenderName" Value="crab@crabas.lnl.infn.it"/>
#<Parameter Name="Notification_SMTPServer" Value="crabas.lnl.infn.it"/>
#<Parameter Name="Notification_SMTPServerDBGLVL" Value="0"/>
#</ConfigBlock>


"""
_NotificationComponent_

"""

__version__ = "$Revision: 1.6 $"
__revision__ = "$Id: NotificationComponent.py,v 1.6 2007/07/02 09:48:50 mcinquil Exp $"

import os
import socket
import pickle
import logging
import time
import JobInfo
import Consumer
import JobInfoList
import TaskInfoList
import Mailer
from CrabServer.CreateXmlJobReport import *
#from CreateXmlJobReport import *
import string
import re
import sys
from ProdAgentCore.Configuration import ProdAgentConfiguration

from logging.handlers import RotatingFileHandler
from threading import Thread
from MessageService.MessageService import MessageService
from FwkJobRep.ReportParser import readJobReport
from xml.dom import minidom

import smtplib

class NotificationComponent:

    #--------------------------------------------------------------------------------------
    def __init__(self, **args):

        self.jobs  = JobInfoList.JobInfoList()
        self.tasks = TaskInfoList.TaskInfoList()
	
        self.args = {}
        
        self.args['Logfile'] = None
    
        self.args.update(args)
           
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

        #  //
        # // Log Handler is a rotating file that rolls over when the
        #//  file hits 1MB size, 3 most recent files are kept
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        
        #  //
        # // Set up formatting for the logger and set the 
        #//  logging level to info level
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

	config = os.environ.get("PRODAGENT_CONFIG", None)
        if config == None:
            msg = "No ProdAgent Config file provided\n"
            msg += "set $PRODAGENT_CONFIG variable\n"
            logging.error(msg)
            sys.exit(1)
            
        okmsg = "NotificationComponent.__init__: Configuration file is ["
        okmsg += config
        okmsg += "]"
        logging.info(okmsg)
                
        cfgObject = ProdAgentConfiguration()
        cfgObject.loadFromFile(config)
        notifCfg = cfgObject.getConfig("Notification")
 
        _perjob_  = notifCfg.get("Notification_per_job")
        _pertask_ = notifCfg.get("Notification_per_task")
	
	if _perjob_ == "true":
		self.PERJOB = True
	else:
		self.PERJOB = False
		
	if _pertask_ == "true":
		self.PERTASK = True
	else:
		self.PERTASK = False
	
        logging.info("NotificationComponent Started...")
	if self.PERJOB:
		logging.info("Notification PER JOB is active")
	
	if self.PERTASK:
		logging.info("Notification PER TASK is active")

##	senderName = notifCfg.get("Notification_SenderName")
##	self.senderName = senderName

##	self.smtpServer = notifCfg.get("Notification_SMTPServer")

        try:
            self.mailer = Mailer.Mailer(config)
        except RuntimeError, mex:
            logging.error( mex )
            sys.exit(1)

    #--------------------------------------------------------------------------------------        
    def startComponent(self):
        #        print "Notification.startComponent CALLED!"
        # create message service
        self.ms = MessageService()
        
        #print "Starting NotificationComponent....."
        
        # register
        self.ms.registerAs("NotificationComponent")
        
        # subscribe to messages
        #self.ms.subscribeTo("JobSuccess")
	self.ms.subscribeTo("TaskSuccess")
        self.ms.subscribeTo("TaskFailed")
        self.ms.subscribeTo("TaskNotSubmitted")
        #self.ms.subscribeTo("NOTIFICATION_SHOWJOBS")
        #self.ms.subscribeTo("NOTIFICATION_RESET")
        #self.ms.subscribeTo("NOTIFICATION_PAUSE")
        #self.ms.subscribeTo("NOTIFICATION_RESUME")
        #self.ms.subscribeTo("NOTIFICATION_EXIT")
	self.ms.subscribeTo("NotificationSetup")

        # Start the thread Consumer
        threadList = []
        consumerThread = Consumer.Consumer()
	consumerThread.setParams(self.jobs, self.tasks, self.PERJOB, self.PERTASK)
        threadList.append(consumerThread)
        consumerThread.start()

        # Main infinite loop retrieving jobs done and ready to be notified to owner
        self.MainLoop()
        
        
    #--------------------------------------------------------------------------------------
    def MessageJobParser(self, message):
    
    	taskname = ""
    	
        message = message.replace('file://','')
	
	#logmsg = "Parsing [" + message + "] file..."
	#logging.info(logmsg)
	
	if not os.path.exists(message):
		logging.error("Cannot process JobSuccess event: " 
	                        + "job report %s does not exist." % message)
	        return
	try:
		reports = readJobReport(message)
	except Exception, msg:
		logging.error("Cannot process JobSuccess event for %s: %s"
				% (message, msg))
		return
		
	try:
		taskname = reports[0].jobSpecId
	except Exception, msg:
		logging.error("Cannot process JobSuccess event for %s: %s"
				% (message, msg)) 
		return
	
	
	
#        doc = minidom.parse( message )
#	array = doc.getElementsByTagName("FrameworkJobReport")
#	jobspecid = array[0].attributes["JobSpecID"].value
	#p = re.compile('.+_(\d+)_(\d+)$')
	#m = p.match(jobspecid)
	
	array = re.compile('(.+)_(\d+)_(\d+)$').match(taskname).groups()
	
#	if len(array) != 2:
#		errmsg = "ERROR Parsing the jobid in the jobspecid [" + jobspecid + "]. Ignoring this message."
#		logging.error(errmsg)
#		return None
		
#	jobid = "_".join( array )
	
	# now looking for user/owner of this job
	path,file = os.path.split( message )
	up1, tmp = os.path.split( path )
	up2, tmp = os.path.split( up1 )
	up3, tmp = os.path.split( up2)
	
	shareDir = up3 + "/share"
	email = shareDir + "/emailAddress"
	
	if not os.path.exists(email):
		errmsg = "Couldn't find file [" + email + "]. Cannot send job notification to the job's owner"
		logging.error(errmsg)
	        return
	
	f=open(email, 'r')
	emailAddress = str(f.read())
	f.close()
	#chomp(emailAddress)
	emailAddress = emailAddress.rstrip()
	
	jobid = array[2]
	task  = array[0]
	
	#subjectProxyLink = up3 + "/share/userSubj"
	#try:
	#	ProxyFile = os.readlink( subjectProxyLink )
	#except OSError, e:
	#	errmsg = "ERROR reading link [" + subjectProxyLink + "]: %s [%d]" % (e.strerror, e.errno)
	#	logging.error( errmsg )
	#	return None
		
	#sslcmd = "openssl x509 -in " + ProxyFile + " -noout -subject"
	emailAddress.rstrip()
	
	return (jobid, task, emailAddress)
	
    #--------------------------------------------------------------------------------------
    def MessageTaskParser(self, pathfile):
    	if not os.path.exists(pathfile):
		logging.error("Cannot process TaskSuccess event: " 
	                        + "xml task report %s does not exist." % pathfile)
	        return
		
	if not os.path.isfile( pathfile ):
		logging.error("Cannot process TaskSuccess event: " 
	                        + "xml task report %s is not a regular file." % pathfile)
	        return
		
	C = CreateXmlJobReport()#.CreateXmlJobReport()
	try:
		C.fromFile( pathfile )
	except RuntimeError, r:
		logging.error("Cannot parse file %s: %s" % (pathfile, r)) 
		return
		
	return C
	
    #--------------------------------------------------------------------------------------	
    def MainLoop(self):
        while True:
            type, payload = self.ms.get(True) # this call is blocking...
            logging.info("  -> " + str(type) + " <-  ")
            if type == "JobSuccess":
	    	if not self.PERJOB:
                        self.ms.commit()
			continue
			
                pieces = []
                pieces = self.MessageJobParser(payload)
		
		if not pieces:
			continue
			
                if len(pieces) >= 3:
                    jobid = pieces[0]
		    task  = pieces[1]
                    #ts = pieces[1]
                    own = pieces[2]
                    newJ = JobInfo.JobInfo(jobid, task, own)
                    self.jobs.lock()
                    msg = "Notification.NotificationComponent.MainLoop: Adding new job ["
                    msg += jobid + "] owned by [" + own + "]"
                    logging.info( msg )
                    self.jobs.pushJob( newJ )
                    self.jobs.unlock()
                    self.ms.commit()
                    continue
            
	    if type == "TaskSuccess" or type == "TaskNotSubmitted":
	    	if not self.PERTASK:
			continue
		C = self.MessageTaskParser( payload )
		
		if not C:
			logging.error("Notification.NotificationComponent.MainLoop: MessageTaskParser returned a null object. Continuing to next iteration...")
			continue
			
		self.tasks.lock()
		msg = "Notification.NotificationComponent.MainLoop: Adding new task ["
                msg += C.getTaskname() + "] owned by [" + ",".join(C.getUserMail()) + "]"
                logging.info( msg )
                #print "Notification.NotificationComponent.MainLoop: [%s]\n" % C.getTaskReport()
		self.tasks.pushTask( C )
		self.tasks.unlock()
		self.ms.commit()
                
            if type == "NotificationSetup":
	        if payload == "RESET":
                	logging.info( "Clearing list of jobs to notify..." )
			self.jobs.lock()
			self.tasks.lock()
                	self.jobs.clearList()
			self.tasks.clearList90
			self.tasks.unlock()
			self.jobs.unlock()
			self.ms.commit()
                continue

##-------------------------------------------------------------------
            
            if type == "TaskFailed":
                pieces = payload.split(":")
                if len(pieces) < 3:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing TaskFailed's payload ["
                    msg += payload + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue

                emaillist = pieces[2].split(",")
                taskname = pieces[0]
                username = pieces[1]

                ##print "emaillist=[%s]\n" % emaillist
                 
                if not emaillist:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing TaskFailed payload's"
                    msg += " email's list [" + pieces[2] + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue

                if len(emaillist) < 1:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing TaskFailed payload's"
                    msg += " email's list [" + pieces[2] + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue

                if len(emaillist) == 1:
                    if emaillist[0] == "":
                        msg = "Notification.NotificationComponent.MainLoop: empty email address ["
                        msg += emaillist[0] + "]"
                        logging.error("%s" % msg)
                        self.ms.commit()
                        continue
 
                infoFile = "/tmp/crabNotifInfoFile." + str(time.time())

                try:
                    os.remove(infoFile)
                except OSError:
                    pass

                mailMess = "The task [" + taskname + "] owned by [" + username + "] is Failed"
                #FILE = open(infoFile,"w")
        	#FILE.write(mailMess)
        	#FILE.close()

                #mainEmail = emaillist.pop(0)
                #CCRecipients = ",".join( emaillist )
		
		#toaddrs  = emaillist
		    
#                if len(pieces[2].split(",")) >=2:
#                    cmd = "mail -s \"CRAB Server Notification: Task Failed! \" "
#                    cmd += mainEmail + " -c " + CCRecipients + " < " + infoFile
#		    
#		    fromaddr = self.senderName
#		    toaddrs  = emaillist
#                else:
#                    cmd = "mail -s \"CRAB Server Notification: Task Failed! \" "
#                    cmd += mainEmail + " < " + infoFile

##		try:
##			#server = smtplib.SMTP('crabas.lnl.infn.it')
##			server = smtplib.SMTP( self.smtpServer )
##			server.set_debuglevel(1)
##			server.sendmail(self.senderName, emaillist, mailMess)
##			server.quit()
			
##		except SMTPException, ex:
##			errmsg = "ERROR! " + str(ex)
##			logging.error(errmsg)

                msg = "Notification.Consumer.Notify: Sending mail to [" + emaillist + "] using SMTPLIB"
                logging.info( msg )

                completeMessage = "Subject:\"CRAB Server Notification: Task Failed! \"\n\n" + mailMess
                
                try:
                    self.mailer.SendMail(emaillist, completeMessage)
                except RuntimeError, mess:
                    logging.error(mess)
                
		#logging.info( cmd )
                
	        #retCode = os.system( cmd )

        	#if(retCode != 0):
                 #   errmsg = "ERROR! Command ["+cmd+"] FAILED!"
                  #  logging.error(errmsg)
                    
                #try:
                #    os.remove(infoFile)
                #except OSError:
                #    pass
                
                self.ms.commit()
