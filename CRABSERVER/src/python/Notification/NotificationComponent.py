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

__version__ = "$Revision: 1.14 $"
__revision__ = "$Id: NotificationComponent.py,v 1.14 2008/05/16 10:09:26 mcinquil Exp $"

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

        self.serverName =  self.args['ProdAgentName']

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
        self.ms.subscribeTo("TaskLifeManager:TaskNotifyLife")
        self.ms.subscribeTo("TaskLifeManager:OverAvailableSpace")
        #self.ms.subscribeTo("NOTIFICATION_SHOWJOBS")
        #self.ms.subscribeTo("NOTIFICATION_RESET")
        #self.ms.subscribeTo("NOTIFICATION_PAUSE")
        #self.ms.subscribeTo("NOTIFICATION_RESUME")
        #self.ms.subscribeTo("NOTIFICATION_EXIT")
	self.ms.subscribeTo("NotificationSetup")
        self.ms.subscribeTo("ProxyExpiring")

        # Start the thread Consumer
        threadList = []
        consumerThread = Consumer.Consumer(self.serverName)
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

            if type == "TaskLifeManager:TaskNotifyLife":
                pieces = payload.split("::")
                if len(pieces) < 3:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload ["
                    msg += payload + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue
                emaillist = pieces[3].split(",")
                taskname = pieces[0]
                lifetime = pieces[1]
                username = pieces[2]
                
                if not emaillist:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload"
                    msg += " email's list [" + pieces[2] + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue

                if len(emaillist) < 1:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload"
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
                #logging.info("lifetime: "+str(lifetime))
                hours = 0
                mins = 0
                secs = 0
                if lifetime.find(":") != -1:
                    hours, mins, secs = lifetime.split(":")
                    hours = int(hours)
                    mins = int(mins)
                    try:
                        secs = int(secs)
                    except Exception:
                        pass
                else:
                    secs = int(lifetime)
                days = 0
                if hours > 24:
                    days = int(hours/24)
                    hours = int(hours - days * 24)

                timeMsg = " "
                if days > 0:
                    timeMsg += str(days) + " days "
                if hours > 0:
                    timeMsg += str(hours) + " hours "
                if mins > 0:
                    timeMsg += str(mins) + " minutes "
                if secs > 0:
                    timeMsg += str(secs) + " seconds"

                mailMess = "The task [" + taskname + "] owned by [" + username + "] is ended and will be deleted within " + timeMsg
                mailMess += ".\nIf you have to retrieve the outputs of those jobs, you should execute the command:\n\n"
                mailMess += "\tcrab -getoutput -c " + taskname
                mailMess += "\n\nfrom your working area."

                msg = "Notification.Consumer.Notify: Sending mail to [" + str(emaillist) + "] using SMTPLIB"
                logging.info( msg )

                completeMessage = 'Subject:"'+str(self.serverName)+' Notification: Task Cleaning"\n\n' + mailMess
                try:
                    self.mailer.SendMail(emaillist, completeMessage)
                except RuntimeError, mess:
                    logging.error(mess)
                except gaierror, mess:
                    logging.error("gaierror: " + mess )
                except timeout, mess:
                    logging.error("timeout error: " + mess )
                except:
                    print "Unexpected error: ", sys.exc_info()[0]

                self.ms.commit()

##-------------------------------------------------------------------
            if type == "TaskLifeManager:OverAvailableSpace":
                pieces = payload.split("::")
                if len(pieces) < 2:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload ["
                    msg += payload + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue
                emaillist = pieces[1].split(",")
                level = pieces[0]

                if not emaillist:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload"
                    msg += " email's list [" + pieces[2] + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue

                if len(emaillist) < 1:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload"
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
                import socket
                mailMess = "The CRABSERVER "+str(self.serverName)+" " + str(socket.gethostbyaddr(socket.gethostname())) + "\n"
                mailMess +="has the drop box partition full at " + str(level) + "% that is over the level you requested.\n"

                msg = "Notification.Consumer.Notify: Sending mail to [" + str(emaillist) + "] using SMTPLIB"
                logging.info( msg )

                completeMessage = 'Subject:"'+str(self.serverName)+' Server Notification: Space Management"\n\n' + mailMess
                try:
                    self.mailer.SendMail(emaillist, completeMessage)
                except RuntimeError, mess:
                    logging.error(mess)
                except gaierror, mess:
                    logging.error("gaierror: " + mess )
                except timeout, mess:
                    logging.error("timeout error: " + mess )
                except:
                    print "Unexpected error: ", sys.exc_info()[0]

                self.ms.commit()


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

                msg = "Notification.Consumer.Notify: Sending mail to [" + str(emaillist) + "] using SMTPLIB"
                logging.info( msg )

                completeMessage = "Subject:\""+str(self.serverName)+" Server Notification: Task Failed! \"\n\n" + mailMess
                
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

##-------------------------------------------------------------------

            if type == "ProxyExpiring":
                pieces = payload.split("::")
                if len(pieces) < 2:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing ProxyExpiring's payload ["
                    msg += payload + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue

                emaillist = pieces[0].split(",")
                #tasknames = eval(pieces[1])
                proxylife = pieces[1]

                ##print "emaillist=[%s]\n" % emaillist

                if not emaillist:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing ProxyExpiring's payload"
                    msg += " email's list [" + pieces[3] + "]"
                    logging.error("%s" % msg)
                    self.ms.commit()
                    continue

                if len(emaillist) < 1:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing ProxyExpiring's payload"
                    msg += " email's list [" + pieces[3] + "]"
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

                hours, mins, secs = self.calcFromSeconds( proxylife )
                timeMsg = ""
                if hours > 0:
                    timeMsg += str(hours) + " hours "
                if mins > 0:
                    timeMsg += str(mins) + " minutes "
                if secs > 0:
                    timeMsg += str(secs) + " seconds"

                mailMess = "Your proxy will expires in " + timeMsg + ". You can renew it doing:\n\t crab -renewProxy "# + task[0]

                msg = "Notification.Consumer.Notify: Sending mail to [" + str(emaillist) + "] using SMTPLIB"
                logging.info( msg )

                import socket
                completeMessage = 'Subject:"'+str(self.serverName)+' Server Notification: Expiring Proxy!"\n\n' + mailMess
                try:
                    self.mailer.SendMail(emaillist, completeMessage)
                except RuntimeError, mess:
                    logging.error(mess)
                except gaierror, mess:
                    logging.error("gaierror: " + mess )
                except timeout, mess:
                    logging.error("timeout error: " + mess )
                except:
                    print "Unexpected error: ", sys.exc_info()[0]

                self.ms.commit()


    def calcFromSeconds(self, totSec):
        """
        _calcFromSeconds_
        """

        secondi = int(totSec) % 60
        temp = int(int(totSec) / 60)
        minuti = temp % 60
        ore = int(temp / 60)

        return ore, minuti, secondi

