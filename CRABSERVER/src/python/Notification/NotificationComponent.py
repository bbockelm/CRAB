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

__version__ = "$Revision: 1.25 $"
__revision__ = "$Id: NotificationComponent.py,v 1.25 2009/02/13 13:26:35 mcinquil Exp $"

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
from ProdCommon.FwkJobRep.ReportParser import readJobReport
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
        self.ms.subscribeTo("TaskLifeManager::CleanStorage")

        # Start the thread Consumer
        threadList = []
        consumerThread = Consumer.Consumer(self.serverName)
	consumerThread.setParams(self.jobs, self.tasks, self.PERJOB, self.PERTASK)
        threadList.append(consumerThread)
        consumerThread.start()

        # Main infinite loop retrieving jobs done and ready to be notified to owner
        try:
            self.MainLoop()
        except Exception, ex:
            import traceback
            logging.error(str(ex))
            logging.error(str(traceback.format_exc()))
        
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
        except ExpatError, e:
            logging.error("Cannot parse file %s: %s" % (pathfile, e))
            return
        return C
	
    #--------------------------------------------------------------------------------------	
    def MainLoop(self):
        while True:
            type, payload = self.ms.get(True) # this call is blocking...
            logging.info("  -> " + str(type) + " <-  ")

            ## info to the logger ##
            excep = ""
            trace = ""
            tasks = []
            mails = ""
            txt   = ""

	    if type == "TaskSuccess" or type == "TaskNotSubmitted":
	    	if not self.PERTASK:
			continue
                pathe, taskname = payload.split("::")
		C = self.MessageTaskParser( pathe )
		
		if not C:
			logging.error("Notification.NotificationComponent.MainLoop: MessageTaskParser returned a null object. Continuing to next iteration...")
			continue
			
		self.tasks.lock()
                tasks = [taskname]
                mails = str(C.getUserMail())
		txt = "Notification.NotificationComponent.MainLoop: Adding new task "
                txt += str(tasks) + " owned by [" + str(mails) + "]"
                logging.info( txt )
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
                try: 
                    if len(pieces) < 3:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload ["
                        msg += payload + "]"
                        raise Exception(msg)

                    emaillist = pieces[3].split(",")
                    taskname = pieces[0]
                    lifetime = pieces[1]
                    username = pieces[2]
                    fullname = pieces[4]

                    ## info to log ##
                    tasks = str(fullname)
                    mails = str(emaillist)

                    if not emaillist:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload"
                        msg += " email's list [" + pieces[2] + "]"
                        raise Exception(msg)

                    if len(emaillist) < 1:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing "+type+"'s payload"
                        msg += " email's list [" + pieces[2] + "]"
                        raise Exception(msg)
 
                    if len(emaillist) == 1:
                        if emaillist[0] == "":
                            msg = "Notification.NotificationComponent.MainLoop: empty email address ["
                            msg += emaillist[0] + "]"
                            raise Exception(msg)
 
                    infoFile = "/tmp/crabNotifInfoFile." + str(time.time())

                    try:
                        os.remove(infoFile)
                    except OSError:
                        pass

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

                    txt = "Notification.Consumer.Notify: Sending mail to [" + str(emaillist) + "] using SMTPLIB"
                    logging.info( txt )

                    subj = str(self.serverName)+' Notification: Task Cleaning"' ## mailMess
                    try:
                        self.mailer.SendMail(emaillist, subj, mailMess)
                    except RuntimeError, mess:
                        ## info to log ##
                        import traceback 
                        excep = str(mess)
                        trace = str(traceback.format_exc())

                        logging.error(mess)
                        logging.error(trace)
                    except Exception, exc:
                        ## info to log ##
                        import traceback
                        excep = str(exc)
                        trace = str(traceback.format_exc())

                        logging.error(exc)
                        logging.error(trace)

                except Exception, exc:
                        ## info to log ##
                        import traceback
                        excep = str(exc)
                        trace = str(traceback.format_exc())

                        logging.error(exc)
                        logging.error(trace)

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

                subj = str(self.serverName)+' Server Notification: Space Management"' ## mailMess
                try:
                    self.mailer.SendMail(emaillist, subj, mailMess)
                except RuntimeError, mess:
                        ## info to log ##
                        import traceback
                        excep = str(mess)
                        trace = str(traceback.format_exc())

                        logging.error(mess)
                        logging.error(trace)
                except Exception, exc:
                        ## info to log ##
                        import traceback
                        excep = str(exc)
                        trace = str(traceback.format_exc())

                        logging.error(exc)
                        logging.error(trace)

                self.ms.commit()

##-------------------------------------------------------------------
            
            if type == "TaskFailed":
                pieces = payload.split(":")
                try:
                    if len(pieces) < 3:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing TaskFailed's payload ["
                        msg += payload + "]"

                    emaillist = pieces[2].split(",")
                    taskname = pieces[0]
                    username = pieces[1]

                    ## info to log ##
                    tasks = [str(pieces[3])]
                    mails = str(emaillist)
 
                    if not emaillist:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing TaskFailed payload's"
                        msg += " email's list [" + pieces[2] + "]"
                        raise Exception(msg)

                    if len(emaillist) < 1:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing TaskFailed payload's"
                        msg += " email's list [" + pieces[2] + "]"
                        raise Exception(msg)

                    if len(emaillist) == 1:
                        if emaillist[0] == "":
                            msg = "Notification.NotificationComponent.MainLoop: empty email address ["
                            msg += emaillist[0] + "]"
                            raise Exception(msg)
 
                    infoFile = "/tmp/crabNotifInfoFile." + str(time.time())
 
                    try:
                        os.remove(infoFile)
                    except OSError:
                        pass
 
                    mailMess = "The task [" + taskname + "] owned by [" + username + "] is Failed"
  
                    txt = "Notification.Consumer.Notify: Sending mail to [" + str(emaillist) + "] using SMTPLIB"
                    logging.info( txt )
   
                    subj = str(self.serverName)+" Server Notification: Task Failed! " ## mailMess
                  
                    try:
                        self.mailer.SendMail(emaillist, subj, mailMess)
                    except RuntimeError, mess:
                        ## info to log ##
                        import traceback
                        excep = str(mess)
                        trace = str(traceback.format_exc())

                        logging.error(mess)
                        logging.error(trace)
                    except Exception, exc:
                        ## info to log ##
                        import traceback
                        excep = str(exc)
                        trace = str(traceback.format_exc())

                        logging.error(exc)
                        logging.error(trace)

                except Exception, exc:
                    ## info to log ##
                    import traceback
                    excep = str(exc)
                    trace = str(traceback.format_exc())

                    logging.error(exc)
                    logging.error(trace)

                self.ms.commit()

##-------------------------------------------------------------------

            if type == "ProxyExpiring":
                pieces = payload.split("::")
                try:
                    if len(pieces) < 3:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing ProxyExpiring's payload ["
                        msg += payload + "]"

                    emaillist = pieces[0].split(",")
                    proxylife = pieces[1]
                    taskslist = eval(file(str(pieces[2]), 'r').read())
                    os.remove(str(pieces[2]))
 
                    ## info to log ##
                    tasks = taskslist
                    mails = str(emaillist)
                    
                    if not emaillist:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing ProxyExpiring's payload"
                        msg += " email's list [" + pieces[3] + "]"
                        raise Exception(msg)
   
                    if len(emaillist) < 1:
                        msg = "Notification.NotificationComponent.MainLoop: error parsing ProxyExpiring's payload"
                        msg += " email's list [" + pieces[3] + "]"
                        raise Exception(msg)

                    if len(emaillist) == 1:
                        if emaillist[0] == "":
                            msg = "Notification.NotificationComponent.MainLoop: empty email address ["
                            msg += emaillist[0] + "]"
                            raise Exception(msg)

                    hours, mins, secs = self.calcFromSeconds( proxylife )
                    timeMsg = ""
                    if hours > 0:
                        timeMsg += str(hours) + " hours "
                    if mins > 0:
                        timeMsg += str(mins) + " minutes "
                    if secs > 0:
                        timeMsg += str(secs) + " seconds"
   
                    stringtask = "\n\t"
                    for taskname in taskslist:
                        stringtask += str(taskname) + "\n\t"
  
                    mailMess = "Your credential will expires in " + timeMsg + ". You can renew it doing:\n"
                    mailMess += "\t crab -renewCredential\n\n"
                    mailMess += "Your active tasks:\n" + stringtask
                    mailMess += "\non the server:\n\t" + self.serverName
                
                    txt = "Notification.Consumer.Notify: Sending mail to [" + str(emaillist) + "] using SMTPLIB"
                    logging.info( txt )

                    import socket
                    subj = str(self.serverName)+' Server Notification: Expiring Credential!' ## mailMess
                    try:
                        self.mailer.SendMail(emaillist, subj, mailMess)
                    except RuntimeError, mess:
                        ## info to log ##
                        import traceback
                        excep = str(mess)
                        trace = str(traceback.format_exc())

                        logging.error(mess)
                        logging.error(trace)
                    except Exception, exc:
                        ## info to log ##
                        import traceback
                        excep = str(exc)
                        trace = str(traceback.format_exc())

                        logging.error(exc)
                        logging.error(trace)

                except Exception, exc:
                    ## info to log ##
                    import traceback
                    excep = str(exc)
                    trace = str(traceback.format_exc())

                    logging.error(exc)
                    logging.error(trace)

                self.ms.commit()

##-------------------------------------------------------------------

            if type == "TaskLifeManager::CleanStorage":
                pieces = payload.split("::")
                if len(pieces) < 2:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing TaskLifeManager::CleanStorage's payload ["
                    msg += payload + "]"

                emaillist = pieces[0].split(",")
                cmdpath = str(pieces[1])

                if not emaillist:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing TaskLifeManager::CleanStorage's payload"
                    msg += " email's list [" + pieces[3] + "]"
                    raise Exception(msg)

                if len(emaillist) < 1:
                    msg = "Notification.NotificationComponent.MainLoop: error parsing TaskLifeManager::CleanStorage's payload"
                    msg += " email's list [" + pieces[3] + "]"
                    raise Exception(msg)

                if len(emaillist) == 1:
                    if emaillist[0] == "":
                        msg = "Notification.NotificationComponent.MainLoop: empty email address ["
                        msg += emaillist[0] + "]"
                        raise Exception(msg)

                mailMess = "Dear Admin, there are sandboxes not anymore needed on the storage area used by "+str(self.serverName)+".\n" + \
                           "Execute this script to clean them:\n\t python "+cmdpath+" your_proxy_full_path"

                msg = "Notification.Consumer.Notify: Sending mail to [" + str(emaillist) + "] using SMTPLIB"
                logging.info( msg )

               

                import socket
                subj = str(self.serverName)+' Server Notification: Clean Storage Area!"'
                try:
                    self.mailer.SendMail(emaillist, subj, mailMess)
                except RuntimeError, mess:
                        ## info to log ##
                        import traceback
                        excep = str(mess)
                        trace = str(traceback.format_exc())

                        logging.error(mess)
                        logging.error(trace)
                except Exception, exc:
                        ## info to log ##
                        import traceback
                        excep = str(exc)
                        trace = str(traceback.format_exc())

                        logging.error(exc)
                        logging.error(trace)

                self.ms.commit()

            if type in ["TaskSuccess", "TaskFailed", "TaskNotSubmitted", "TaskLifeManager:TaskNotifyLife", "ProxyExpiring"]:
                logging.info("Building info")
                for task in tasks:
                    logging.info("Building info for task [%s]"%task)
                    buildinfo = {\
                                 "ev":     type,       \
                                 "reason": str(excep), \
                                 "exc":    str(trace), \
                                 "mails":  str(mails), \
                                 "txt":    txt         \
                                }
                    self.infoLogger(task, buildinfo)


    def calcFromSeconds(self, totSec):
        """
        _calcFromSeconds_
        """

        secondi = int(totSec) % 60
        temp = int(int(totSec) / 60)
        minuti = temp % 60
        ore = int(temp / 60)

        return ore, minuti, secondi

    def infoLogger(self, taskname, diction, jobid = -1):
        """             
        _infoLogger_
        Send the default message to log the information in the task/job log info
        """ 
        from IMProv.IMProvAddEntry import Event
        eve = Event( )
        eve.initialize( diction )
        import time
        unifilename = os.path.join(self.args["ComponentDir"], taskname+str(time.time())+".pkl")
        eve.dump( unifilename )
        message = "TTXmlLogging"
        payload  = taskname + "::" +str(jobid) + "::" + unifilename
        self.ms.publish(message, payload)
        logging.info("DONE")
