from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time

import commands
import os, errno, time, sys, re

import select
import fcntl
import zlib
import traceback
import xml.dom.minidom
from ServerCommunicator import ServerCommunicator 
	 
class SubmitterServer(Actor):
    def __init__(self, cfg_params, parsedRange, nominalRange):
	self.cfg_params = cfg_params
        self.submitRange = parsedRange
	self.nominalRange = nominalRange 
		
	try:  
            #print self.cfg_params['CRAB.server_name'].split(':')
	    #self.server_name, self.server_port = self.cfg_params['CRAB.server_name'].split(':')
	    #self.server_port = int(self.server_port)
            ## BRTUAL SERVER-SE HARDCODED - MATTY
            ## the API (wget or siteDB) can be callet here
            self.server_name, self.server_path = self.cfg_params['CRAB.server_name'].split('/',1)
            self.server_path = os.path.join('/',self.server_path)
            self.server_port = int("20081")
	except KeyError:
	    msg = 'No server selected or port specified.' 
	    msg = msg + 'Please specify a server in the crab cfg file' 
	    raise CrabException(msg) 

	return

    def run(self):
	"""
	The main method of the class: submit jobs in range self.nj_list
	"""
	start = time.time()
	isFirstSubmission = False

        # check proxy and send to the crab-server  
        self.proxyPath = self.moveProxy()
 
	# partial submission code # TODO is that file the right way? 
	if os.path.exists(common.work_space.shareDir()+'/first_submission') == False:
	    self.moveISB()
	    isFirstSubmission = True
	    os.system('touch %s'%str(common.work_space.shareDir()+'/first_submission'))

	# standard submission to the server
	common.logger.debug(5, "SubmitterServer::run() called")
	self.performSubmission(isFirstSubmission)
	return
	
    def moveISB(self):

	## get task info from BL ##
	taskuuid = common._db.queryTask('name')
	common.logger.debug(3, "Task name: " + taskuuid)
	isblist = common._db.queryTask('globalSandbox').split(',')
        
	common.logger.debug(3, "List of ISB files: " +str(isblist) )
	scriptexe = common._db.queryTask('scriptName')
	common.logger.debug(3, "Executable: " +str(scriptexe) )
	common.logger.message("Starting sending the project to the server "+str(self.server_name)+"...")

	## create remote dir ##
        self.remotedir = os.path.join(self.server_path, taskuuid)
	try:
	    cmd = "edg-gridftp-mkdir gsiftp://" +str(self.server_name)+ self.remotedir
	    common.logger.debug(3, "Creating project directory on gsiftp://" +str(self.server_name)+self.remotedir)
	    common.logger.debug(5, " with:\n    " + cmd)
	    status, out = commands.getstatusoutput (cmd)
	    if int(status) != 0:
		common.logger.debug(1, str(out))
		msg = "ERROR : Unable to ship the project to the server \n"
		msg +="Project "+str(taskuuid)+" not Submitted \n"
		raise CrabException(msg)
	except Exception, ex:
	    common.logger.debug(1, str(ex))
	    msg = "ERROR : Unable to ship the project to the server \n"
	    msg +="Project "+str(taskuuid)+" not Submitted \n"
	    raise CrabException(msg)

	## copy ISB ##
	for filetocopy in isblist:
	    try:
		cmd = 'lcg-cp -v --vo cms file://' + os.path.abspath(filetocopy) + \
					' gsiftp://' + str(self.server_name) + \
					  os.path.join(self.remotedir, os.path.basename(filetocopy))
		common.logger.debug(1, "Sending "+filetocopy+" to "+str(self.server_name))
		common.logger.debug(5, " with:\n    " + cmd)
		status, out = commands.getstatusoutput(cmd)
		if int(status) != 0:
		    common.logger.debug(1, str(out))
		    msg = "ERROR : Unable to ship the project to the server \n"
		    msg +="Project "+str(taskuuid)+" not Submitted \n"
		    raise CrabException(msg)
	    except Exception, ex:
		common.logger.debug(1, str(ex))
		msg = "ERROR : Unable to ship the project to the server \n"
		msg +="Project "+str(taskuuid)+" not Submitted \n"
		raise CrabException(msg)
	try:
	    cmd = 'lcg-cp -v --vo cms file://' + scriptexe + \
				    ' gsiftp://' + str(self.server_name) + \
				      os.path.join(self.remotedir, os.path.basename(scriptexe))
	    common.logger.debug(3, "Sending "+scriptexe+" to "+str(self.server_name))
	    common.logger.debug(5, " with:\n    " + cmd)
	    status, out = commands.getstatusoutput(cmd)
	    if int(status) != 0:
		common.logger.debug(1, str(out))
		msg = "ERROR : Unable to ship the project to the server \n"
		msg +="Project "+str(taskuuid)+" not Submitted \n"
		raise CrabException(msg)
	except Exception, ex:
	    common.logger.debug(1, str(ex))
	    msg = "ERROR : Unable to ship the project to the server \n"
	    msg +="Project "+str(taskuuid)+" not Submitted \n"
	    raise CrabException(msg)

	## if here then project submitted ##
	msg = 'Project '+str(taskuuid)+' files successfully submitted to the supporting storage element.\n'
	common.logger.message(msg)
	return

    def moveProxy(self):
	WorkDirName = os.path.basename(os.path.split(common.work_space.topDir())[0])
	proxySubject = None

	## get subject ##
	# TODO here we should get the proxy from the task
	x509 = None # common._db.queryTask('proxy')
	#
	if 'X509_USER_PROXY' in os.environ:
	    x509 = os.environ['X509_USER_PROXY']
	else:
	    status, x509 = commands.getstatusoutput('ls /tmp/x509up_u`id -u`')
	    x509 = x509.strip()

	## register proxy ##
	common.scheduler.checkProxy()
	try:
	    flag = " --myproxy"
	    common.logger.message("Registering a valid proxy to the server\n")
	    cmd = 'asap-user-register --server '+str(self.server_name) + flag
	    attempt = 3
	    while attempt:
		common.logger.debug(3, " executing:\n    " + cmd)
		status, outp = commands.getstatusoutput(cmd)
		common.logger.debug(3, outp)
		if status == 0:
		    common.logger.message("Proxy successfully delegated to the server.")
		    break
		else:
		    attempt = attempt - 1
		if (attempt == 0):
		    raise CrabException("ASAP ERROR: Unable to ship a valid proxy to the server "+str(self.server_name)+"\n")
	except:
	    msg = "ASAP ERROR: Unable to ship a valid proxy to the server \n"
	    msg +="Project "+str(WorkDirName)+" not Submitted \n"
	    raise CrabException(msg)
	return x509

    def performSubmission(self, firstSubmission=True):
        # create the communication session with the server frontend
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params, self.proxyPath)
 
        # TODO to be fixed
        # taskname is equal to taskuuid
        taskname = common._db.queryTask('name')
        taskuuid = ''+str(taskname)
        taskXML = ''
        subOutcome = 0

        # transfer remote dir to server
        self.cfg_params['CRAB.se_remote_dir'] = self.remotedir

        if firstSubmission==True:
            # first time submit
            try:
                task = common._db.getTask() 

                # set the paths refered to SE remotedir
                surlpreamble = '' # TODO: parametric 'gsiftp://' + self.server_name
                remoteSBlist = [surlpreamble + os.path.join(self.remotedir, os.path.basename(f)) \
                        for f in common._db.queryTask('globalSandbox').split(',') ]
                task['globalSandbox'] = ','.join(remoteSBlist)
                task['scriptName'] = surlpreamble + os.path.join( self.remotedir, \
                        os.path.basename(common._db.queryTask('scriptName')) )
                task['cfgName'] = surlpreamble + os.path.join( self.remotedir, \
                        os.path.basename(common._db.queryTask('cfgName')) )

                for j in task.jobs:
                    j['executable'] = surlpreamble + os.path.join( self.remotedir, os.path.basename(j['executable']) )
                #

                taskXML += common._db.serializeTask(task)
                common.logger.debug(5, taskXML)
            except Exception, e:
                msg = "BossLite ERROR: Unable to serialize task object\n"
                msg +="Project "+str(taskname)+" not Submitted \n"
                msg += str(e)
                msg += traceback.format_exc()
                raise CrabException(msg)
            subOutcome = csCommunicator.submitNewTask(taskuuid, taskname, taskXML, self.submitRange)
        else:
            # subsequent submissions and resubmit
            subOutcome = csCommunicator.subsequentJobSubmit(taskuuid, taskname, self.submitRange)

        if subOutcome != 0:
            msg = "ClientServer ERROR: %d raised during the communication.\n"%subOutcome
            raise CrabException(msg)

        del csCommunicator  
        return     



