from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf

import os, errno, time, sys, re 
import commands
import zlib

from Submitter import Submitter
from ServerCommunicator import ServerCommunicator 
from ServerConfig import *

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface
 
class SubmitterServer( Submitter ):
    def __init__(self, cfg_params, parsed_range, val):
	self.srvCfg = {}
        self.cfg_params = cfg_params
        self.submitRange = []
       
        Submitter.__init__(self, cfg_params, parsed_range, val)      

        # init client-server interactions 
	try:
            self.srvCfg = ServerConfig(self.cfg_params['CRAB.server_name']).config()

            self.server_name = str(self.srvCfg['serverName']) 
            self.server_port = int(self.srvCfg['serverPort'])

            self.storage_name = str(self.srvCfg['storageName'])
            self.storage_path = str(self.srvCfg['storagePath'])
            self.storage_proto = str(self.srvCfg['storageProtocol'])
            self.storage_port = str(self.srvCfg['storagePort'])
	except KeyError:
	    msg = 'No server selected or port specified.' 
	    msg = msg + 'Please specify a server in the crab cfg file' 
	    raise CrabException(msg)

        # path fix
        if self.storage_path[0]!='/':
            self.storage_path = '/'+self.storage_path
	return

    def run(self):
	"""
	The main method of the class: submit jobs in range self.nj_list
	"""
	common.logger.debug(5, "SubmitterServer::run() called")

        self.submitRange = self.nj_list

        check = self.checkIfCreate() 

        if check == 0 :
	    isFirstSubmission = False

            self.taskuuid = str(common._db.queryTask('name'))
            self.remotedir = os.path.join(self.storage_path, self.taskuuid)
            self.proxyPath = self.moveProxy()
            
            # check if it is the first submission  
            n_createdJob = len(common._db.queryAttrRunJob({'status':'C'},'status'))
            if n_createdJob == len(self.complete_List): isFirstSubmission = True

	    # standard submission to the server
	    self.performSubmission(isFirstSubmission)
        
            msg = '\nTotal of %d jobs submitted'%len(self.submitRange) 
            common.logger.message(msg)
 
	return

    def moveISB_SEAPI(self):
        ## get task info from BL ##
        common.logger.debug(3, "Task name: " + self.taskuuid)
        isblist = common._db.queryTask('globalSandbox').split(',')
        common.logger.debug(3, "List of ISB files: " +str(isblist) )
        
        # init SE interface
        common.logger.message("Starting sending the project to the storage "+str(self.storage_name)+"...")
        try:  
            seEl = SElement(self.storage_name, self.storage_proto, self.storage_port)
        except Exception, ex:
            common.logger.debug(1, str(ex))
            msg = "ERROR : Unable to create SE destination interface \n"
            msg +="Project "+ self.taskuuid +" not Submitted \n"
            raise CrabException(msg)
          
        try:  
            loc = SElement("localhost", "local")
        except Exception, ex:
            common.logger.debug(1, str(ex))
            msg = "ERROR : Unable to create SE source interface \n"
            msg +="Project "+ self.taskuuid +" not Submitted \n"
            raise CrabException(msg)


        ### it should not be there... To move into SE API. DS

        # create remote dir for gsiftp 
        if self.storage_proto == 'gridftp':
            try:
                action = SBinterface( seEl )  
                action.createDir( self.remotedir, self.proxyPath)
            except Exception, ex:
                common.logger.debug(1, str(ex))
                msg = "ERROR : Unable to create project destination on the Storage Element \n"
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)

        ## copy ISB ##
        sbi = SBinterface( loc, seEl )

        for filetocopy in isblist:
            source = os.path.abspath(filetocopy) 
            dest = os.path.join(self.remotedir, os.path.basename(filetocopy))
            common.logger.debug(1, "Sending "+ os.path.basename(filetocopy) +" to "+ self.storage_name)

            try:
                sbi.copy( source, dest, self.proxyPath)
            except Exception, ex:
                common.logger.debug(1, str(ex))
                msg = "ERROR : Unable to ship the project to the server \n"
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)

        ## if here then project submitted ##
        msg = 'Project '+ self.taskuuid +' files successfully submitted to the supporting storage element.\n'
        common.logger.debug(3,msg)
        return

    def moveProxy(self):
	WorkDirName = os.path.basename(os.path.split(common.work_space.topDir())[0])

        x509 = getSubject(self)
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
	    msg +="Project "+str(self.taskuuid)+" not Submitted \n"
	    raise CrabException(msg)
            return None
	return x509

    def performSubmission(self, firstSubmission=True):
        # create the communication session with the server frontend
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params, self.proxyPath)
        taskXML = ''
        subOutcome = 0

        # transfer remote dir to server
        self.cfg_params['CRAB.se_remote_dir'] = self.remotedir

        if firstSubmission==True:
            # move the sandbox
            self.moveISB_SEAPI() 

            # first time submit
            try:
                task = common._db.getTask() 

                # set the paths refered to SE remotedir
                # NOTE WMS/JDL supports only gsiftp protocol for base ISB/OSB 
                surlpreamble = '' #'gsiftp://%s:%s'%(self.storage_name, str(self.storage_port) )
                remoteSBlist = [surlpreamble + os.path.join(self.remotedir, os.path.basename(f)) \
                        for f in common._db.queryTask('globalSandbox').split(',') ]
                task['globalSandbox'] = ','.join(remoteSBlist)
                task['outputDirectory'] = self.remotedir
                task['scriptName'] = surlpreamble + os.path.join( self.remotedir, \
                        os.path.basename(common._db.queryTask('scriptName')) )
                task['cfgName'] = surlpreamble + os.path.join( self.remotedir, \
                        os.path.basename(common._db.queryTask('cfgName')) )

                for j in task.jobs:
                    j['executable'] = os.path.basename(j['executable'])
                    # buggy, only the local file needed #surlpreamble + os.path.join( self.remotedir, os.path.basename(j['executable']) )
                #

                taskXML += common._db.serializeTask(task)
                common.logger.debug(5, taskXML)
            except Exception, e:
                msg = "BossLite ERROR: Unable to serialize task object\n"
                msg +="Project "+str(self.taskuuid)+" not Submitted \n"
                msg += str(e)
                raise CrabException(msg)

            # TODO fix not needed first field 
            subOutcome = csCommunicator.submitNewTask(self.taskuuid, taskXML, self.submitRange)
        else:
            # subsequent submissions and resubmit
            subOutcome = csCommunicator.subsequentJobSubmit(self.taskuuid, self.submitRange)

        if subOutcome != 0:
            msg = "ClientServer ERROR: %d raised during the communication.\n"%subOutcome
            raise CrabException(msg)

        del csCommunicator

        # update runningjobs status
        updList = [{'statusScheduler':'Submitted', 'status':'S'}] * len(self.submitRange) 
        common._db.updateRunJob_(self.submitRange, updList)
        return


