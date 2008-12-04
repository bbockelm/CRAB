from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf

import os, errno, time, sys, re 
import commands, traceback
import zlib

from Submitter import Submitter
from ServerCommunicator import ServerCommunicator 

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface
from ProdCommon.Storage.SEAPI.Exceptions import *

 
class SubmitterServer( Submitter ):
    def __init__(self, cfg_params, parsed_range, val):
	self.srvCfg = {}
        self.cfg_params = cfg_params
        self.submitRange = []
        self.credentialType = 'Proxy' 
        if common.scheduler.name().upper() in ['LSF', 'CAF']:
            self.credentialType = 'Token' 

        Submitter.__init__(self, cfg_params, parsed_range, val)      
    
        # init client server params...
        CliServerParams(self)       

        # path fix
        if self.storage_path[0]!='/':
            self.storage_path = '/'+self.storage_path

        self.taskuuid = str(common._db.queryTask('name'))
        
	return

    def run(self):
	"""
	The main method of the class: submit jobs in range self.nj_list
	"""
	common.logger.debug(5, "SubmitterServer::run() called")

        self.submitRange = self.nj_list
     
        check = self.checkIfCreate() 

        if check == 0 :

            self.remotedir = os.path.join(self.storage_path, self.taskuuid)
            self.manageCredential()
            
            # check if it is the first submission  
            isFirstSubmission =  common._db.checkIfNeverSubmittedBefore() 

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
        if self.storage_proto in ['gridftp','rfio']:
            try:
                action = SBinterface( seEl )  
                action.createDir( self.remotedir )
            except AlreadyExistsException, ex:
                msg = "Project %s already exist on the Storage Element \n"%self.taskuuid 
                msg +='\t%s'%str(ex)
                common.logger.debug(1, msg)
            except OperationException, ex:
                common.logger.debug(1, str(ex))
                msg = "ERROR : Unable to create project destination on the Storage Element \n"
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)
            if self.storage_proto == 'rfio':
                opt = '777' # REMOVE me 
                try:
                    action.setGrant( self.remotedir, opt)
                except Exception, ex:
                    common.logger.debug(1, str(ex))
                    msg = "ERROR : Unable to change permission on the Storage Element \n"
                    msg +="Project "+ self.taskuuid +" not Submitted \n"
                    raise CrabException(msg)

        ## copy ISB ##
        sbi = SBinterface( loc, seEl )

        for filetocopy in isblist:
            source = os.path.abspath(filetocopy) 
            dest = os.path.join(self.remotedir, os.path.basename(filetocopy))
            common.logger.debug(1, "Sending "+ os.path.basename(filetocopy) +" to "+ self.storage_name)
            try:
                sbi.copy( source, dest)
            except Exception, ex:
                common.logger.debug(1, str(ex))
                msg = "ERROR : Unable to ship the project to the server \n"
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)

        ## if here then project submitted ##
        msg = 'Project '+ self.taskuuid +' files successfully submitted to the supporting storage element.\n'
        common.logger.debug(3,msg)
        return


    def manageCredential(self): 
        """
        Prepare configuration and Call credential API 
        """
        common.logger.message("Registering credential to the server")
        # only for temporary back-comp. 
        if  self.credentialType == 'Proxy': 
             # for proxy all works as before....
             self.moveProxy()
             # myProxyMoveProxy() # check within the API ( Proxy.py ) 
        else:
             #from ProdCommon.Credential.CredentialAPI import CredentialAPI
             from CredentialAPI import CredentialAPI
             myproxyserver = self.cfg_params.get('EDG.proxy_server', 'myproxy.cern.ch')
             configAPI = {'credential' : self.credentialType, \
                          'myProxySvr' : myproxyserver,\
                          'serverDN'   : self.server_dn,\
                          'shareDir'   : common.work_space.shareDir() ,\
                          'userName'   : UnixUserName(),\
                          'serverName' : self.server_name \
                          }
             try:
                 CredAPI =  CredentialAPI( configAPI )            
             except Exception, err : 
                 common.logger.debug(3, "Configuring Credential API: " +str(traceback.format_exc()))
                 raise CrabException("ERROR: Unable to configure Credential Client API  %s\n"%str(err))
             try:
                 dict = CredAPI.registerCredential('submit') 
             except Exception, err:
                 common.logger.debug(3, "Configuring Credential API: " +str(traceback.format_exc()))
                 raise CrabException("ERROR: Unable to register %s delegating server: %s\n"%(self.credentialType,self.server_name ))
             self.cfg_params['EDG.proxyInfos'] = dict

        common.logger.message("Credential successfully delegated to the server.\n")
	return
    # TO REMOVE
    def moveProxy( self ):
        WorkDirName = os.path.basename(os.path.split(common.work_space.topDir())[0])
        ## Temporary... to remove soon  
        common.scheduler.checkProxy(minTime=100)
        try:
            common.logger.message("Registering a valid proxy to the server:")
            flag = " --myproxy"
            cmd = 'asap-user-register --server '+str(self.server_name) + flag
            attempt = 3
            while attempt:
                common.logger.debug(3, " executing:\n    " + cmd)
                status, outp = commands.getstatusoutput(cmd)
                common.logger.debug(3, outp)
                if status == 0:
                    break
                else:
                    attempt = attempt - 1
                if (attempt == 0):
                    raise CrabException("ASAP ERROR: Unable to ship a valid proxy to the server "+str(self.server_name)+"\n")
        except:
            msg = "ASAP ERROR: Unable to ship a valid proxy to the server \n"
            msg +="Project "+str(self.taskuuid)+" not Submitted \n"
            raise CrabException(msg)
        return

    def performSubmission(self, firstSubmission=True):
        # create the communication session with the server frontend
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
        taskXML = ''
        subOutcome = 0

        # transfer remote dir to server
        self.cfg_params['CRAB.se_remote_dir'] = self.remotedir

        if firstSubmission==True:
 
            TotJob = common._db.nJobs() 
            # move the sandbox
            self.moveISB_SEAPI() 

            # first time submit
            try:
                taskXML += common._db.serializeTask( common._db.getTask() )
                common.logger.debug(5, taskXML)
            except Exception, e:
                msg = "BossLite ERROR: Unable to serialize task object\n"
                msg +="Project "+str(self.taskuuid)+" not Submitted \n"
                msg += str(e)
                raise CrabException(msg)

            # TODO fix not needed first field 
            subOutcome = csCommunicator.submitNewTask(self.taskuuid, taskXML, self.submitRange,TotJob)
        else:
            # subsequent submissions and resubmit
            subOutcome = csCommunicator.subsequentJobSubmit(self.taskuuid, self.submitRange)

        if subOutcome != 0:
            msg = "ClientServer ERROR: %d raised during the communication.\n"%subOutcome
            raise CrabException(msg)
        elif firstSubmission is True:
            self.markSubmitting(firstSubmission)

        del csCommunicator

        return


    def markSubmitting(self, firstSubmission):
        """
        _markSubmitting_
        sign local db for jobs sent -submitted- to the server
        (just for the first submission)
        """
        common.logger.debug(4, "Updating submitting jobs %s"%str(self.submitRange))
        updlist = [{'statusScheduler':'Submitting', 'status':'CS'}] * len(self.submitRange)
        common._db.updateRunJob_(self.submitRange, updlist)

