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
from ProdCommon.Credential.CredentialAPI import CredentialAPI

class SubmitterServer( Submitter ):
    def __init__(self, cfg_params, parsed_range, val):
        self.srvCfg = {}
        self.cfg_params = cfg_params
        self.submitRange = []
        self.credentialType = 'Proxy'
        self.copyTout= setLcgTimeout()
        if common.scheduler.name().upper() in ['LSF', 'CAF']:
            self.credentialType = 'Token'
            self.copyTout= ' '

        Submitter.__init__(self, cfg_params, parsed_range, val)

        # init client server params...
        CliServerParams(self)

        # path fix
        if self.storage_path[0]!='/':
            self.storage_path = '/'+self.storage_path

        self.taskuuid = str(common._db.queryTask('name'))
        self.limitJobs = False

	return

    def run(self):
	"""
	The main method of the class: submit jobs in range self.nj_list
	"""
        common.logger.debug("SubmitterServer::run() called")

        start = time.time()

        self.BuildJobList()

        self.submitRange = self.nj_list

        check = self.checkIfCreate()

        if check == 0 :

            self.remotedir = os.path.join(self.storage_path, self.taskuuid)
            self.manageCredential()

            # check if it is the first submission
            isFirstSubmission =  common._db.checkIfNeverSubmittedBefore()

	    # standard submission to the server
	    self.performSubmission(isFirstSubmission)

            stop = time.time()
            common.logger.debug("Submission Time: "+str(stop - start))

            msg = 'Total of %d jobs submitted'%len(self.submitRange)
            common.logger.info(msg)

	return

    def moveISB_SEAPI(self):
        ## get task info from BL ##
        common.logger.debug("Task name: " + self.taskuuid)
        isblist = common._db.queryTask('globalSandbox').split(',')
        common.logger.debug("List of ISB files: " +str(isblist) )

        # init SE interface
        common.logger.info("Starting sending the project to the storage "+str(self.storage_name)+"...")
        try:
            seEl = SElement(self.storage_name, self.storage_proto, self.storage_port)
        except Exception, ex:
            common.logger.debug(str(ex))
            msg = "ERROR : Unable to create SE destination interface \n"
            msg +="Project "+ self.taskuuid +" not Submitted \n"
            raise CrabException(msg)

        try:
            loc = SElement("localhost", "local")
        except Exception, ex:
            common.logger.debug(str(ex))
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
                common.logger.debug(msg)
            except OperationException, ex:
                common.logger.debug(str(ex.detail))
                msg = "ERROR: Unable to create project destination on the Storage Element %s\n"%str(ex)
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)
            except AuthorizationException, ex:
                common.logger.debug(str(ex.detail))
                msg = "ERROR: Unable to create project destination on the Storage Element: %s\n"%str(ex)
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)
            except TransferException, ex:
                common.logger.debug(str(ex.detail))
                msg = "ERROR: Unable to create project destination on the Storage Element: %s\n"%str(ex)
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)

        ## copy ISB ##
        sbi = SBinterface( loc, seEl )

        for filetocopy in isblist:
            source = os.path.abspath(filetocopy)
            dest = os.path.join(self.remotedir, os.path.basename(filetocopy))
            common.logger.debug("Sending "+ os.path.basename(filetocopy) +" to "+ self.storage_name)
            try:
                sbi.copy( source, dest, opt=self.copyTout)
            except AuthorizationException, ex:
                common.logger.debug(str(ex.detail))
                msg = "ERROR: Unable to create project destination on the Storage Element: %s\n"%str(ex)
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)
            except Exception, ex:
                common.logger.debug(str(ex))
                msg = "ERROR : Unable to ship the project to the server %s\n"%str(ex)
                msg +="Project "+ self.taskuuid +" not Submitted \n"
                raise CrabException(msg)

        ## if here then project submitted ##
        msg = 'Project '+ self.taskuuid +' files successfully submitted to the supporting storage element.\n'
        common.logger.debug(msg)
        return


    def manageCredential(self):
        """
        Prepare configuration and Call credential API
        """
        common.logger.info("Registering credential to the server : %s"%self.server_name)

        myproxyserver = self.cfg_params.get('GRID.proxy_server', 'myproxy.cern.ch')
        configAPI = {'credential' : self.credentialType, \
                     'myProxySvr' : myproxyserver,\
                     'serverDN'   : self.server_dn,\
                     'shareDir'   : common.work_space.shareDir() ,\
                     'userName'   : getUserName(),\
                     'serverName' : self.server_name, \
                     'logger'     : common.logger() \
                     }

        try:
             CredAPI =  CredentialAPI( configAPI )
        except Exception, err :
             common.logger.debug("Configuring Credential API: " +str(traceback.format_exc()))
             raise CrabException("ERROR: Unable to configure Credential Client API  %s\n"%str(err))


        if  self.credentialType == 'Proxy':
             # Proxy delegation through MyProxy, 4 days lifetime minimum 
             if not CredAPI.checkMyProxy(Time=4, checkRetrieverRenewer=True) :
                common.logger.info("Please renew MyProxy delegated proxy:\n")
                try:
                    CredAPI.credObj.serverDN = self.server_dn
                    CredAPI.ManualRenewMyProxy()
                except Exception, ex:
                    common.logger.debug("Delegating Credentials to MyProxy : " +str(traceback.format_exc()))
                    raise CrabException(str(ex))
        else:
             # Kerberos token movement
             if not CredAPI.checkCredential(Time=12) :
                common.logger.info("Please renew the token:\n")
                try:
                    CredAPI.ManualRenewCredential()
                except Exception, ex:
                    raise CrabException(str(ex))

             try:
                 dict = CredAPI.registerCredential()
             except Exception, err:
                 common.logger.debug("Registering Credentials : " +str(traceback.format_exc()))
                 raise CrabException("ERROR: Unable to register %s delegating server: %s\n"%(self.credentialType,self.server_name ))

        common.logger.info("Credential successfully delegated to the server.\n")
	return

    def performSubmission(self, firstSubmission=True):
        # create the communication session with the server frontend
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
        taskXML = ''
        subOutcome = 0

        # transfer remote dir to server
        self.cfg_params['CRAB.se_remote_dir'] = self.remotedir

        if firstSubmission==True:

            totJob = common._db.nJobs()
            # move the sandbox
            self.moveISB_SEAPI()

            # first time submit
            try:
                self.stateChange( self.submitRange, "SubRequested" )
                taskXML += common._db.serializeTask( common._db.getTask() )
                common.logger.debug(taskXML)
            except Exception, e:
                self.stateChange( self.submitRange, "Created" )
                msg = "BossLite ERROR: Unable to serialize task object\n"
                msg +="Project "+str(self.taskuuid)+" not Submitted \n"
                msg += str(e)
                raise CrabException(msg)

            # TODO fix not needed first field
            subOutcome = csCommunicator.submitNewTask(self.taskuuid, taskXML, self.submitRange, totJob)
        else:
            # subsequent submissions and resubmit
            self.stateChange( self.submitRange, "SubRequested" )
            try:
                subOutcome = csCommunicator.subsequentJobSubmit(self.taskuuid, self.submitRange)
            except Exception, ex: ##change to specific exception
                ## clean sub. requested status
                self.stateChange( self.submitRange, "Created" )


        if subOutcome != 0:
            msg = "ClientServer ERROR: %d raised during the communication.\n"%subOutcome
            self.stateChange( self.submitRange, "Created" )
            common.logger.debug(msg)
            raise CrabException('ERROR Jobs NOT submitted.')

        del csCommunicator

        return


    def markSubmitting(self):
        """
        _markSubmitting_
        sign local db for jobs sent -submitted- to the server
        (just for the first submission)
        """
        common.logger.debug("Updating submitting jobs %s"%str(self.submitRange))
        updlist = [{'statusScheduler':'Submitting', 'status':'CS'}] * len(self.submitRange)
        common._db.updateRunJob_(self.submitRange, updlist)


