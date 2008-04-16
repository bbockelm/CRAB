from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf

import os, errno, time, sys, re 
import commands
import zlib

from ServerCommunicator import ServerCommunicator 
from ServerConfig import *

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface
 
class SubmitterServer(Actor):
    def __init__(self, cfg_params, parsed_range, val):
	self.srvCfg = {}
        self.cfg_params = cfg_params
        self.submitRange = []

        # range parse
        nsjobs = -1
        chosenJobsList = None
        if val:
            if val=='range':  # for Resubmitter
                chosenJobsList = parsed_range
            elif val=='all':
                pass
            elif (type(eval(val)) is int) and eval(val) > 0:
                # positive number
                nsjobs = eval(val)
            elif (type(eval(val)) is tuple)or( type(eval(val)) is int and eval(val)<0 ) :
                chosenJobsList = parsed_range
                nsjobs = len(chosenJobsList)
            else:
                msg = 'Bad submission option <'+str(val)+'>\n'
                msg += '      Must be an integer or "all"'
                msg += '      Generic range is not allowed"'
                raise CrabException(msg)
            pass
        common.logger.debug(5,'nsjobs '+str(nsjobs))

        # total jobs
        nj_list = []
        common.logger.debug(5,'Total jobs '+str(common._db.nJobs()))
        jobSetForSubmission = 0
        jobSkippedInSubmission = []

        tmp_jList = common._db.nJobs('list')
        if chosenJobsList != None:
            tmp_jList = chosenJobsList

        # build job list
        dlsDest=common._db.queryJob('dlsDestination',tmp_jList)
        jStatus=common._db.queryRunJob('status',tmp_jList)
        for nj in range(len(tmp_jList)):
            if nsjobs>0 and nsjobs == jobSetForSubmission:
                break
            if ( jStatus[nj] not in ['SS','SU','SR','R','S','K','Y','A','D','Z']):
                jobSetForSubmission +=1
                nj_list.append(tmp_jList[nj])## Warning added +1 for jobId BL--DS
            else :
                jobSkippedInSubmission.append(tmp_jList[nj])
            pass

        if nsjobs>jobSetForSubmission:
            common.logger.message('asking to submit '+str(nsjobs)+' jobs, but only '+str(jobSetForSubmission)+' left: submitting those')
        if len(jobSkippedInSubmission) > 0 :
            mess =""
            for jobs in jobSkippedInSubmission:
                mess += str(jobs) + ","
            common.logger.message("Jobs:  " +str(mess) + "\n      skipped because no sites are hosting this data\n")
            pass
        common.logger.debug(5,'nj_list '+str(nj_list))
        self.submitRange = nj_list 

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

        if len(self.submitRange) == 0:
            return

	isFirstSubmission = False

        self.taskuuid = str(common._db.queryTask('name'))
        self.remotedir = os.path.join(self.storage_path, self.taskuuid)
        self.proxyPath = self.moveProxy()

	# partial submission code # TODO is that file the right way? 
        ## NO! to be changed... query DB... 
	if os.path.exists(common.work_space.shareDir()+'/first_submission') == False:
            self.moveISB_SEAPI() 
	    #self.moveISB()
	    isFirstSubmission = True
	    os.system('touch %s'%str(common.work_space.shareDir()+'/first_submission'))

	# standard submission to the server
	common.logger.debug(5, "SubmitterServer::run() called")
	self.performSubmission(isFirstSubmission)
	return

    def moveISB_SEAPI(self):
        ## get task info from BL ##
        common.logger.debug(3, "Task name: " + self.taskuuid)
        isblist = common._db.queryTask('globalSandbox').split(',')
        common.logger.debug(3, "List of ISB files: " +str(isblist) )
        
        # init SE interface
        common.logger.message("Starting sending the project to the storage "+str(self.storage_name)+"...")
        seEl = SElement(self.storage_name, self.storage_proto, self.storage_port)
        loc = SElement("localhost", "local")

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

    def moveISB(self):
        ########################################
        ##  TODO Deprecated, remove this method. Use the above one # Fabio
        ########################################


	## get task info from BL ##
	common.logger.debug(3, "Task name: " + self.taskuuid)
	isblist = common._db.queryTask('globalSandbox').split(',')
        
	common.logger.debug(3, "List of ISB files: " +str(isblist) )
	scriptexe = common._db.queryTask('scriptName')
	common.logger.debug(3, "Executable: " +str(scriptexe) )
	common.logger.message("Starting sending the project to the storage "+str(self.storage_name)+"...")

	## create remote dir ##
	try:
	    cmd = "edg-gridftp-mkdir gsiftp://" +self.storage_name + self.remotedir
	    common.logger.debug(3, "Creating project directory on gsiftp://" + self.storage_name + self.remotedir)
	    common.logger.debug(5, " with:\n    " + cmd)
	    status, out = commands.getstatusoutput (cmd)
	    if int(status) != 0:
		common.logger.debug(1, str(out))
		msg = "ERROR : Unable to ship the project to the server \n"
		msg +="Project "+ self.taskuuid + " not Submitted \n"
		raise CrabException(msg)
	except Exception, ex:
	    common.logger.debug(1, str(ex))
	    msg = "ERROR : Unable to ship the project to the server \n"
	    msg +="Project "+ self.taskuuid +" not Submitted \n"
	    raise CrabException(msg)

	## copy ISB ##
	for filetocopy in isblist:
	    try:
		cmd = 'lcg-cp -v --vo cms file://%s'%os.path.abspath(filetocopy) + ' '
                cmd += 'gsiftp://' + self.storage_name + os.path.join(self.remotedir, os.path.basename(filetocopy))
		common.logger.debug(1, "Sending "+filetocopy+" to "+ self.storage_name)
		common.logger.debug(5, " with:\n    " + cmd)
		status, out = commands.getstatusoutput(cmd)
		if int(status) != 0:
		    common.logger.debug(1, str(out))
		    msg = "ERROR : Unable to ship the project to the server \n"
		    msg +="Project "+ self.taskuuid +" not Submitted \n"
		    raise CrabException(msg)
	    except Exception, ex:
		common.logger.debug(1, str(ex))
		msg = "ERROR : Unable to ship the project to the server \n"
		msg +="Project "+ self.taskuuid +" not Submitted \n"
		raise CrabException(msg)
	try:
	    cmd = 'lcg-cp -v --vo cms file://%s'%scriptexe + ' '
            cmd += 'gsiftp://' + self.storage_name + os.path.join(self.remotedir, os.path.basename(scriptexe))
	    common.logger.debug(3, "Sending "+scriptexe+" to "+ self.storage_name)
	    common.logger.debug(5, " with:\n    " + cmd)
	    status, out = commands.getstatusoutput(cmd)
	    if int(status) != 0:
		common.logger.debug(1, str(out))
		msg = "ERROR : Unable to ship the project to the server \n"
		msg +="Project "+ self.taskuuid+" not Submitted \n"
		raise CrabException(msg)
	except Exception, ex:
	    common.logger.debug(1, str(ex))
	    msg = "ERROR : Unable to ship the project to the server \n"
	    msg +="Project "+ self.taskuuid+" not Submitted \n"
	    raise CrabException(msg)

	## if here then project submitted ##
	msg = 'Project '+ self.taskuuid +' files successfully submitted to the supporting storage element.\n'
	common.logger.debug(3,msg)
	return

    def moveProxy(self):
	WorkDirName = os.path.basename(os.path.split(common.work_space.topDir())[0])

	## get subject ##
	x509 = None # TODO From task object alreadyFrom task object already  ? common._db.queryTask('proxy')
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


