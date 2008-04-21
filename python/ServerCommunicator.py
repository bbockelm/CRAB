#!/usr/bin/env python
"""
_ServerCommunicator_

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "farinafa@cern.ch"

from crab_exceptions import *
from crab_util import *
from crab_logger import Logger
import common

from CRAB_Server_API import CRAB_Server_Session as C_AS_Session
# from CRAB_Server_fastAPI import CRAB_Server_https as C_AS_Session
from xml.dom import minidom
import os
import commands

class ServerCommunicator:
    """
    Common interface for the interaction between the Crab client and the server Web Service    
    """
    def __init__(self, serverName, serverPort, cfg_params, proxyPath=None):
        """
        Open the communication with an Analysis Server by passing the server URL and the port
        """
  
        self.asSession = C_AS_Session(serverName, serverPort)
        self.cfg_params = cfg_params
        self.userSubj = ''
        self.serverName = serverName

        self.crab_task_name = common.work_space.topDir().split('/')[-2] # nice task name "crab_0_..."

        # get the user subject from the proxy
        x509 = proxyPath
        if x509 is None: 
            if 'X509_USER_PROXY' in os.environ:
                x509 = os.environ['X509_USER_PROXY']
            else:
                exitCode, x509 = commands.getstatusoutput('ls /tmp/x509up_u`id -u`').strip()
                if exitCode != 0:
                    raise CrabException("Error while locating the user proxy file")
                    return

        exitCode, self.userSubj = commands.getstatusoutput('openssl x509 -in %s -subject -noout'%x509)
        if exitCode != 0:
            raise CrabException("Error while getting the subject from the user proxy")  
            return

        self.userSubj = str(self.userSubj).strip()
        pass

###################################################
    # Interactions with the server
###################################################

    def submitNewTask(self, blTaskName, blXml, rng):
        """
        _submitNewTask_
        Send a new task to the server to be submitted. 
        
        Accepts in input: 
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)  
             - the range of the submission as specified by the user at the command line 
        """

        if not blXml:
            raise CrabException('Error while serializing task object to XML')
            return -2

        if not blTaskName:
            raise CrabException('Error while extracting the Task Unique Name string')
            return -3

        cmdXML = None
        cmdXML = self._createXMLcommand(blTaskName, 'submit', rng, newTaskAddIns=True)
        if not cmdXML:
            raise CrabException('Error while creating the Command XML')
            return -4

        ret = -1  
        ret = self.asSession.transferTaskAndSubmit(blXml, cmdXML, blTaskName)
        logMsg = ''
        if ret == 0:
             # success
             logMsg = 'Task %s successfully submitted to server %s'%(self.crab_task_name, self.serverName)
        elif ret == 10:
             # overlaod
             logMsg = 'The server %s refused to accept the task %s because it is overloaded'%(self.serverName, self.crab_task_name)
        elif ret == 101:
             # overlaod
             logMsg = 'The server %s refused the submission %s because you asked a too large task. Please submit by range'%(self.serverName, self.crab_task_name)
        elif ret == 11:
             # failed to push message in DB
             logMsg = 'Backend unable to release messages to trigger the computation of task %s'%self.crab_task_name
        elif ret == 12:
             # failed SOAP communication
             logMsg = 'Error during SOAP communication with server %s.\n'%self.serverName
             logMsg +='\t ----- The server could be under maintainance. ----- '
        else:
             logMsg = 'Unexpected return code from server %s: %d'%(self.serverName, ret)

        # print loggings
        common.logger.message(logMsg+'\n')
        return ret
         
    def subsequentJobSubmit(self, blTaskName, rng):
        """
        _subsequentJobSubmit_
        Let the submission of other jobs of a task that has been already sent to a server.
        This method is used for subsequent submission of ranged sets of jobs.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('submit', blTaskName, rng)

    def killJobs(self, blTaskName, rng):
        """
        _killJobs_
        Send a kill command to one or more jobs running on the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('kill', blTaskName, rng)

    def cleanJobs(self, blTaskName, rng):
        """
        _cleanJobs_
        Force the server to clean the jobs on the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('clean', blTaskName, rng)

    def getStatus(self, blTaskName, statusFile=None):
        """
        _getStatus_
        Retrieve the task status from the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        # fill the filename
        filename = str(statusFile)

        if not blTaskName:
            raise CrabException('Exception while getting the task unique name')
            return ''

        # get the data and fill the file content
        statusMsg = self.asSession.getTaskStatus(blTaskName)
        if 'Error:' in  statusMsg[:6] or len(statusMsg)==0:
             raise CrabException('Error occurred while retrieving task %s status from server %s'%(self.crab_task_name, self.serverName) )
             return

        if statusFile is not None:
            f = open(statusFile, 'w')
            f.write(statusMsg)
            f.close()
            return statusFile 
        return statusMsg

    def outputRetrieved(self, blTaskName, rng):
        """
        _getJobsOutput_
        Get from the server the output file locations to be transfered back.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('outputRetrieved', blTaskName, rng)

    def postMortemInfos(self, blTaskName, rng):
        """
        _postMortemInfos_
        Retrieve the job postmortem information from the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        # get the status in 
        raise NotImplementedError
        return None

###################################################
    # Auxiliary methods
###################################################
    
    def _createXMLcommand(self, taskUName, cmdSpec='status', rng='all', newTaskAddIns=False):
        xmlString = ''
        cfile = minidom.Document()
            
        node = cfile.createElement("TaskCommand")
        node.setAttribute("Task", str(taskUName) )
        node.setAttribute("Subject", str(self.userSubj) )
        node.setAttribute("Command", str(cmdSpec) )
        node.setAttribute("Range", str(rng) )
        node.setAttribute("Scheduler", str(self.cfg_params['CRAB.scheduler']) ) 

        # first submission specific attributes: not available or not considered for the other kind of messages
        if (newTaskAddIns == True):
            # add here one time fields if needed
            pass
            # node.setAttribute("Service", self.cfg_params[''])
        
        # create a mini-cfg to be transfered to the server
        miniCfg = {}

        ## migrate CE/SE infos
        miniCfg['EDG.ce_white_list'] = ""
        if 'EDG.ce_white_list' in self.cfg_params:
            miniCfg['EDG.ce_white_list'] = str( self.cfg_params['EDG.ce_white_list'] )

        miniCfg['EDG.ce_black_list'] = ""
        if 'EDG.ce_black_list' in self.cfg_params:
            miniCfg['EDG.ce_black_list'] = str( self.cfg_params['EDG.ce_black_list'] )

        miniCfg['EDG.se_white_list'] = ""
        if 'EDG.se_white_list' in self.cfg_params:
            miniCfg['EDG.se_white_list'] = str( self.cfg_params['EDG.se_white_list'] )

        miniCfg['EDG.se_black_list'] = ""
        if 'EDG.se_black_list' in self.cfg_params:
            miniCfg['EDG.se_black_list'] = str( self.cfg_params['EDG.se_black_list'] )

        miniCfg['cfgFileNameCkSum'] = makeCksum(common.work_space.cfgFileName()) 
        if 'cfgFileNameCkSum' in self.cfg_params:
            miniCfg['cfgFileNameCkSum'] = str(self.cfg_params['cfgFileNameCkSum'])

        miniCfg['CRAB.se_remote_dir'] = ''
        if 'CRAB.se_remote_dir' in self.cfg_params:
            miniCfg['CRAB.se_remote_dir'] = str(self.cfg_params['CRAB.se_remote_dir']) 

        ## JDL requirements specific data. Scheduler dependant
        miniCfg['EDG.max_wall_time'] = self.cfg_params.get('EDG.max_wall_clock_time', None)
        miniCfg['EDG.max_cpu_time'] = self.cfg_params.get('EDG.max_cpu_time', '130')
        miniCfg['proxyServer'] = self.cfg_params.get('EDG.proxy_server', 'myproxy.cern.ch')
        miniCfg['VO'] = self.cfg_params.get('EDG.virtual_organization', 'cms')
        miniCfg['EDG_retry_count'] = self.cfg_params.get('EDG.retry_count',0)
        miniCfg['EDG_shallow_retry_count'] = self.cfg_params.get('EDG.shallow_retry_count',-1)

        ## Additional fields for DashBoard
        miniCfg['CMSSW.datasetpath'] = self.cfg_params.get('CMSSW.datasetpath', 'None') 

        ## put here other fields if needed
        node.setAttribute("CfgParamDict", str(miniCfg) )
        cfile.appendChild(node)
        xmlString += str(cfile.toprettyxml())
        return xmlString

    def _genericCommand(self, cmd, blTaskName, rng):
        if not blTaskName:
            raise CrabException('Error while extracting the Task Unique Name string')
            return -2

        cmdXML = None
        cmdXML = self._createXMLcommand(blTaskName, cmd, rng)
        if not cmdXML:
            raise CrabException('Error while creating the Command XML')
            return -3

        ret = -1
        ret = self.asSession.sendCommand(cmdXML, blTaskName)
        logMsg = ''
        debugMsg = ''  
        if ret == 0:
             # success
             debugMsg = 'Task %s successfully submitted to server %s'%(self.crab_task_name, self.serverName)
        elif ret == 101:
             # overlaod
             logMsg = 'The server %s refused the submission %s because you asked to handle a too large task. Please submit by range'%(self.serverName, self.crab_task_name)
        elif ret == 20:
             # failed to push message in PA
             logMsg = 'Backend unable to release messages to trigger the computation of task %s'%self.crab_task_name
        elif ret == 22:
             # failed SOAP communication
             logMsg = 'Error during SOAP communication with server %s'%self.serverName
        else:
             logMsg = 'Unexpected return code from server %s: %d'%(self.serverName, ret) 

        # print loggings
        if logMsg != '':
            common.logger.message(logMsg+'\n')  
        else: 
            common.logger.debug(3,debugMsg+'\n')  
        return ret
    
