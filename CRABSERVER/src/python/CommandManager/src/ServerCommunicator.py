#!/usr/bin/env python
"""
_ServerCommunicator_

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "farinafa@cern.ch"

from crab_exceptions import *
from crab_logger import Logger
import common

from CRAB_Server_API import CRAB_Server_Session as C_AS_Session
# from CRAB_Server_fastAPI import CRAB_Server_https as C_AS_Session
from xml.dom import minidom
import os

class ServerCommunicator:
    """
    Common interface for the interaction between the Crab client and the server Web Service    
    """
    def __init__(self, serverName, serverPort, cfg_params, proxyPath):
        """
        Open the communication with an Analysis Server by passing the server URL and the port
        """
  
        self.asSession = C_AS_Session(serverName, serverPort)
        self.cfg_params = cfg_params
        # self.blSession = bossSession
        self.userSubj = ''
        self.serverName = serverName
        
        # get the user subject from the proxy
        import commands
        x509 = None
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

    def submitNewTask(self, blTaskId, blTaskName, blXml, rng):
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

        taskUniqName = None
        taskUniqName = self._extractTaskUniqName(blTaskId)
        if not taskUniqName:
            raise CrabException('Error while extracting the Task Unique Name string')
            return -3

        cmdXML = None
        cmdXML = self._createXMLcommand('submit', rng, newTaskAddIns=True)
        if not cmdXML:
            raise CrabException('Error while creating the Command XML')
            return -4

        ret = -1  
        ret = self.asSession.transferTaskAndSubmit(blXml, cmdXML, taskUniqName)
        logMsg = ''
        if ret == 0:
             # success
             logMsg = 'Task %s successfully submitted to server %s'%(blTaskName, self.serverName)
        elif ret == 10:
             # overlaod
             logMsg = 'The server %s refused to accept the task %s because it is overloaded'%(self.serverName, blTaskName)
        elif ret == 11
             # failed to push message in PA
             logMsg = 'Backend unable to release messages to trigger the computation of task'%blTaskName
        elif ret == 12:
             # failed SOAP communication
             logMsg = 'Error during SOAP communication with server %s.\n'%self.serverName
             logMsg +='\t ----- The server could be under maintainance. ----- '
        else:
             logMsg = 'Unexpected return code from server %s: %d'%(self.serverName, ret)

        # print loggings
        common.logger.message(logMsg)
        return ret
         
    def subsequentJobSubmit(self, blTaskId, blTaskName, blXml, rng):
        """
        _subsequentJobSubmit_
        Let the submission of other jobs of a task that has been already sent to a server.
        This method is used for subsequent submission of ranged sets of jobs.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('submit', blTaskId, blTaskName, rng)

    def killJobs(self, blTaskId, blTaskName, blXml, rng):
        """
        _killJobs_
        Send a kill command to one or more jobs running on the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('kill', blTaskId, blTaskName, rng)

    def cleanJobs(self, blTaskId, blTaskName, rng):
        """
        _cleanJobs_
        Force the server to clean the jobs on the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        return self._genericCommand('clean', blTaskId, blTaskName, rng)

    def getStatus(self, blTaskId, blTaskName, rng):
        """
        _getStatus_
        Retrieve the task status from the server.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        # fill the filename
        filename = None

        taskUniqName = None
        taskUniqName = self._extractTaskUniqName(blTaskId)
        if not taskUniqName:
            # raise exception
            raise CrabException('Exception while getting the task unique name')
            return ''

        # get the data and fill the file content
        statusMsg = self.asSession.getTaskStatus(taskUniqName)
        f = open(filename, 'w')
        f.write(statusMsg)
        f.close()
        
        # check if foreseen errors occurred
        if 'Error:' in  statusMsg[:6]:
             common.logger.message('Error occurred while retrieving task %s status from server %s'%(blTaskName, self.serverName))
        return filename

    def getJobsOutput(self, blTaskId, blTaskName, rng):
        """
        _getJobsOutput_
        Get from the server the output file locations to be transfered back.

        Accepts in input:
             - the bossLite object representing the task (jobs are assumed to be RunningJobs)
             - the range of the submission as specified by the user at the command line
        """
        # Handle data transfers here
        raise NotImplementedError
        return None

    def postMortemInfos(self, blTaskId, blTaskName, rng):
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
        root = self.cfile.createElement("TaskCommand")
            
        node = self.cfile.createElement("TaskAttributes")
        node.setAttribute("Task", taskUName)
        node.setAttribute("Subject", self.userSubj)
        node.setAttribute("Command", cmdSpec)
        node.setAttribute("Range", rng)
        
        # first submission specific attributes: not available or not considered for the other kind of messages
        if (newTaskAddIns == True):
            node.setAttribute("Scheduler", self.cfg_params['CRAB.scheduler'])
            # TODO not clear what it means # Fabio
            # node.setAttribute("Service", self.cfg_params[''])
        
        # create a mini-cfg to be transfered to the server
        miniCfg = {}
        miniCfg['EDG.ce_white_list'] = str( self.cfg_params['EDG.ce_white_list'] )
        miniCfg['EDG.ce_black_list'] = str( self.cfg_params['EDG.ce_black_list'] )
        self.cfg_params['cfgFileNameCkSum'] = makeCksum(common.work_space.cfgFileName())
        ## put here other fields if needed

        node.setAttribute("CfgParamDict", str(miniCfg) )

        root.appendChild(node)
        self.cfile.appendChild(root)
        xmlString += str(cfile.toxml())
        #
        return xmlString

    def _genericCommand(self, cmd, blTaskId, blTaskName, rng):
        taskUniqName = None
        taskUniqName = self._extractTaskUniqName(blTaskId)
        if not taskUniqName:
            raise CrabException('Error while extracting the Task Unique Name string')
            return -2

        cmdXML = None
        cmdXML = self._createXMLcommand(cmd)
        if not cmdXML:
            raise CrabException('Error while creating the Command XML')
            return -3

        ret = -1
        ret = self.asSession.sendCommand(cmdXML, taskUniqName)
        logMsg = ''
        if ret == 0:
             # success
             logMsg = 'Task %s successfully submitted to server %s'%(blTaskName, self.serverName)
        elif ret == 20:
             # failed to push message in PA
             logMsg = 'Backend unable to release messages to trigger the computation of task'%blTaskName
        elif ret == 22:
             # failed SOAP communication
             logMsg = 'Error during SOAP communication with server %s'%self.serverName
        else:
             logMsg = 'Unexpected return code from server %s: %d'%(self.serverName, ret) 

        # print loggings
        common.logger.message(logMsg)  
        return ret
    
    def _extractTaskUniqName(self, taskId):
        tskUniqName = '' 
        workDir = str( os.path.basename(os.path.split(common.work_space.topDir())[0]) )
        tskUniqName = 'crab_'+ workDir + '_' + taskId
        return tskUniqName




