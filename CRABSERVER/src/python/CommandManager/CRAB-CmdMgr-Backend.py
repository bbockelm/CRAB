# Business logic module for CRAB Server WS-based Proxy
# Acts as a gateway between the gSOAP/C++ WebService and the MessageService Component
__version__ = "$Revision: 1.39 $"
__revision__ = "$Id: CRAB-CmdMgr-Backend.py,v 1.39 2010/03/02 16:40:08 riahi Exp $"

import threading
import os
import time
import getopt
import urllib
import pickle
import zlib
import base64
from xml.dom import minidom

import logging
from logging.handlers import RotatingFileHandler
import traceback

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from MessageService.MessageService import MessageService
from JabberThread import JabberThread

# WMCORE 
from WMCore.WMFactory import WMFactory
from WMCore.WMInit import WMInit
from WMCore.WMBS.Workflow import Workflow

class CRAB_AS_beckend:
    """
    _CRABProxyGateway_

    Gateway class from the WS CRAB-AS front-end to the server back-end messaging system

    """
    def __init__(self):
        # load balancing feature. Use an integer, useful for future developments
        self.args = {}
        self.args["maxCmdAttempts"] = '2'
        self.cmdAttempts = int(self.args["maxCmdAttempts"])
        self.args['resourceBroker'] = 'CERN'

        self.ms = None
        self.jabber = None
        self.log = None
        self.initArgs()

        self.args['ComponentDir'] = os.path.expandvars(self.args['ComponentDir'])
        self.wdir = self.args['ComponentDir']

        if self.args['CacheDir']:
            self.wdir = self.args['CacheDir']

        self.initLogging()
        self.log.debug("wdir: %s, attempts: %s commands"%(self.wdir, self.cmdAttempts))

        # Get configuration
        self.init = WMInit()
        self.init.setLogging()
        self.init.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))

        # CRAB_CmdMgr registration in WMCore.MsgService
        self.myThread = threading.currentThread()
        self.factory = WMFactory("msgService", "WMCore.MsgService."+ \
                             self.myThread.dialect)
        self.newMsgService = self.myThread.factory['msgService'].loadObject("MsgService")
        self.myThread.transaction.begin()
        self.newMsgService.registerAs("CRAB_CmdMgr")
        self.myThread.transaction.commit()

        self.ms = MessageService()
        self.ms.registerAs("CRAB_CmdMgr")
        self.log.info("Front-end message service created")

        self.initWLoadJabber()

        self.log.info("Python gateway loaded ...")
        self.log.info("CRAB Server gateway service working directory: %s"%self.wdir)

        self.log.info("Waiting for RPC...")
        pass

################################
#   Initialization Methods
################################

    def initArgs(self):
        """
        Read the XML configuration of the server and parse the required data
        """

        try:
            config = loadProdAgentConfiguration()
            self.args.update( config.getConfig("CommandManager") )
            self.args.update( config.getConfig("CrabServerConfigurations") )
        except StandardError, ex:
            msg = "Error reading configuration:\n"
            msg += str(ex)
            raise RuntimeError, msg
            sys.exit(-1)

        self.args['ComponentDir'] = os.path.expandvars(self.args['ComponentDir'])
        return

    def initLogging(self):
        """
        Initialize the logging system
        """

        # Logging system init
        self.log = logging

        logging.info("CRABProxyGateway allocating ...")
        logging.info("Component arguments parsed")
        logging.info("Logging system initialized")
        pass

    def initWLoadJabber(self):
        """
        This method creates a thread that monitors the front-end load with respect to the
        actual throughput that the server itself is able to handle.

        Whenever the back-end signals an overload condition the jabber suspends the acceptance
        of new tasks for a fixed time.
        """

        ## DISABLED FOR DEBUG
        #self.args['acceptableThroughput'] = -1
        ## DISABLED FOR DEBUG
        self.log.debug("Create Jabber: thsLevel %d"%int(self.args['acceptableThroughput']) )
        self.jabber = JabberThread(self, self.log, int(self.args['acceptableThroughput']) )
        pass

###############################
#   Service Logic Methods
###############################

    def gway_transferTaskAndSubmit(self, taskDescriptor="", cmdDescriptor="", taskUniqName=""):
        """
        Method summoned for new task submission.
        Return codes semantics:
            0   success (CrabServerProxy:NewTask message published)
            10  task refused for exceeding overload
            11  error during MessageService sending
            14  draining
           102
            33 client compatibility
        """

        skipClientCheck = 0
        allowedClient = self.args.get('acceptableClient',None)
        if allowedClient:
            ClientList=[]
            if str(allowedClient).find(','):
                [ ClientList.append(x.strip()) for x in str(allowedClient).split(',')]
            else:
                ClientList.append(allowedClient)
        else :
            # ONLY to help the deplyment ServerSide
            skipClientCheck = 1

        if self.jabber.go_on_accepting_load == 0:
            self.log.info("Task refused for overloading: "+ taskUniqName)
            return 10
        elif self.jabber.go_on_accepting_load == 2:
            self.log.info("Server is in Draining: Task %s refused. "%taskUniqName)
            return 14

        try:
            # check for too large submissions
            xmlCmd = minidom.parseString(cmdDescriptor).getElementsByTagName("TaskCommand")[0]
            cmdRng = str(xmlCmd.getAttribute('Range'))
            # ONLY to guarantee BackComp  ClientSide
            cmdToTjobs = -1
            if xmlCmd.getAttribute('TotJob')  : cmdToTjobs = xmlCmd.getAttribute('TotJob')
            if len(eval(cmdRng, {}, {})) > 5000:
                self.log.info("Task refused for too large submission requirements: "+ taskUniqName)
                return 101
            # ONLY to guarantee BackComp  ClientSide
            if not xmlCmd.getAttribute('ClientVersion'): skipClientCheck = 1
            if skipClientCheck == 0 :
                if xmlCmd.getAttribute('ClientVersion') not in ClientList :
                    self.log.info("Task %s refused since generated by uncompatible client version: %s"\
                                 %( taskUniqName,xmlCmd.getAttribute('ClientVersion')))
                    return 33

            dirName = self.prepareTaskDir(taskDescriptor, cmdDescriptor, taskUniqName)
            if dirName is None:
                self.log.info('Unable to create directory tree for task %s'%taskUniqName)
                return 11

            self.ms.publish("CRAB_Cmd_Mgr:NewTask",'%s::%s::%s'%( taskUniqName,cmdToTjobs,cmdRng ) )
            self.ms.commit()

            # send additional informations for TT and Notification
            notifDict = eval(xmlCmd.getAttribute('CfgParamDict'))
            if 'eMail' in notifDict and notifDict['eMail']:
                msg = "%s::%s::%s"%(taskUniqName, notifDict['eMail'], notifDict.get('threshold',100) )
                self.ms.publish("CRAB_Cmd_Mgr:MailReference", msg)
                self.ms.commit()

        except Exception, e:
            self.log.info( traceback.format_exc() )
            return 11

        self.log.info("NewTask "+taskUniqName)
        return 0

    def gway_sendCommand(self, cmdDescriptor="", taskUniqName=""):
        """
        Single command submission to the server towards the CommandManager component.
        Return codes:
            0 success (CrabServerProxy:NewCommand)
            20 error while publishing the message
        """

        try:
            cmdName = self.wdir + '/' + taskUniqName + '_spec/cmd.xml'

            if not os.path.exists(self.wdir + '/' + taskUniqName + '_spec'):
                self.log.info("Task spec location missing %s "%taskUniqName)
                return 20

            if os.path.exists(cmdName):
                os.rename(cmdName, cmdName+'.%s'%time.time())

            f = open(cmdName, 'w')
            f.write(cmdDescriptor)
            f.close()

            xmlCmd = minidom.parseString(cmdDescriptor).getElementsByTagName("TaskCommand")[0]
            cmdKind = str(xmlCmd.getAttribute('Command'))
            cmdRng = str(xmlCmd.getAttribute('Range'))

            cmdFlavour = str(xmlCmd.getAttribute('Flavour'))
            cmdType = str(xmlCmd.getAttribute('Type'))


            msg = '%s::%s::%s::%s'%(taskUniqName, str(self.cmdAttempts), str(cmdRng), str(cmdKind))
            self.ms.publish("CRAB_Cmd_Mgr:NewCommand", msg)
            self.log.info("NewCommand %s for task %s"%(cmdKind, taskUniqName))
            self.ms.commit()
            # submission part
            if cmdKind in ['submit', 'resubmit']:
                # check for too large submissions
                if len( eval(cmdRng, {}, {}) ) > 5000:
                    self.log.info("Task refused for too large submission requirements: "+ taskUniqName)
                    return 101
                # send submission directive
                return 0

            # getoutput
            if cmdKind == 'outputRetrieved':
                msg = taskUniqName + "::" + str(cmdRng)
                self.ms.publish("CRAB_Cmd_Mgr:GetOutputNotification", msg)
                self.ms.commit()
                self.log.info("NewCommand GetOutput "+taskUniqName)
                return 0

            # kill
            if cmdKind == 'kill':
                msg = taskUniqName + ':' + cmdRng
                self.ms.publish("KillTask", msg)
                self.ms.commit()
                self.log.info("NewCommand Kill "+taskUniqName)
                return 0

            # StopWorkflow 
            if cmdKind == 'StopWorkflow':
                self.stopWorkflow(taskUniqName)
                self.log.info("NewCommand StopWorkflow "+taskUniqName)
                return 0

            # clean
            if cmdKind == 'clean':
                # Clean triggered through task-life manager.
                # Once invoked the task get scheduled for cleaning as if it is
                # stored on the server for a long time
                msg = taskUniqName
                self.ms.publish("CRAB_Cmd_Mgr:CleanRequest", msg)
                self.ms.commit()
                self.log.info("NewCommand Clean "+taskUniqName)
                return 0

            # complete here with additional message classes

        except Exception, e:
            self.log.info( traceback.format_exc() )

        # unknown message
        return 20

    def gway_getTaskStatus(self, statusType="status", taskUniqName=""):
        """
        Transfer the task status description to the client
        Return codes:
            XML content if successfull
            otherwise the methods return the error condition
        """
        if taskUniqName is None or statusType is None:
            retStatus = "Error: unrecognized [%s] of status information required for [%s]"%(str(statusType),str(taskUniqName))
            handledStatus = base64.urlsafe_b64encode(zlib.compress(retStatus))
            handledStatus += "="*( len(handledStatus)%8 )
            return handledStatus

        self.log.info("TaskStatus requested "+taskUniqName+"(%s)"%statusType)
        retStatus, prjUName_fRep = ("", "")

        if statusType == "status":
            prjUName_fRep = self.wdir + "/" + taskUniqName + "_spec/xmlReportFile.xml"
        elif statusType == "serverLogging":
            prjUName_fRep = self.wdir + "/" + taskUniqName + "_spec/internalog.xml"
        elif statusType == "isServerDrained":
            retStatus = "false"
            if self.jabber.go_on_accepting_load != 1: 
                retStatus =  "true"
        else:
            prjUName_fRep = None
            retStatus = "Error: unrecognized kind of status information required"

        # collect the information from source
        if prjUName_fRep:
            if not os.path.exists(prjUName_fRep):
                retStatus = "Error: file %s not exists"%prjUName_fRep
            else:
                try:
                    f = open(prjUName_fRep, 'r')
                    retStatus = f.readlines()
                    f.close()
                except Exception, e:
                    self.log.debug( traceback.format_exc() )

        # return the document
        retStatus = "".join(retStatus)
        handledStatus = base64.urlsafe_b64encode(zlib.compress(retStatus))
        handledStatus += "="*( len(handledStatus)%8 )
        return handledStatus #retStatus


###############################
#   Auxiliary Methods
###############################

    def prepareTaskDir(self, taskDescriptor, cmdDescriptor, taskUniqName):
        """
        create the task directory storing the commodity files for the task

        """
        taskDir = self.wdir + '/' + taskUniqName + '_spec'

        if not os.path.exists(taskDir):
            try:
                os.mkdir(taskDir)
                os.chmod(taskDir,0755)
            except Exception, e:
                self.log.info("Error crating directories %s"%taskDir)
                self.log.info( traceback.format_exc() )
                return None

        try:
            f = open(taskDir + '/task.xml', 'w')
            f.write( taskDescriptor )
            f.close()
            del f
            f = open(taskDir + '/cmd.xml', 'w')
            f.write(cmdDescriptor)
            f.close()
        except Exception, e:
            self.log.info("Error saving xml files for task %s"%taskUniqName)
            self.log.info( traceback.format_exc() )
            return None

        return taskDir

################################
    def stopWorkflow(self, taskName):
        """
        Contact workflowManager component to remove the workflow from management 
        """

        self.myThread = threading.currentThread()

        # Get configuration
        self.init = WMInit()
        self.init.setLogging()
        self.init.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))

        self.log.info("Stopping workflow %s"%taskName)

        # Workflow creation
        wf = Workflow(name = "wf_"+taskName)

        try:
            self.log.info("Loading workflow with name %s" %("wf_"+taskName))
            wf.load()
            self.log.info("WorkFlow loaded has Id:%s"%wf.id)

        except Exception, e:

            logMsg = ("Problem when loading WF for %s" \
                      %("wf_"+taskName))
            logMsg += str(e)
            self.log.info( logMsg )
            self.log.debug( traceback.format_exc() )
            return

        dataset, feeder, processing, startrun = self.getWorkflowParameterFromXml(self.wdir, taskName)

        # CRAB sends a message to the WorkflowManager to remove the workflow from management
        self.myThread.transaction.begin()
        WFManagerdict = {'WorkflowId' : wf.id , 'FilesetMatch': \
                dataset + ':' + feeder + ':' + processing + ':' + startrun}

        WFManagerSent = pickle.dumps(WFManagerdict)
        msg = {'name' : 'RemoveWorkflowFromManagement', \
                'payload' : WFManagerSent}
        self.newMsgService.publish(msg)
        self.myThread.transaction.commit()
        self.newMsgService.finish()

    def getWorkflowParameterFromXml(self, wdir, taskName):
        from xml.dom import minidom
        status = 0
        cmdSpecFile = os.path.join(wdir, taskName + '_spec/cmd.xml' )
        try:
            doc = minidom.parse(cmdSpecFile)
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]
            self.cfg_params = eval( cmdXML.getAttribute("CfgParamDict"), {}, {} )
            dataset = self.cfg_params['CMSSW.datasetpath']
            feeder = self.cfg_params.get('feeder','Feeder')
            processing = self.cfg_params.get('processing','bulk')
            startrun = self.cfg_params.get('startrun','None') 

        except Exception, e:
            return None,None
        return dataset, feeder, processing, startrun

################################

