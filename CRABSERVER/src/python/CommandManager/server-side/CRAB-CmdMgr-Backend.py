# Business logic module for CRAB Server WS-based Proxy
# Acts as a gateway between the gSOAP/C++ WebService and the MessageService Component
__version__ = "$Revision: 1.0 $"
__revision__ = "$Id: CRAB-Proxy.py, v 1.0 2007/12/12 19:21:47 farinafa Exp $"

import os
import time
import getopt
import urllib
import pickle
import zlib
from xml.dom import minidom

import logging
from logging.handlers import RotatingFileHandler
import traceback

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from MessageService.MessageService import MessageService
from JabberThread import JabberThread

class CRAB_AS_beckend:
    """
    _CRABProxyGateway_

    Gateway class from the WS CRAB-AS front-end to the server back-end messaging system
    
    """    
    def __init__(self):
        # load balancing feature. Use an integer, useful for future developments
        self.go_on_accepting = 1 # True
        #
        self.args = {}
        self.args["maxCmdAttempts"] = '5'
        self.cmdAttempts = int(self.args["maxCmdAttempts"])
        self.args['resourceBroker'] = 'CERN'

        self.ms = None
        self.jabber = None
        self.log = None
        self.initArgs()

        self.args['ComponentDir'] = os.path.expandvars(self.args['ComponentDir'])
        self.wdir = self.args['ComponentDir']
        
        if self.args['dropBoxPath']:
            self.wdir = self.args['dropBoxPath']

        self.initLogging()
        self.log.debug("wdir: %s, attempts: %s commands"%(self.wdir, self.cmdAttempts))

        self.ms = MessageService()
        self.ms.registerAs("CRAB_CmdMgr")
        self.log.info("Front-end message service created")

        self.initWLoadJabber()
        self.initUiConfigs()

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

        jabber_ms = MessageService()
        jabber_ms.registerAs("CRAB_CmdMgr_jabber")
        jabber_ms.subscribeTo("CrabServerWorkerComponent:FatWorkerResult")

        ## DISABLED FOR DEBUG
        self.args['acceptableThroughput'] = -1
        ## DISABLED FOR DEBUG
        self.log.debug("Create Jabber: thsLevel %d"%self.args['acceptableThroughput'])
        self.jabber = JabberThread(self, self.log, self.args['acceptableThroughput'], jabber_ms)
        pass

    def initUiConfigs(self):
        """
        Download the UI Configuration files for the different Schedulers
        These files will be used by Submitting threads to address to correct Brokers
        """
        #### Adapted from Mattia contributions to old ProxyTar Component

        # Configuration files parameters
        schedList = ["edg", "glite"]    ## as well as above
        basicUrl = 'http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/useful_script/'

        # Check if everything is already on the server
        existsUIcfgRB = os.path.exists( self.args['uiConfigRB'] )
        existsUIcfgWMS = os.path.exists( self.args['uiConfigWMS'] )
        existsUIcfgRBVO = os.path.exists( self.args['uiConfigRBVO'] )

        self.log.debug("Available configuration files:")
        self.log.debug("\t edg(VO):\t%s (%s)\n\t glite:\t%s\n"%(existsUIcfgRB, existsUIcfgWMS, existsUIcfgRBVO))
        
        if existsUIcfgRB and existsUIcfgWMS and existsUIcfgRBVO:
            return

        # Get the missing configurations
        self.log.info("Some configuration files are missing: downloading ...")
        for sched in schedList:
            # build the cfgFile filename
            fileName = sched + '.conf.CMS_' + self.args['resourceBroker']
            if sched == "edg":
               fileName = sched + '_wl_ui_cmd_var.conf.CMS_' + self.args['resourceBroker']
               
            # get data from http channel and save locally
            try:
                f = urllib.urlopen( basicUrl + fileName )
                ff = open(os.path.join( self.wdir, fileName ), 'w')
                ff.write(f.read())
                ff.close()
            except Exception, e:
                self.log.info( "Error while downloading configuration file %s:%s"%(fileName, e) )
                continue
            self.log.debug(basicUrl + fileName + " downloaded into: " + os.path.join( self.wdir, fileName ) )
        self.log.info('Download ended.')
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
        """
        
        if self.go_on_accepting != 1:
            self.log.info("Task refused for overloading: "+ taskUniqName)
            return 10

        try:
            # check for too large submissions
            xmlCmd = minidom.parseString(cmdDescriptor).getElementsByTagName("TaskCommand")[0]
            if len(eval(xmlCmd.getAttribute('Range'))) > 5000:
                self.log.info("Task refused for too large submission requirements: "+ taskUniqName)
                return 101

            dirName = self.prepareTaskDir(taskDescriptor, cmdDescriptor, taskUniqName) 
            if dirName is None:
                self.log.info('Unable to create directory tree for task %s'%taskUniqName) 
                return 11
              
            self.ms.publish("CRAB_Cmd_Mgr:NewTask", taskUniqName)
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
        
        # TODO? # Fabio
        # Put here some routing for the messages 'submit'->CW, 'kill'->CW+JK, ...
        # Postponed

        try:
            cmdName = self.wdir + '/' + taskUniqName + '_spec/cmd.xml'
            if os.path.exists(cmdName):
                os.rename(cmdName, cmdName+'.%s'%time.time())
            f = open(cmdName, 'w')
            f.write(cmdDescriptor)
            f.close()

            xmlCmd = minidom.parseString(cmdDescriptor).getElementsByTagName("TaskCommand")[0]
            cmdKind = str(xmlCmd.getAttribute('Command'))
            cmdRng = xmlCmd.getAttribute('Range') 

            # submission part
            if cmdKind in ['submit', 'resubmit']:
                # check for too large submissions
                if len(eval(cmdRng)) > 5000:
                    self.log.info("Task refused for too large submission requirements: "+ taskUniqName)
                    return 101
                # send submission directive 
                msg = taskUniqName + "::" + str(self.cmdAttempts)
                self.ms.publish("CRAB_Cmd_Mgr:NewCommand", msg)
                self.ms.commit()
                self.log.info("NewCommand Submit "+taskUniqName)
                return 0

            # getoutput
            if cmdKind == 'outputRetrieved':
                msg = taskUniqName + "::" + str(cmdRng)
                self.ms.publish("CRAB_Cmd_Mgr:GetOutputNotification", msg)
                self.ms.commit()
                self.log.info("NewCommand GetOutput "+taskUniqName)
                return 0

            # TODO kill
            # complete here            

        except Exception, e:
            self.log.info( traceback.format_exc() )

        # unknown message
        return 20
    
    def gway_getTaskStatus(self, taskUniqName=""):
        """
        Transfer the task status description to the client
        Return codes:
            XML content if successfull
            otherwise the methods return the error condition
        """
        self.log.info("TaskStatus requested "+taskUniqName)
        
        # TODO # Fabio
        # TO BE CHECKED FOR CONSISTENCY w.r.t. the rest of the system
        # NAME CONVENSION IDENTICAL TO THE ONE USED BY CURRENT SERVER CLIENT (see StatusServer.py, line 79)
        retStatus = ""

        prjUName_fRep = self.wdir + "/" + taskUniqName + "_spec/xmlReportFile.xml"
        try:
            f = open(prjUName_fRep, 'r')
            retStatus = f.readlines()
            f.close()
        except Exception, e:
            errLog = traceback.format_exc()  
            self.log.info( errLog )
            return str("Error: Unable to open %s"%prjUName_fRep)
        
        # return the document
        retStatus = "".join(retStatus)
        return retStatus


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


