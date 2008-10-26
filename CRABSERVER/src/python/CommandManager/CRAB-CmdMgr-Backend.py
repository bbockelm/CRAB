# Business logic module for CRAB Server WS-based Proxy
# Acts as a gateway between the gSOAP/C++ WebService and the MessageService Component
__version__ = "$Revision: 1.26 $"
__revision__ = "$Id: CRAB-CmdMgr-Backend.py,v 1.26 2008/10/26 16:13:30 spiga Exp $"

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

class CRAB_AS_beckend:
    """
    _CRABProxyGateway_

    Gateway class from the WS CRAB-AS front-end to the server back-end messaging system
    
    """    
    def __init__(self):
        # load balancing feature. Use an integer, useful for future developments
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

    def initUiConfigs(self):
        """
        Download the UI Configuration files for the different Schedulers
        These files will be used by Submitting threads to address to correct Brokers
        """
        #### Adapted from Mattia contributions to old ProxyTar Component

        # Configuration files parameters
        schedList = ["edg", "glite"]    ## as well as above
        basicUrl = 'https://cmsweb.cern.ch/crabconf/files/'

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
            fileName = sched + '_wms_' + self.args['resourceBroker'] + '.conf' 
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
            if len(eval(xmlCmd.getAttribute('Range'), {}, {})) > 5000:
                self.log.info("Task refused for too large submission requirements: "+ taskUniqName)
                return 101
            # ONLY to help the deplyment ClientSide  
            if not xmlCmd.getAttribute('ClientVersion')  :
                self.log.info("ci PAS S0 ma non mi blocco %s ")
                skipClientCheck = 1
            if skipClientCheck == 0 :
                if xmlCmd.getAttribute('ClientVersion') not in ClientList :
                    self.log.info("Task %s refused since generated by uncompatible client version: %s"\
                                 %( taskUniqName,xmlCmd.getAttribute('ClientVersion')))
                    return 33 
 
            dirName = self.prepareTaskDir(taskDescriptor, cmdDescriptor, taskUniqName)
            if dirName is None:
                self.log.info('Unable to create directory tree for task %s'%taskUniqName) 
                return 11
              
            self.ms.publish("CRAB_Cmd_Mgr:NewTask", taskUniqName)
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
        
        # TODO? # Fabio
        # Put here some routing for the messages 'submit'->CW, 'kill'->CW+JK, ...
        # Postponed

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

            # TODO not yet used, but available
            cmdFlavour = str(xmlCmd.getAttribute('Flavour')) 
            cmdType = str(xmlCmd.getAttribute('Type'))

            # submission part
            if cmdKind in ['submit', 'resubmit']:
                # check for too large submissions
                if len( eval(cmdRng, {}, {}) ) > 5000:
                    self.log.info("Task refused for too large submission requirements: "+ taskUniqName)
                    return 101
                # send submission directive 
                msg = taskUniqName +"::"+ str(self.cmdAttempts) +"::"+ cmdRng
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

            # kill
            if cmdKind == 'kill':
                # prepare the killTask payload with the proper range
                # old killer payload format (as adopted by the killer)

                # WARNING: the field proxy is not needed for BossLite, 
                #    as it is included in the task object
                msg = taskUniqName + ':' + 'fake_proxy' + ':' + cmdRng 
                self.ms.publish("KillTask", msg)


                self.ms.commit()
                self.log.info("NewCommand Kill "+taskUniqName)
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
        self.log.info("TaskStatus requested "+taskUniqName+"(%s)"%statusType)
        retStatus, prjUName_fRep = ("", "")

        if statusType == "status":
            prjUName_fRep = self.wdir + "/" + taskUniqName + "_spec/xmlReportFile.xml"
        elif statusType == "serverLogging":
            prjUName_fRep = self.wdir + "/" + taskUniqName + "_spec/internalog.xml"
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


