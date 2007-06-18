#!/usr/bin/env python
"""
_CommandManager_

"""

__version__ = "$Revision: 1.0 $"
__revision__ = "$Id: CommandManager.py,v 1.0 2007/05/28 17:50:00 farinafa Exp $"

import os
import socket
import logging
import time
from logging.handlers import RotatingFileHandler
import xml.dom.minidom

from MessageService.MessageService import MessageService
from TaskTracking.TaskStateAPI import *
import commands

# BOSS API import
from BossSession import *

class CommandManagerComponent:
    """
    _CommandManager_

    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args['dropBoxPath'] = None
        self.args['bossClads'] = None

        self.args.update(args)
           
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        #  //
        # // Log Handler is a rotating file that rolls over when the
        #//  file hits 1MB size, 3 most recent files are kept
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        #  //
        # // Set up formatting for the logger and set the 
        #//  logging level to info level
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
       
        self.dropBoxPath = str( self.args['dropBoxPath'] )
        self.BSession = BossSession( self.args['bossClads'] )
        logging.info("CommandManager Started...")


    def startComponent(self):
        """
        _startComponent_

        Start up the component
        """
        # create message service
        self.ms = MessageService()
        self.msFwdCmd = MessageService() # To forward commands # Fabio
        self.ms.registerAs("CommandManager")
        self.msFwdCmd.registerAs("CommandManagerForwarder")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("DropBoxGuardianComponent:NewCommand")
        self.ms.subscribeTo("CommandManager:StartDebug")
        self.ms.subscribeTo("CommandManager:EndDebug")

        # Events listening and translation
        while True :
            type, payload = self.ms.get()
            self.ms.commit() # anticipated to avoid pending items to block the startup # Fabio 
            logging.debug("CommandManager: %s %s" % ( type, payload))
            self.__call__(type, payload)
            pass

    def processKillCommand(self, dict, filename):
        # 1 - get the taskName from the dict
        logging.info('debug message:%s'+str(dict) ) # Convert to debug # Fabio

        # locate taskName and dir to check if proxyTar has managed the task
        taskName = str(dict['Task'])   #.split('.')[0].strip() Matteo FIX: Task name is yet correct and ready to use
        logging.debug('TaskName:%s'+str(taskName))

        
        #Matteo add: manages exceptions from listdir when there are not searched files in DropBox 
        #
        dBStatus=[]
        try:
             dBStatus = os.listdir(self.dropBoxPath+'*.tgz')
        except Exception, e:
             logging.info("No .tgz in the DropBox: initialize dbStatus")
             dBStatus = []

        if taskName in [ s.split('.tgz')[0] for s in dBStatus]:
             logging.info('Task %s not yet managed.'%taskName)
             logging.info('The command will be retried during next DropBox cycle.')
             self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:00:30")
             self.msFwdCmd.commit()
             return

        if taskName not in os.listdir(self.dropBoxPath):
             logging.info('Unable to locate directory for task %s'%taskName)
             logging.info('The command file will not be processed.')
             #os.remove(filename)
             os.rename(filename, filename+'.noGood')
             return
        logging.info('Now Cheking the Proxy')


        # Matteo add: check proxy matching

        subject = ""
        try:
             f = open(taskName+'/share/userSubj', 'r')
             subject =f.readline()
             f.close()
        except Exception, e:
             logging.info("Warning: Unable to read " + str(taskName+'/share/userSubj'))
             logging.info(e)
             return

        logging.info(str(subject)+'   '+str(dict['Subject'])) 
        if dict['Subject'].strip() != subject.strip():    
             logging.info('Unable to match subjects for %s'%taskName)
             logging.info('The command file will not be processed.')
             #os.remove(filename)
             os.rename(filename, filename+'.noGood')
             return
        logging.info("Proxy subject verified") 
        # 2 - query the TT to get the taskId

        #Matteo add: manages DB interaction problems and publish message for TT
        taskDict=""
        try:
             taskDict = self.BSession.loadByName( taskName ) # just for crosscheck: exists iif CrabWorker performed the registration
             logging.info('loadByname terminated   '+str(taskDict))
        except Exception, e:
             logging.info('Problems with DB interaction  %s\n'%filename + str(e))
             self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:00:30")
             self.msFwdCmd.commit()
             return

        taskSpecId = ''
        
        # Retrive tasks status from TT
        try:
             stat="" 
             stat=getStatus(taskName)
             logging.info('Task status returned by TT:   '+str(stat))
        except Exception, e:
             logging.info('Problems with TT Api:  %s\n'%filename + str(e))
             self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:00:30")
             self.msFwdCmd.commit()
             return

        if len(taskDict) > 0 and stat not in ["partially submitted", "submitted"]:
             taskSpecId = taskName
        else:
             del taskDict
             logging.info('Task %s not found in BOSS or in no killable status. '%taskName)
             self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:00:30")
             self.msFwdCmd.commit()
             return

        # 3 - publish the message to the JobKiller
        logging.info('Now pubblish the message for jobKiller')
        del taskDict
        fwPayload = taskSpecId+':'+self.dropBoxPath+'/'+taskName+'/share/userProxy' #Matteo add: Payload is taskName:pathUserProxy
        self.msFwdCmd.publish('KillTask', fwPayload, "00:00:10")
        self.msFwdCmd.commit()
        logging.info('....Command dispatched!')
        # Command dispatched, the file is no more needed. Remove it #Fabio
        os.remove(filename)
        pass

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """
        logging.debug('Event: %s %s'%(event, payload))

        if event=='DropBoxGuardianComponent:NewCommand':
            try:
                # Parse XML file with generic command data structure
                os.chdir(self.dropBoxPath)
                doc = xml.dom.minidom.parse(payload)
                dict = {}
                for node in doc.documentElement.childNodes:
                    if node.attributes:
		        for i in range(node.attributes.length):
		            a = node.attributes.item(i)
		            dict[str(node.attributes.item(i).name)] = str(node.attributes.item(i).value)
                doc.unlink()

                #
                # Dispatch the command to the proper handler/component
                #
                if str(dict['Command'])=='kill':
                     logging.info('Now Process Kill Command!')                        
                     self.processKillCommand(dict, payload)
                elif str(dict['Command'])=='somethingElse':
                       # self.processSomethingElseCommand(dict, payload)
                       pass
                else:
                       logging.info('Unrecognized command for %s:\n%s'%(payload, dict))

            except Exception, e:
                logging.info('Unable to process the message %s\n'%payload + str(e))
            return

        # Logging events 
	if event=="CommandManagerComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event=="CommandManagerComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        return 

