#!/usr/bin/env python
"""
_CommandManager_

"""

__version__ = "$Revision: 1.6 $"
__revision__ = "$Id: CommandManagerComponent.py,v 1.6 2007/06/21 19:12:31 farinafa Exp $"

import os
import socket
import logging
import time
from logging.handlers import RotatingFileHandler
import xml.dom.minidom

from MessageService.MessageService import MessageService
from TaskTracking.TaskStateAPI import *
import commands

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session

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

        self.args['crabMaxRetry'] = 5

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
        logging.info("CommandManager Started...")


    def startComponent(self):
        """
        _startComponent_

        Start up the component
        """
        # create message service
        self.ms = MessageService()
        self.msFwdCmd = MessageService() 
        self.ms.registerAs("CommandManager")
        self.msFwdCmd.registerAs("CommandManagerForwarder")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("DropBoxGuardianComponent:NewCommand")
        self.ms.subscribeTo("CommandManager:StartDebug")
        self.ms.subscribeTo("CommandManager:EndDebug")

        # Events listening and translation
        while True :
             # Session.set_database(dbConfig)
             # Session.connect('ComMan_session')
             # Session.set_session('ComMan_session')
             # Session.start_transaction('ComMan_session')

             type, payload = self.ms.get()
             self.ms.commit()  
             logging.debug("CommandManager: %s %s" % ( type, payload))
             self.__call__(type, payload)

             # close the session
             # Session.commit('ComMan_session')
             # Session.close('ComMan_session')
        pass

    def processKillCommand(self, dict, filename, nretry):
        # 1 - get the taskName from the dict
        logging.info('debug message:%s'+str(dict) ) # Convert to debug # Fabio

        # locate taskName and dir to check if proxyTar has managed the task
        taskName = str(dict['Task'])   
        logging.debug('TaskName: ' + str(taskName))

        #Prapare massage payload 
        fwPayload = taskName+':'+self.dropBoxPath+'/'+taskName+'/share/userProxy'+':'+str(dict['Range'])
        # fwPayload = taskName+'::'+self.dropBoxPath+'/'+taskName+'/share/userProxy'+'::'+str(dict['Range']) Activate it when switch to new msg standard        

        #manages exceptions from listdir when there are not searched files in DropBox 
        dBStatus=[]
        try:
             dBStatus = os.listdir(self.dropBoxPath+'*.tgz')
        except Exception, e:
             logging.info("No .tgz in the DropBox: initialize dbStatus")
             dBStatus = []

        if taskName in [ s.split('.tgz')[0] for s in dBStatus]:
             logging.info('Task %s not yet managed.'%taskName)
             if nretry == '5':
                  logging.info('The command'+ str(filename) +' will be removed. ')
                  os.remove(filename)
                  self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
                  self.msFwdCmd.commit()
                  logging.info("Message TaskKilledFailed sent")
                  return

             logging.info('The command will be retried during next DropBox cycle.')
             newfilename = taskName + '.' + str(eval(nretry)+1) + '.xml'
             os.rename(filename, newfilename)  
             logging.info('The new file name is '+newfilename)
             #self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", newfilename, "00:00:30")
             #self.msFwdCmd.commit()
             return

        if taskName not in os.listdir(self.dropBoxPath):
             logging.info('Unable to locate directory for task %s'%taskName)

             if nretry == '5':
                  logging.info('The command'+ str(filename) +' will be removed. ')
                  os.remove(filename)
                  self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
                  self.msFwdCmd.commit()
                  logging.info("Message TaskKilledFailed sent")
                  return

             logging.info('The command will be retried during next DropBox cycle.')
             newfilename = taskName + '.' + str(eval(nretry)+1) + '.xml'
             os.rename(filename, newfilename)
             logging.info('The new file name is '+newfilename)
             #self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", newfilename, "00:00:30")
             #self.msFwdCmd.commit()
             return
        logging.info('Now Cheking the Proxy')


        # check proxy matching
        subject = ""
        try:
             proxyPath = self.dropBoxPath+'/'+taskName+'/share/userSubj'
             f = open(proxyPath, 'r')
             subject =f.readline()
             f.close()
        except Exception, e:
             logging.info("Warning: Unable to read " + str(taskName+'/share/userSubj'))
             logging.info(e)

             self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
             self.msFwdCmd.commit()
             logging.info("Message TaskKilledFailed sent")
             return

        if dict['Subject'].strip() != subject.strip():    
             logging.info('Unable to match subjects for %s'%taskName)
             logging.info('The command file will not be processed.')
             #os.remove(filename)
             os.rename(filename, filename+'.noGood')

             self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
             self.msFwdCmd.commit()
             logging.info("Message TaskKilledFailed sent") 
             return

        logging.info("Proxy subject verified") 

        # 2 - query BOSS to have the taskId

        # manages DB interaction problems and publish message for TT
        taskDict=""
        try:
             BSession = BossSession( self.args['bossClads'] )
             taskDict = BSession.loadByName(taskName)
             logging.info('loadByname terminated   '+str(taskDict))
             del BSession

             # PreKill for task not in BOSS (pre ProxyTar work finish)
             if len(taskDict)== 0 and dict['Range']=='all':
                  logging.info('Range = %s and task not in Boss: try PreKill     \n'%dict['Range'])
                  self.msFwdCmd.publish("CommandManagerComponent:prekill",fwPayload)
                  self.msFwdCmd.commit()
                  logging.info('Message to Watch Dog sended for task '+ taskName)
                  os.remove(filename) 
                  return  
        except Exception, e:

             # PreKill  
             if dict['Range']=='all':
                  logging.info('Range = %s and task not in Boss: try PreKill     \n'%dict['Range'])
                  self.msFwdCmd.publish("CommandManagerComponent:prekill",fwPayload)
                  self.msFwdCmd.commit()
                  logging.info('Message to Watch Dog sended for task '+ taskName)
                  os.remove(filename)  
             else: 
                  logging.info('Problems with DB interaction  %s\n'%filename + str(e))
                  if nretry == '5':
                       logging.info('The command'+ str(filename) +' will be removed. ')
                       os.remove(filename)
                       self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
                       self.msFwdCmd.commit()
                       logging.info("Message TaskKilledFailed sent")
                       return

                  logging.info('The command will be retried during next DropBox cycle.')
                  newfilename = taskName + '.' + str(eval(nretry)+1) + '.xml'
                  os.rename(filename, newfilename)
                  logging.info('The new file name is '+newfilename)
                  #self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:01:00")
                  #self.msFwdCmd.commit()
             return

        taskSpecId = ''
        
        # Retrive tasks status from TT
        try:
             stat=getStatus(taskName)[0]
             logging.info('Task status returned by TT is:   '+str(stat[0]))
        except Exception, e:
             logging.info('Problems with TT Api for:  %s . The command will be retried!\n'%filename + str(e))
             if nretry == '5':
                  logging.info('The command'+ str(filename) +' will be removed. ')
                  os.remove(filename)
                  self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
                  self.msFwdCmd.commit()
                  logging.info("Message TaskKilledFailed sent")
                  return

             logging.info('The command will be retried during next DropBox cycle.')
             newfilename = taskName + '.' + str(eval(nretry)+1) + '.xml'
             os.rename(filename, newfilename)
             logging.info('The new file name is '+newfilename)
             #self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:01:00")
             #self.msFwdCmd.commit()
             return

        if len(taskDict) > 0:
             
             # Killable status  
             if stat[0] in ["partially submitted", "submitted", "ended", "partially killed", "range submitted"]:
                  taskSpecId = taskName

             # No killable status
             elif stat[0] in ["not submitted","killed"]: 
                  logging.info('Task %s is in no killable status. The command will be remove '%taskName)
                  self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
                  self.msFwdCmd.commit()
                  logging.info("Message TaskKilledFailed sent") 
                  os.remove(filename)
                  return

             # Status consistent with PreKill  
             elif stat[0] in ["submitting","arrived","unpacked"]:
                  if dict['Range']=='all':
                       logging.info('Range = %s and status submitting: try PreKill     \n'%dict['Range'])
                       self.msFwdCmd.publish("CommandManagerComponent:prekill",fwPayload)
                       self.msFwdCmd.commit()
                       logging.info('Message to Watch Dog sended for task '+ taskName)
                       os.remove(filename)
                  else: 
                       logging.info('Task %s is in no killable status. The command will be retried'%taskName)
                       self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:01:00")
                       self.msFwdCmd.commit()
                  return

             # Unexpected status  
             else:    
                  del taskDict
                  logging.info('Unexpected status '+ str(stat[0]) +' for the task %s. The command will be retried!' %taskName)
                  if nretry == '5':
                       logging.info('The command'+ str(filename) +' will be removed. ')
                       os.remove(filename)
                       self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
                       self.msFwdCmd.commit()
                       logging.info("Message TaskKilledFailed sent")
                       return

                  logging.info('The command will be retried during next DropBox cycle.')
                  newfilename = taskName + '.' + str(eval(nretry)+1) + '.xml'
                  os.rename(filename, newfilename)
                  logging.info('The new file name is '+newfilename)
                  #self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:01:00")
                  #self.msFwdCmd.commit()
                  return
        else:
              logging.info('Taskdict for task %s is empty. No jobs to kill: The command will be remove '%taskName)
              self.msFwdCmd.publish("TaskKilledFailed", taskName+'::'+dict["Range"])
              self.msFwdCmd.commit()
              os.remove(filename)
              logging.info("Message TaskKilledFailed sent")
              return
         
        # 3 - publish the message to the JobKiller
        logging.info('Now pubblish the message for jobKiller')
        del taskDict
        self.msFwdCmd.publish('KillTask', fwPayload, "00:00:10")
        self.msFwdCmd.commit()
        logging.info('....Command dispatched!')

        # The file is no more needed. Remove it 
        os.remove(filename)
        pass

    def processSubmitRangeCommand(self, dict, filename, nretry):
        # Create a fake ProxyTar message to drive the CW to the submission
        subject = ''
        taskName = str(dict['Task']) 
        proxyPath = self.dropBoxPath+'/'+taskName+'/share/userSubj'

        if taskName not in os.listdir(self.dropBoxPath):
             logging.info('Unable to locate directory for task %s'%taskName + '. The command will be retried later.')
             if nretry == '5':
                  logging.info('The command'+ str(filename) +' will be removed. ')
                  logging.info("Partial submission wont be performed.") 
                  os.remove(filename)
                  return

             logging.info('The command will be retried during next DropBox cycle.')
             newfilename = taskName + '.' + str(int(nretry)+1) + '.xml'
             os.rename(filename, newfilename)
             logging.info('The new file name is '+newfilename)
             #self.msFwdCmd.publish("DropBoxGuardianComponent:NewCommand", filename, "00:00:30")
             #self.msFwdCmd.commit()
             return

        # check the proxy matching
        try:
             f = open(proxyPath, 'r')
             subject = f.readline()
             f.close()
        except Exception, e:
             logging.info("Warning: Unable to read " + str(taskName+'/share/userSubj') + str(e) )
             logging.info("Partial submission wont be performed.")
             return

        if dict['Subject'].strip() != subject.strip():
             logging.info('Unable to match subjects for %s'%taskName)
             logging.info('The partial submission command file will not be processed.')
             os.rename(filename, filename+'.noGood')
             return

        # push the message
        cwMsg = proxyPath+'::'+taskName+'::'+str(dict['Range'])+'::'+str(self.args['crabMaxRetry'])
        self.msFwdCmd.publish("ProxyTarballAssociatorComponent:CrabWork", cwMsg)
        self.msFwdCmd.commit()

        # The file is no more needed. Remove it
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
                numretry = '0'
 
                # Extract the number of retry
                if len(payload.split("."))==3: 
                    numretry=payload.split(".")[1] 
                    logging.info('Retry number '+str(numretry))

                doc = xml.dom.minidom.parse(payload)
                dict = {'Range':'all'}
                for node in doc.documentElement.childNodes:
                    if node.attributes:
		        for i in range(node.attributes.length):
		            a = node.attributes.item(i)
		            dict[str(node.attributes.item(i).name)] = str(node.attributes.item(i).value)
                doc.unlink()
                
                # Dispatch the command to the proper handler/component
                if str(dict['Command'])=='kill':
                     logging.info('Now Processing Kill Command!')                        
                     self.processKillCommand(dict, payload, numretry)   # MODIFICATO
                elif str(dict['Command'])=='submit_range':
                     logging.info('Now Processing Range Submission Directive Command!')
                     self.processSubmitRangeCommand(dict, payload, numretry)
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

