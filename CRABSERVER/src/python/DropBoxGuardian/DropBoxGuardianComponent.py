#!/usr/bin/env python
"""
_DropBoxGuardianComponent_

"""

__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: DropBoxGuardianComponent.py,v 1.1 2006/11/08 18:12:00 farinafa Exp $"

import os
import socket
import pickle
import logging
import time
from logging.handlers import RotatingFileHandler

from MessageService.MessageService import MessageService

class DropBoxGuardianComponent:
    """
    _DropBoxGuardianComponent_

    """
    def __init__(self, **args):
        self.args = {}
        self.args['sleepTime'] = None
        self.args['dropBoxPath'] = None
        self.args['ProxiesDir'] = None
        self.args['Logfile'] = None
        self.args.update(args)
        
           
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        if self.args['dropBoxPath'] == None:
             self.args['dropBoxPath'] = self.args['ComponentDir']

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
        
        # Load the drop box set file
        self.sleepTime = int(self.args['sleepTime'])
        self.dropBoxPath = str(self.args['dropBoxPath'])
        self.proxyDir = str(self.args['ProxiesDir'])
	
        try:
            path = self.dropBoxPath+"/drop.set"
            logging.info("Opening Dropbox "+path)
            f = open(path, 'r')
            lload = pickle.load(f)
            f.close()
            self.previousDropBoxStatus = lload[0]
            self.previousProxyList = lload[1]
            self.tarSizeRegistry = lload[2]
            logging.info("dropBox status loaded from file")
        except IOError, ex:
            logging.info("Building a new dropBox status")
            self.previousDropBoxStatus = [] # python 2.2 lacks set object
            self.previousProxyList = []
            self.tarSizeRegistry = {} 
            self.materializeDropBoxStatus()

        logging.info("DropBoxGuardianComponent Started...")

    def materializeDropBoxStatus(self):
        ldump = []
        ldump.append(self.previousDropBoxStatus)
        ldump.append(self.previousProxyList)
	ldump.append(self.tarSizeRegistry)
 
        f = open( self.dropBoxPath+"/drop.set", 'w')
        pickle.dump(ldump, f)
        f.close()

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events

        """
        logging.debug("Event: %s %s" % (event, payload))
        # No specific event handling for these component
        # It just push tickets
        if event == "DropBoxGuardianComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "DropBoxGuardianComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        return 
        
    def startComponent(self):
        """
        _startComponent_

        Start up the component
        """
     
        # create message service
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("DropBoxGuardianComponent")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("DropBoxGuardianComponent:StartDebug")
        self.ms.subscribeTo("DropBoxGuardianComponent:EndDebug")

        while True:
            ## Watch the dropbox and find new arrivals
            time.sleep(self.sleepTime)
            dropBoxStatus = os.listdir(self.dropBoxPath)
	    
            # Marco. Modified for Kill command
            newCommand = []
            newCommand = [s for s in dropBoxStatus if (s not in self.previousDropBoxStatus) and (s.split('.')[-1]=='xml')]

            # Watch for new projects
            newFiles = []
	    preservedTars = []
            ## newFiles = [s.split('.tgz')[0] for s in dropBoxStatus if (s not in self.previousDropBoxStatus) and (s.split('.')[-1]=='tgz')]
            # gridFTP tar arrival bugfix -- Fabio
            for s in dropBoxStatus:
                 if s.split('.')[-1]=='tgz' and (s not in self.previousDropBoxStatus):
                     tarSize = 0
                     try:
                          tarSize = int(os.stat(os.path.join(self.dropBoxPath ,s))[6])
                          if s in self.tarSizeRegistry and tarSize == self.tarSizeRegistry[s]:
                               newFiles.append(s.split('.tgz')[0])
                               del self.tarSizeRegistry[s]
                          else:
                               self.tarSizeRegistry[s] = tarSize
                               preservedTars.append(s)
                     except Exception, e:
                          logging.info("%s has encoutered problems, it wont be processed."%s + '\n'+ str(e))
                          if s in self.tarSizeRegistry:
                               del self.tarSizeRegistry[s]
                          pass
                 pass
            pass

            # Watch for new proxies iif you have not recived new project files
            newProxies = []
            if newFiles == []: 
                  proxyList = os.listdir(self.proxyDir)
                  newProxies = [p for p in proxyList if (p not in self.previousProxyList) ]
                  self.previousProxyList = proxyList
            else:
                  self.previousProxyList=os.listdir(self.proxyDir)

            # Update persistent data-structures
            self.previousDropBoxStatus = dropBoxStatus
	    for t in preservedTars:
	    	self.previousDropBoxStatus.remove(t)
		
            self.materializeDropBoxStatus()
	    
	    # Logging
            if newFiles != []:
                 logging.info("New Project(s) Arrived:" + str(len(newFiles)) )
                 logging.info(newFiles) # change me if needed # F&M
            if newProxies != []:
                 logging.info("New Proxy Arrived:" + str(len(newProxies)) )
                 logging.debug(newProxies)
            if newCommand != []:
                 logging.info("New Command Arrived:" + str(len(newCommand)) )
                 logging.debug(newCommand)

            # Notify the arrival for the new tarball
            for i in newFiles:
               self.ms.publish("DropBoxGuardianComponent:NewFile",i)
            for i in newCommand:
               self.ms.publish("DropBoxGuardianComponent:NewCommand",i)
            if newProxies != [] and newFiles==[]:
               self.ms.publish("DropBoxGuardianComponent:NewProxy", "")
            self.ms.commit()


