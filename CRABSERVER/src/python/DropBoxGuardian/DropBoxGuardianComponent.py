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
            logging.info("dropBox status loaded from file")
        except IOError, ex:
            logging.info("Building a new dropBox status")
            self.previousDropBoxStatus = [] # python 2.2 lacks set object
            self.previousProxyList = []
            self.materializeDropBoxStatus()

        logging.info("DropBoxGuardianComponent Started...")

    def materializeDropBoxStatus(self):
        ldump = []
        ldump.append(self.previousDropBoxStatus)
        ldump.append(self.previousProxyList)
 
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

            ## Watch for new projects
            newFiles = []
            newProxies = []
            dropBoxStatus = os.listdir(self.dropBoxPath)
            newFiles = [s for s in dropBoxStatus if (s not in self.previousDropBoxStatus) and (s.split('.')[-1]=='tgz')]
            self.previousDropBoxStatus = dropBoxStatus

            ## Watch for new proxies iif you have not recived new project files
            if newFiles == []: 
                  proxyList = os.listdir(self.proxyDir)
                  newProxies = [p for p in proxyList if (p not in self.previousProxyList) and (p.split('.')[-1]=='proxy')]
                  self.previousProxyList = proxyList
            else:
                  self.previousProxyList=os.listdir(self.proxyDir)
            
            self.materializeDropBoxStatus()

            if newFiles != []:
                 logging.info("New Project(s) Arrived:" + str(len(newFiles)) )
            if newProxies != []:
                 logging.info("New Proxy Arrived:" + str(len(newProxies)) )

            ## Notify the arrival for the new tarball
            for i in newFiles:
               self.ms.publish("DropBoxGuardianComponent:NewFile",i)
            # no payload is needed. It is just a trigger to proxy arrival
            if newProxies != []:
               self.ms.publish("DropBoxGuardianComponent:NewProxy", "")

            # Commit all the tickets
            self.ms.commit()


