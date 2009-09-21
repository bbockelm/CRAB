#!/usr/bin/env
#pylint: disable-msg=W0613
"""
The CrabJobCreator itself, set up event listeners and work event thread
"""
__all__ = []
__revision__ = "$Id: CrabJobCreator.py,v 0 2009/09/22 00:46:10 riahi Exp $"
__version__ = "$Revision: 0 $"

import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from CrabJobCreatorPoller import CrabJobCreatorPoller

class CrabJobCreator(Harness):
    """
    _CrabJobCreator receives and extends partially task in bosslite. Poll wmbs db to
    get new subscription_ 
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
    
    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['AddTaskToRegister'] = \
            factory.loadObject(\
                "CrabJobCreator.Handler.AddTaskToRegister", self)
        
        # Add event loop to worker manager
        myThread = threading.currentThread()
        pollInterval = self.config.CrabJobCreator.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(\
                       CrabJobCreatorPoller(self.config), pollInterval) 


