#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from Notification.NotificationComponent import NotificationComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("Notification")
    #compCfg.update( config.getConfig("NotificationConfigurations") )
    
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    print "FATAL: %s" % msg
    exit
    #raise RuntimeError, msg


compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
#  //
# // Initialise and start the component
#//
print "Notification.Startup: creating Daemon..."
createDaemon(compCfg['ComponentDir'])
print "Notification.Startup: Daemon created..."
print "Notification.Startup: Creating NotificationComponent object..."
component = NotificationComponent(**dict(compCfg))
print "Notification.Startup: Object created"
print "Notification.Startup: Calling NotificationComponent.startComponent()"
component.startComponent()
