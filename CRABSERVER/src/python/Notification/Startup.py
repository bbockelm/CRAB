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
from ProdAgentCore.PostMortem import runWithPostMortem
from Notification.NotificationComponent import NotificationComponent

#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("Notification")
    #compCfg.update( config.getConfig("NotificationConfigurations") )
    compCfg.update( config.getConfig("ProdAgent") )
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
createDaemon(compCfg['ComponentDir'])
component = NotificationComponent(**dict(compCfg))
#component.startComponent()
runWithPostMortem(component, compCfg['ComponentDir'])

