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
from ProxyTarballAssociator.ProxyTarballAssociatorComponent import ProxyTarballAssociatorComponent


#  //
# // Find and load the Configuration
#//

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("ProxyTarballAssociator")
    compCfg.update( config.getConfig("CrabServerConfigurations") )
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
#  //
# // Initialise and start the component
#//

createDaemon(compCfg['ComponentDir'])
component = ProxyTarballAssociatorComponent(**dict(compCfg))
component.startComponent()
