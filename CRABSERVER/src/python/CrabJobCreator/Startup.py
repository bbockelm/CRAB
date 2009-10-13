#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from CrabJobCreator.CrabJobCreatorComponent import CrabJobCreatorComponent

# Find and load the Configuration
try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("CrabJobCreator")
    compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
    compCfg.update( config.getConfig("CrabServerConfigurations") )
    compCfg.update( config.getConfig("JobStates"))
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

# Initialise and start the component
createDaemon(compCfg['ComponentDir'])
component = CrabJobCreatorComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])

