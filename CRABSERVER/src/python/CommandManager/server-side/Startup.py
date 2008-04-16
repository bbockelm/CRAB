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
from CommandManager.CmdMgrComponent import CmdMgrComponent

# Find and load the Configuration

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("CommandManager")
    compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
    compCfg.update( config.getConfig("CrabServerConfigurations") )
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

# Initialise and start the component
createDaemon(compCfg['ComponentDir'])
component = CmdMgrComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])

