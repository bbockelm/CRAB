#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""
__revision__ = "$Id$"
__version__ = "$Revision$"

import os

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from TaskLifeManager.TaskLifeManagerComponent import TaskLifeManagerComponent

# Find and load the Configuration

try:
    config = loadProdAgentConfiguration()
    
    # Basic task tracking configuration
    compCfg = config.getConfig("TaskLifeManager")
    compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
    compCfg.update( config.getConfig("CrabServerConfigurations") )
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


# Initialize and start the component

createDaemon(compCfg['ComponentDir'])
component = TaskLifeManagerComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])

