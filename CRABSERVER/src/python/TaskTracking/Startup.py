#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""
__revision__ = "$Id: Startup.py,v 1.3 2007/04/13 10:25:53 mcinquil Exp $"
__version__ = "$Revision: 1.3 $"

import os


from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from TaskTracking.TaskTrackingComponent import TaskTrackingComponent

# Find and load the Configuration

try:
    config = loadProdAgentConfiguration()
    
    # Basic task tracking configuration
    compCfg = config.getConfig("TaskTracking")
    compCfg['ComponentDir'] = os.path.expandvars(compCfg['ComponentDir'])
    compCfg.update( config.getConfig("CrabServerConfigurations") )
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg


# Initialize and start the component

createDaemon(compCfg['ComponentDir'])
component = TaskTrackingComponent(**dict(compCfg))
runWithPostMortem(component, compCfg['ComponentDir'])


                  

