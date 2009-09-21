#!/usr/bin/env python
#pylint: disable-msg=E1101,E1103,C0103,R0902
"""
Defines default config values for CrabJobCreator specific
parameters.
"""
__all__ = []
__revision__ = "$Id: DefaultConfig.py,v 0 2009/09/22 00:46:10 riahi Exp $"
__version__ = "$Revision: 0 $"

import os
from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("CrabJobCreator")
config.CrabJobCreator.logLevel = "DEBUG"
config.CrabJobCreator.componentName = "CrabJobCreator"
config.CrabJobCreator.componentDir = \
    os.path.join(os.getenv("TESTDIR"), "CrabJobCreator")

# The maximum number of threads to process each message type
config.CrabJobCreator.maxThreads = 10

# CrabServer parameters
config.CrabJobCreator.wdir = '/data/Storage/logs' 
config.CrabJobCreator.maxRetries = 3

# The poll interval at which to look for new filesets
config.CrabJobCreator.pollInterval = 60
