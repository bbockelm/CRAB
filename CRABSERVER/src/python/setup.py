#!/usr/bin/env python
"""
_CrabServer_

Python packages for CRAB server

"""

__revision__ = "$Id: setup.py,v 1.12 2009/10/05 16:54:34 spiga Exp $"

from distutils.core import setup

packages = [
    'CrabJobCreator',
    'CrabJobCreator.Database',
    'CrabJobCreator.Database.MySQL', 
    'TaskRegister',
    'CrabServerWorker',
    'Notification',
    'TaskTracking',
    'CommandManager',
    'CrabServer',
    'TaskLifeManager',
    'Plugins',
    'Plugins.ErrorHandler',
    'Plugins.HTTPFrontend'
    ]

setup(name='CrabServer',
      version='1.0',
      description='CRAB server',
      author='',
      author_email='',
      url='',
      packages=packages,
     )
