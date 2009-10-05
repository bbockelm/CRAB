#!/usr/bin/env python
"""
_CrabServer_

Python packages for CRAB server

"""

__revision__ = "$Id: setup.py,v 1.11 2008/10/08 17:49:39 spiga Exp $"

from distutils.core import setup

packages = [
    'CrabJobCreator',
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
