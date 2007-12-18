#!/usr/bin/env python
"""
_CrabServer_

Python packages for CRAB server

"""

__revision__ = "$Id: setup.py,v 1.6 2007/07/02 20:10:52 spiga Exp $"

from distutils.core import setup

packages = [
    'CrabServerWorker',
    'DropBoxGuardian',
    'Notification',
    'ProxyTarballAssociator',
    'TaskTracking',
    'CommandManager',
    'CW_WatchDog',
    'CrabServer',
    'TaskLifeManager'
    ]

setup(name='CrabServer',
      version='1.0',
      description='CRAB server',
      author='',
      author_email='',
      url='',
      packages=packages,
     )
