#!/usr/bin/env python
"""
_CrabServer_

Python packages for CRAB server

"""

__revision__ = "$Id: setup.py,v 1.4 2007/06/20 11:06:30 mcinquil Exp $"

from distutils.core import setup

packages = [
    'CrabServerWorker',
    'DropBoxGuardian',
    'Notification',
    'ProxyTarballAssociator',
    'TaskTracking',
    'CommandManager',
    'CW_WatchDog',
    ]

setup(name='CrabServer',
      version='1.0',
      description='CRAB server',
      author='',
      author_email='',
      url='',
      packages=packages,
     )
