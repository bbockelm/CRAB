#!/usr/bin/env python
"""
_CrabServer_

Python packages for CRAB server

"""

__revision__ = "$Id: setup.py,v 1.3 2007/02/27 10:40:03 spiga Exp $"

from distutils.core import setup

packages = [
    'CrabServerWorker',
    'DropBoxGuardian',
    'Notification',
    'ProxyTarballAssociator',
    'TaskTracking',
    'CommandManager',
    ]

setup(name='CrabServer',
      version='1.0',
      description='CRAB server',
      author='',
      author_email='',
      url='',
      packages=packages,
     )
