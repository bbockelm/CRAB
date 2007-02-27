#!/usr/bin/env python
"""
_CrabServer_

Python packages for CRAB server

"""

__revision__ = "$Id$"

from distutils.core import setup

packages = [
    'CrabServerWorker',
    'DropBoxGuardian',
    'Notification',
    'ProxyTarballAssociator',
    'TaskTracking',
    ]

setup(name='CrabServer',
      version='1.0',
      description='CRAB server',
      author='',
      author_email='',
      url='',
      packages=packages,
     )
