#!/usr/bin/env python
"""
_CrabServer_

Python packages for CRAB server

"""

__revision__ = "$Id: setup.py,v 1.7 2007/12/18 19:08:22 mcinquil Exp $"

from distutils.core import setup

packages = [
    'CrabServerWorker',
    'Notification',
    'TaskTracking',
    'CommandManager',
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
