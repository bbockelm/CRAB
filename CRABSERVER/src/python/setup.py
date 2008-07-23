#!/usr/bin/env python
"""
_CrabServer_

Python packages for CRAB server

"""

__revision__ = "$Id: setup.py,v 1.8 2008/03/27 11:44:34 farinafa Exp $"

from distutils.core import setup

packages = [
    'TaskRegister',
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
