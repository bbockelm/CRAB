#!/usr/bin/env python
from distutils.core import setup

setup (name='prodcommon',
       version='1.0',
       package_dir={'CrabServerWorker': 'CrabServerWorker',
                    'Notification': 'Notification',
                    'ProxyTarballAssociator': 'ProxyTarballAssociator',
                    'TaskTracking': 'TaskTracking',
                    'DropBoxGuardian': 'DropBoxGuardian'},
       packages=['CrabServerWorker', 'Notification',
		 'ProxyTarballAssociator', 'TaskTracking', 'DropBoxGuardian'],)
