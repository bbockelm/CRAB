from Scheduler import Scheduler
from SchedulerLsf import SchedulerLsf
from crab_exceptions import *
from crab_logger import Logger
import common

import os,string

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerCaf(SchedulerLsf) :

    def __init__(self):
        SchedulerLsf.__init__(self)
        Scheduler.__init__(self,"CAF")

        return

    def configure(self, cfg_params):
        """
        CAF is just a special queue and resources for LSF at CERN
        """
        SchedulerLsf.configure(self, cfg_params)
        self.queue = cfg_params.get(self.name().upper()+'.queue','dedicated')
        self.res = cfg_params.get(self.name().upper()+'.resource','cmscaf')
    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        txt = SchedulerLsf.wsSetupEnvironment(self)
        txt += '# CAF specific stuff\n'
        txt += 'middleware=CAF \n'
        txt += 'export STAGE_SVCCLASS=cmscaf \n'

        return txt

