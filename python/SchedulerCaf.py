from Scheduler import Scheduler
from SchedulerLsf import SchedulerLsf
from crab_exceptions import *
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
        self.queue = cfg_params.get(self.name().upper()+'.queue','cmscaf1nw')
        self.res = cfg_params.get(self.name().upper()+'.resource','"type==SLC5_64 || type==SLC4_64"')
        self.pool = cfg_params.get('USER.storage_pool','cmscafuser')

#    def wsSetupEnvironment(self):
#        """
#        Returns part of a job script which does scheduler-specific work.
#        """
#        txt = SchedulerLsf.wsSetupEnvironment(self)
#        txt += '# CAF specific stuff\n'
#        #txt += 'export STAGE_SVCCLASS=cmscaf \n'
#        txt += '\n'
#        return txt

    def wsCopyOutput(self):
        ### default is the name of the storage pool 
        ### where users can copy job outputs  
        txt=self.wsCopyOutput_comm(self.pool)
        return txt
