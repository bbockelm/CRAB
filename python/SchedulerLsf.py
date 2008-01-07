from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_logger import Logger
import common

import os,string

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerLsf(SchedulerLocal) :

    def __init__(self):
        Scheduler.__init__(self,"LSF")

        return

    def configure(self, cfg_params):
        SchedulerLocal.configure(self, cfg_params)
        self.environment_unique_identifier = 'LSB_BATCH_JID'

        return
