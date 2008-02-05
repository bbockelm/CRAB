from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
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
        self.environment_unique_identifier = "https://"+common.scheduler.name()+":/${LSB_BATCH_JID}-"+ \
            string.replace(common.taskDB.dict('taskId'),"_","-")

        return

    def loggingInfo(self, id):
        """ return logging info about job nj """
        cmd = 'bjobs -l ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

        return
