__revision__ = "$Id: SchedulerCondor.py,v 0 2008/04/25 18:23:10 ewv Exp $"
__version__ = "$Revision: 0 $"

from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
from crab_logger import Logger
import common

import os

#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').

class SchedulerCondor(SchedulerLocal) :

  def __init__(self):
    Scheduler.__init__(self,"CONDOR")
    return

  def configure(self, cfg_params):
    SchedulerLocal.configure(self, cfg_params)
    self.environment_unique_identifier ='${HOSTNAME}_${CONDOR_ID}_' + common._db.queryTask('name')
    return

  def sched_parameter(self,i,task):
    """
    Return scheduler-specific parameters
    """
    index = int(common._db.nJobs()) - 1
    sched_param= ''

    for i in range(index):
      pass

    return sched_param

  def loggingInfo(self, id):
    """ return logging info about job nj """
    cmd = 'something'
    #cmd_out = runCommand(cmd)
    return ''
