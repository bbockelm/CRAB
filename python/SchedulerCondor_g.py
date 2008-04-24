from SchedulerCondorCommon import SchedulerCondorCommon
from crab_exceptions import *
import common
__revision__ = "$Id: SchedulerCondor_g.py,v 1.101 2008/04/23 18:20:01 ewv Exp $"
__version__ = "$Revision: 1.101 $"

# All of the content moved to SchedulerCondorCommon.

class SchedulerCondor_g(SchedulerCondorCommon):
  def __init__(self):
    SchedulerCondorCommon.__init__(self,"CONDOR_G")
    return

  def sched_parameter(self,i,task):
    """
    Return scheduler-specific parameters
    """
    jobParams = SchedulerCondorCommon.sched_parameter(self,i,task)

    ceDest = self.seListToCElist(task.jobs[i-1]['dlsDestination'])

    if len(ceDest) == 1:
      jobParams += "globusscheduler = "+ceDest[0]+"; "
    else:
      jobParams += "schedulerList = "+','.join(ceDest)+"; "

    common._db.updateTask_({'jobType':jobParams})
    return jobParams # Not sure I even need to return anything
