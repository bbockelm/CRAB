from SchedulerCondorCommon import SchedulerCondorCommon
import common

__revision__ = "$Id: SchedulerCondor_g.py,v 1.96 2008/04/16 19:42:59 ewv Exp $"
__version__ = "$Revision: 1.96 $"

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

    seDest = self.blackWhiteListParser.cleanForBlackWhiteList(eval(task.jobs[i-1]['dlsDestination']))
    ceDest = self.getCEfromSE(seDest)

    jobParams += "globusscheduler = "+ceDest+":2119/jobmanager-condor; "

    common._db.updateTask_({'jobType':jobParams})
    return jobParams # Not sure I even need to return anything
