from SchedulerCondorCommon import SchedulerCondorCommon
import common
import Scram
from osg_bdii import getJobManagerList
__revision__ = "$Id: SchedulerCondor_g.py,v 1.99 2008/04/21 16:12:21 ewv Exp $"
__version__ = "$Revision: 1.99 $"

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

    seDest = self.blackWhiteListParser.cleanForBlackWhiteList(task.jobs[i-1]['dlsDestination'],"list")
    scram = Scram.Scram(None)

    versionCMSSW = scram.getSWVersion()
    arch = scram.getArch()
    ceDest = getJobManagerList(seDest,versionCMSSW,arch)

    if len(ceDest) == 1:
      jobParams += "globusscheduler = "+ceDest[0]+"; "
    else:
      jobParams += "schedulerList = "+','.join(ceDest)+"; "

    common._db.updateTask_({'jobType':jobParams})
    return jobParams # Not sure I even need to return anything
