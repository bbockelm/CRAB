from SchedulerCondorCommon import SchedulerCondorCommon
import common
import Scram
from osg_bdii import getJobManagerList
__revision__ = "$Id: SchedulerCondor_g.py,v 1.97.2.2 2008/04/18 22:01:48 ewv Exp $"
__version__ = "$Revision: 1.97.2.2 $"

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

    print "Raw SE: ",eval(task.jobs[i-1]['dlsDestination'])

    seDest = self.blackWhiteListParser.cleanForBlackWhiteList(eval(task.jobs[i-1]['dlsDestination']),"list")
    print "Cleaned SE: ",seDest
    scram = Scram.Scram(None)

    versionCMSSW = scram.getSWVersion()
    arch = scram.getArch()
    ceDest = getJobManagerList(seDest,versionCMSSW,arch)
    if not ceDest[0]:
      ceDest = ceDest[1:]
    print "CE's",ceDest
#    ceDest = self.getCEfromSE(seDest)

    if len(ceDest) == 1:
      jobParams += "globusscheduler = "+ceDest[0]+"; "
    else:
      jobParams += "schedulerList = "+','.join(ceDest)+"; "

    common._db.updateTask_({'jobType':jobParams})
    return jobParams # Not sure I even need to return anything
