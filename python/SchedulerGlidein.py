from SchedulerCondorCommon import SchedulerCondorCommon
import common

__revision__ = "$Id: SchedulerGlidein.py,v 1.8 2008/04/24 18:51:12 ewv Exp $"
__version__ = "$Revision: 1.8 $"

class SchedulerGlidein(SchedulerCondorCommon):
  def __init__(self):
    SchedulerCondorCommon.__init__(self,"GLIDEIN")
    return

  def sched_parameter(self,i,task):
    """
    Return scheduler-specific parameters
    """
    jobParams = SchedulerCondorCommon.sched_parameter(self,i,task)

    ceDest = self.seListToCElist(task.jobs[i-1]['dlsDestination'], onlyOSG=False)
    ceString = ','.join(ceDest)

    jobParams += '+DESIRED_Gatekeepers = "'+ceString+'"; '
    jobParams += '+DESIRED_Archs = "INTEL,X86_64"; '
    jobParams += "Requirements = stringListMember(GLIDEIN_Gatekeeper,DESIRED_Gatekeepers) &&  stringListMember(Arch,DESIRED_Archs); "

    common._db.updateTask_({'jobType':jobParams})
    return jobParams # Not sure I even need to return anything
