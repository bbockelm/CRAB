from SchedulerCondorCommon import SchedulerCondorCommon
import common

__revision__ = "$Id: SchedulerCondor_g.py,v 1.96 2008/04/16 19:42:59 ewv Exp $"
__version__ = "$Revision: 1.96 $"

# All of the content moved to SchedulerCondorCommon.

class SchedulerGlidein(SchedulerCondorCommon):
  def __init__(self):
    SchedulerCondorCommon.__init__(self,"GLIDEIN")
    return

  def sched_parameter(self,i,task):
    """
    Return scheduler-specific parameters
    """
    jobParams = SchedulerCondorCommon.sched_parameter(self,i,task)

    seDest = self.blackWhiteListParser.cleanForBlackWhiteList(eval(task.jobs[i-1]['dlsDestination']))
    #ceDest = self.getCEfromSE(seDest)

    # FIXME: Translate seDest into glidein name using SiteDB

    jobParams += '+DESIRED_Sites = "UCSDT2"; '
    jobParams += '+DESIRED_Archs = "INTEL,X86_64"; '
    jobParams += "Requirements = stringListMember(GLIDEIN_Site,DESIRED_Sites) &&  stringListMember(Arch,DESIRED_Archs); "

    common._db.updateTask_({'jobType':jobParams})
    return jobParams # Not sure I even need to return anything
