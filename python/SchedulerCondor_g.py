"""
CondorG specific parts of the scheduler
Non-specific code comes from SchedulerCondorCommon
"""

from SchedulerCondorCommon import SchedulerCondorCommon

__revision__ = "$Id: SchedulerCondor_g.py,v 1.107 2009/03/09 15:56:26 ewv Exp $"
__version__ = "$Revision: 1.107 $"

# All of the content moved to SchedulerCondorCommon.

class SchedulerCondor_g(SchedulerCondorCommon):
    """
    CondorG specific parts of the CondorG scheduler
    """


    def __init__(self):
        SchedulerCondorCommon.__init__(self,"CONDOR_G")
        return


    def sched_parameter(self, i, task):
        """
        Return scheduler-specific parameters
        """
        jobParams = SchedulerCondorCommon.sched_parameter(self, i, task)

        ceDest = self.seListToCElist(task.jobs[i-1]['dlsDestination'])

        if len(ceDest) == 1:
            jobParams += "grid_resource = gt2 "+ceDest[0]+"; "
        else:
            jobParams += "schedulerList = "+','.join(ceDest)+"; "

        globusRSL = self.GLOBUS_RSL
        if (self.EDG_clock_time):
            globusRSL += '(maxWalltime='+self.EDG_clock_time+')'
        else:
            globusRSL += '(maxWalltime=120)'

        if (globusRSL != ''):
            jobParams +=  'globusrsl = ' + globusRSL + '; '

        return jobParams
