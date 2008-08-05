"""
Implements the vanilla (local) Condor scheduler
"""

__revision__ = "$Id: SchedulerCondor.py,v 1.9 2008/06/17 21:54:14 ewv Exp $"
__version__ = "$Revision: 1.9 $"

from SchedulerLocal  import SchedulerLocal
from crab_exceptions import CrabException
#from crab_logger import Logger

import common
import os

# Naming convention:  Methods starting with 'ws' provide the corresponding part of the job script
# ('ws' stands for 'write script').

class SchedulerCondor(SchedulerLocal) :
    """
    Class to implement the vanilla (local) Condor scheduler
    """

    def __init__(self):
        SchedulerLocal.__init__(self,"CONDOR")
        self.datasetPath   = None
        self.selectNoInput = None
        self.environment_unique_identifier = None
        return


    def configure(self, cfg_params):
        """
        Configure the scheduler with the config settings from the user
        """

        SchedulerLocal.configure(self, cfg_params)
        self.environment_unique_identifier ='${HOSTNAME}_${CONDOR_ID}_' + common._db.queryTask('name')

        try:
            tmp =  cfg_params['CMSSW.datasetpath']
            if tmp.lower() == 'none':
                self.datasetPath = None
                self.selectNoInput = 1
            else:
                self.datasetPath = tmp
                self.selectNoInput = 0
        except KeyError:
            msg = "Error: datasetpath not defined "
            raise CrabException(msg)

        return


    def sched_parameter(self, i, task):
        """
        Return scheduler-specific parameters
        """

        index = int(common._db.nJobs()) - 1
        schedParam = ''

        for i in range(index):
            pass

        return schedParam


    def realSchedParams(self, cfg_params):
        """
        Return dictionary with specific parameters, to use with real scheduler
        """

        tmpDir = os.path.join(common.work_space.shareDir(),'.condor_temp')
        params = {'tmpDir':tmpDir}
        return params


    def listMatch(self, seList, full):
        """
        Check the compatibility of available resources
        """

        if self.selectNoInput:
            return [True]
        else:
            return SchedulerLocal.listMatch(self, seList, full)


    def decodeLogInfo(self, fileName):
        """
        Parse logging info file and return main info
        """

        import CondorGLoggingInfo
        loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
        reason = loggingInfo.decodeReason(fileName)
        return reason


    def wsExitFunc(self):
        """
        Returns the part of the job script which runs prior to exit
        """

        txt = '\n'
        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()

        txt += '    tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    cp  ${out_files}.tgz $ORIG_WD/\n'
        txt += '    cp  crab_fjr_$NJob.xml $ORIG_WD/\n'

        txt += '    exit $job_exit_code\n'
        txt += '}\n'

        return txt

    def wsInitialEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """

        txt  = '\n# Written by SchedulerCondor::wsInitialEnvironment\n'
        txt += 'echo "Beginning environment"\n'
        txt += 'printenv | sort\n'

        txt += 'middleware='+self.name()+' \n'
        txt += """
if [ $_CONDOR_SCRATCH_DIR ] && [ -d $_CONDOR_SCRATCH_DIR ]; then
    ORIG_WD=`pwd`
    echo "Change from $ORIG_WD to Condor scratch directory: $_CONDOR_SCRATCH_DIR"
    if [ -e ../default.tgz ] ;then
      echo "Found ISB in parent directory (Local Condor)"
      cp ../default.tgz $_CONDOR_SCRATCH_DIR
    fi
    cd $_CONDOR_SCRATCH_DIR
fi
"""

        return txt
