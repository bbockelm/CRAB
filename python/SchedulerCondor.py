"""
Implements the vanilla (local) Condor scheduler
"""

__revision__ = "$Id: SchedulerCondor.py,v 1.25 2009/05/26 10:23:01 spiga Exp $"
__version__ = "$Revision: 1.25 $"

from SchedulerLocal  import SchedulerLocal
from crab_exceptions import CrabException

import common
import os
import socket
import sha

class SchedulerCondor(SchedulerLocal) :
    """
    Class to implement the vanilla (local) Condor scheduler
     Naming convention:  Methods starting with 'ws' provide
     the corresponding part of the job script
     ('ws' stands for 'write script').
    """

    def __init__(self):
        SchedulerLocal.__init__(self,"CONDOR")
        self.datasetPath   = None
        self.selectNoInput = None
        self.return_data   = 0
        self.copy_data     = 0

        self.environment_unique_identifier = None
        return


    def configure(self, cfg_params):
        """
        Configure the scheduler with the config settings from the user
        """

        SchedulerLocal.configure(self, cfg_params)

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

        self.return_data = cfg_params.get('USER.return_data', 0)
        self.copy_data   = cfg_params.get("USER.copy_data", 0)

        self.proxyValid = 0
        self.dontCheckProxy = int(cfg_params.get("GRID.dont_check_proxy", 0))
        self.proxyServer = cfg_params.get("GRID.proxy_server", 'myproxy.cern.ch')
        common.logger.debug('Setting myproxy server to ' + self.proxyServer)

        self.group = cfg_params.get("GRID.group", None)
        self.role  = cfg_params.get("GRID.role", None)
        self.VO    = cfg_params.get('GRID.virtual_organization', 'cms')

        self.checkProxy()

        return

    def envUniqueID(self):
        taskHash = sha.new(common._db.queryTask('name')).hexdigest()
        id = "https://" + socket.gethostname() + '/' + taskHash + "/${NJob}"
        return id

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
        tmpDir = os.path.join(common.work_space.shareDir(),'.condor_temp')
        jobDir = common.work_space.jobDir()
        params = {'tmpDir':tmpDir,
                  'jobDir':jobDir}
        return params


    def listMatch(self, seList, full):
        """
        Check the compatibility of available resources
        """

        return [True]


    def decodeLogInfo(self, fileName):
        """
        Parse logging info file and return main info
        """

        import CondorGLoggingInfo
        loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
        reason = loggingInfo.decodeReason(fileName)
        return reason


    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        txt = self.wsCopyOutput_comm()
        return txt


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
        txt += '    cp  ${out_files}.tgz $_CONDOR_SCRATCH_DIR/\n'
        txt += '    cp  crab_fjr_$NJob.xml $_CONDOR_SCRATCH_DIR/\n'

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
        txt += 'if [ -e /opt/d-cache/srm/bin ]; then\n'
        txt += '  export PATH=${PATH}:/opt/d-cache/srm/bin\n'
        txt += 'fi\n'

        txt += """
if [ $_CONDOR_SCRATCH_DIR ] && [ -d $_CONDOR_SCRATCH_DIR ]; then
    echo "cd to Condor scratch directory: $_CONDOR_SCRATCH_DIR"
    if [ -e ../default.tgz ] ;then
      echo "Found ISB in parent directory (Local Condor)"
      cp ../default.tgz $_CONDOR_SCRATCH_DIR
    fi
    cd $_CONDOR_SCRATCH_DIR
fi
"""

        return txt
