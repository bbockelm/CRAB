"""
Implements the vanilla (local) Condor scheduler
"""

__revision__ = "$Id: SchedulerCondor.py,v 1.17 2009/02/09 21:16:34 ewv Exp $"
__version__ = "$Revision: 1.17 $"

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
        self.environment_unique_identifier = None
        return


    def configure(self, cfg_params):
        """
        Configure the scheduler with the config settings from the user
        """

        SchedulerLocal.configure(self, cfg_params)
        taskHash = sha.new(common._db.queryTask('name')).hexdigest()
        self.environment_unique_identifier = "https://" + socket.gethostname() + \
                                              '/' + taskHash + "/${NJob}"

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
        self.backup_copy = cfg_params.get('USER.backup_copy',0)

        if ( int(self.return_data) == 0 and int(self.copy_data) == 0 ):
            msg = 'Error: return_data and copy_data cannot be set both to 0\n'
            msg = msg + 'Please modify your crab.cfg file\n'
            raise CrabException(msg)

        if ( int(self.return_data) == 1 and int(self.copy_data) == 1 ):
            msg = 'Error: return_data and copy_data cannot be set both to 1\n'
            msg = msg + 'Please modify your crab.cfg file\n'
            raise CrabException(msg)

        if ( int(self.copy_data) == 0 and int(self.publish_data) == 1 ):
            msg = 'Warning: publish_data = 1 must be used with copy_data = 1\n'
            msg = msg + 'Please modify copy_data value in your crab.cfg file\n'
            common.logger.message(msg)
            raise CrabException(msg)

        if ( int(self.copy_data) == 0 and int(self.backup_copy) == 1 ):
            msg = 'Error: copy_data = 0 and backup_data = 1 ==> to use the backup_copy function, the copy_data value has to be = 1\n'
            msg = msg + 'Please modify copy_data value in your crab.cfg file\n'
            raise CrabException(msg)

        if int(self.copy_data) == 1:
            self.SE = cfg_params.get('USER.storage_element', None)
            if not self.SE:
                msg = "Error. The [USER] section has no 'storage_element'"
                common.logger.message(msg)
                raise CrabException(msg)
                
        if ( int(self.backup_copy) == 1 and int(self.publish_data) == 1 ):
            msg = 'Warning: currently the publication is not supported with the backup copy. Work in progress....\n'
            common.logger.message(msg)
            raise CrabException(msg)

            self.proxyValid = 0
            self.dontCheckProxy = int(cfg_params.get("EDG.dont_check_proxy",0))
            self.proxyServer = cfg_params.get("EDG.proxy_server",'myproxy.cern.ch')
            common.logger.debug(5,'Setting myproxy server to ' + self.proxyServer)

            self.group = cfg_params.get("EDG.group", None)
            self.role  = cfg_params.get("EDG.role", None)
            self.VO    = cfg_params.get('EDG.virtual_organization', 'cms')

            self.checkProxy()
        self.role  = None

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
