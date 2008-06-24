__revision__ = "$Id: SchedulerCondor.py,v 1.9 2008/06/17 21:54:14 ewv Exp $"
__version__ = "$Revision: 1.9 $"

from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
from crab_logger import Logger
import common

import os

#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').

class SchedulerCondor(SchedulerLocal) :

  def __init__(self):
    Scheduler.__init__(self,"CONDOR")
    return


  def configure(self, cfg_params):
    SchedulerLocal.configure(self, cfg_params)
    self.environment_unique_identifier ='${HOSTNAME}_${CONDOR_ID}_' + common._db.queryTask('name')

    try:
      tmp =  cfg_params['CMSSW.datasetpath']
      if string.lower(tmp)=='none':
        self.datasetPath = None
        self.selectNoInput = 1
      else:
        self.datasetPath = tmp
        self.selectNoInput = 0
    except KeyError:
      msg = "Error: datasetpath not defined "
      raise CrabException(msg)

    return


  def sched_parameter(self,i,task):
    """
    Return scheduler-specific parameters
    """
    index = int(common._db.nJobs()) - 1
    sched_param= ''

    for i in range(index):
      pass

    return sched_param


  def realSchedParams(self,cfg_params):
    """
    Return dictionary with specific parameters, to use
    with real scheduler
    """

    tmpDir = os.path.join(common.work_space.shareDir(),'.condor_temp')
    params = {'tmpDir':tmpDir}
    return  params


  def listMatch(self, seList, full, onlyOSG=True):
    """
    Check the compatibility of available resources
    """

    # May have problems with onlyOSG being false, probably due to lengths of lists and command line.
    # Either re-write osg_bdii.py with a proper ldap library or break the queries apart

    if self.selectNoInput:
      return [True]
    else:
      return SchedulerLocal.listMatch(self, seList, full)

  def decodeLogInfo(self, file):
    """
    Parse logging info file and return main info
    """
    import CondorGLoggingInfo
    loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
    reason = loggingInfo.decodeReason(file)
    return reason


  def wsExitFunc(self):
    """
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
