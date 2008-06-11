from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
from crab_logger import Logger
import common

import os,string

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#
# Author: Hartmut Stadie <stadie@mail.desy.de> Inst. f. Experimentalphysik; Universitaet Hamburg
#

class SchedulerSge(SchedulerLocal) :

    def __init__(self):
        Scheduler.__init__(self,"SGE")

        return

    def configure(self, cfg_params):
        SchedulerLocal.configure(self, cfg_params)
        self.environment_unique_identifier = "https://"+common.scheduler.name()+":/${JOB_ID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")

        return

    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use 
        with real scheduler  
        """
        params = {}
        return  params

    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        index = int(common._db.nJobs()) - 1
        sched_param= ''

        for i in range(index): # Add loop DS
            sched_param= ''
            if (self.queue):
                sched_param += '-q '+self.queue +' '
                if (self.res): sched_param += ' -R '+self.res +' '
            pass

        #request 2G memory and 48 hours CPU time
        sched_param += ' -l h_vmem=2G -l h_cpu=172800 '
        return sched_param

    def loggingInfo(self, id):
        """ return logging info about job nj """
        print "Warning: SchedulerSge::loggingInfo not implemented!"
        return ""

    def wsExitFunc(self):
        """
        """
        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()

        txt += '    cp ${SGE_STDOUT_PATH} CMSSW_${NJob}.stdout \n'
        txt += '    cp ${SGE_STDERR_PATH} CMSSW_${NJob}.stderr \n'         
        txt += '    tar zcvf ${out_files}.tgz  ${filesToCheck}\n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'

        return txt

