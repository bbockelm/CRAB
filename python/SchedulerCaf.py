from Scheduler import Scheduler
from SchedulerLsf import SchedulerLsf
from crab_exceptions import *
import common

import os,string

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerCaf(SchedulerLsf) :

    def __init__(self):
        SchedulerLsf.__init__(self)
        Scheduler.__init__(self,"CAF")
        self.OSBsize = 55000000

        return

    def configure(self, cfg_params):
        """
        CAF is just a special queue and resources for LSF at CERN
        """
        SchedulerLsf.configure(self, cfg_params)
        self.queue = cfg_params.get(self.name().upper()+'.queue','cmscaf1nw')
        self.res = cfg_params.get(self.name().upper()+'.resource','"type==SLC5_64 || type==SLC4_64"')
        self.group = cfg_params.get(self.name().upper()+'.group', None)
        self.pool = cfg_params.get('USER.storage_pool','cmscafuser')

    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        sched_param= ''

        if (self.queue):
            sched_param += '-q '+self.queue +' '
        if (self.res): sched_param += ' -R '+self.res +' '
        if (self.group): sched_param += ' -G '+str(self.group).upper() +' '
        return sched_param


#    def wsSetupEnvironment(self):
#        """
#        Returns part of a job script which does scheduler-specific work.
#        """
#        txt = SchedulerLsf.wsSetupEnvironment(self)
#        txt += '# CAF specific stuff\n'
#        #txt += 'export STAGE_SVCCLASS=cmscaf \n'
#        txt += '\n'
#        return txt

    def wsCopyOutput(self):
        ### default is the name of the storage pool 
        ### where users can copy job outputs  
        txt=self.wsCopyOutput_comm(self.pool)
        return txt

    def wsExitFunc(self):
        """
        """
        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'
        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()

        txt += '    cp *.${LSB_BATCH_JID}.out CMSSW_${NJob}.stdout \n'
        txt += '    cp *.${LSB_BATCH_JID}.err CMSSW_${NJob}.stderr \n'
        txt += '    tar zcvf ${out_files}.tgz  ${filesToCheck}\n'
        txt += '    tmp_size=`ls -gGrta ${out_files}.tgz | awk \'{ print $3 }\'`\n'
        txt += '    rm ${out_files}.tgz\n'
        txt += '    size=`expr $tmp_size`\n'
        txt += '    echo "Total Output dimension: $size"\n'
        txt += '    limit='+str(self.OSBsize) +' \n'
        txt += '    echo "WARNING: output files size limit is set to: $limit"\n'
        txt += '    if [ "$limit" -lt "$size" ]; then\n'
        txt += '        exceed=1\n'
        txt += '        job_exit_code=70000\n'
        txt += '        echo "Output Sanbox too big. Produced output is lost "\n'
        txt += '    else\n'
        txt += '        exceed=0\n'
        txt += '        echo "Total Output dimension $size is fine."\n'
        txt += '    fi\n'

        txt += '    echo "JOB_EXIT_STATUS = $job_exit_code"\n'
        txt += '    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    if [ $exceed -ne 1 ]; then\n'
        txt += '        tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    else\n'
        txt += '        tar zcvf ${out_files}.tgz CMSSW_${NJob}.stdout CMSSW_${NJob}.stderr\n'
        txt += '    fi\n'
        txt += '    python $RUNTIME_AREA/fillCrabFjr.py $RUNTIME_AREA/crab_fjr_$NJob.xml --errorcode $job_exit_code \n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'
        return txt
