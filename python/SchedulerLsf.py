from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
from crab_logger import Logger
import common
from LFNBaseName import *

import os,string

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerLsf(SchedulerLocal) :

    def __init__(self):
        Scheduler.__init__(self,"LSF")
        
        return

    def configure(self, cfg_params):
        SchedulerLocal.configure(self, cfg_params)
        self.outputDir = cfg_params.get('USER.outputdir' ,common.work_space.resDir())
        self.environment_unique_identifier = "https://"+common.scheduler.name()+":/${LSB_BATCH_JID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")

        return

    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use 
        with real scheduler  
        """
        ### use by the BossLite script 
        self.cpCmd  =  cfg_params.get(self.name().upper()+'.cp_command','cp')
        self.rfioName =  cfg_params.get(self.name().upper()+'.rfio_server','')

        params = { 'cpCmd'  : self.cpCmd, \
                   'rfioName' : self.rfioName
                 }
        return  params

    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        sched_param= ''

        if (self.queue):
            sched_param += '-q '+self.queue +' '
            if (self.res): sched_param += ' -R '+self.res +' '

        sched_param+='-cwd '+ str(self.outputDir)  + ' '
        return sched_param
   
    def listMatch(self, dest, full):
        """
        """ 
        if len(dest)!=0: 
            sites = [self.blackWhiteListParser.cleanForBlackWhiteList(dest,'list')]
        else:     
            sites = [str(getLocalDomain(self))]  
        return sites    

    def loggingInfo(self, id):
        """ return logging info about job nj """
        cmd = 'bjobs -l ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

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
        txt += '    exit $job_exit_code\n'
        txt += '}\n'

        return txt

    def wsCopyOutput_tmp(self,pool=None):
        #########################################################################
        ### Temporary we added this function to be used by
        ### SchedulerLsf and SchedulerCaf to have a copy function based on cmscp
        ### This function overwrite the wsCopyOutput written in SchedulerLocal 
        ### and used by CondorLocal
        #########################################################################
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        txt = '\n'
        if not self.copy_data: return txt

        if int(self.publish_data) == 1:
                ### FEDE FOR NEW LFN ####
                #self.path_add = PFNportion(self.publish_data_name,LocalUser=True) +'_${PSETHASH}/'
                self.path_add = PFNportion(self.primaryDataset, self.publish_data_name,LocalUser=True) +'/${PSETHASH}/'
                #########################
                self.SE_path = self.SE_path + self.path_add
      
        txt += '#\n'
        txt += '# COPY OUTPUT FILE TO '+self.SE_path+ '\n'
        txt += '#\n\n'

        if (pool):
            txt += 'export STAGE_SVCCLASS='+str(pool)+'\n'
            
        if int(self.publish_data) == 1:
            txt += 'export SE='+self.SE+'\n'
            
        txt += 'export SE_PATH='+self.SE_path+'\n'

        txt += 'export TIME_STAGEOUT_INI=`date +%s` \n'
        txt += '# Verify is the SE path exists '+self.SE_path+'\n'
        txt += '#\n\n'
        txt += 'verifySePath ' + self.SE_path + '\n'
        txt += 'if [ $exit_verifySePath -ne 0 ]; then\n'
        txt += '    echo ">>> Copy output files from WN = `hostname` to SE_PATH = $SE_PATH :"\n'
        txt += '    copy_exit_status=0\n'
        txt += '    for out_file in $file_list ; do\n'
        txt += '        if [ -e $SOFTWARE_DIR/$out_file ] ; then\n'
        txt += '            echo "Trying to copy output file to $SE_PATH"\n'
        txt += '            cmscp $middleware $SOFTWARE_DIR/$out_file $out_file ${SE_PATH}\n'
        txt += '            if [ $cmscp_exit_status -ne 0 ]; then\n'
        txt += '                echo "Problem copying $out_file to $SE_PATH"\n'
        txt += '                copy_exit_status=$cmscp_exit_status\n'
        txt += '            else\n'
        txt += '                echo "output copied into $SE/$SE_PATH directory"\n'
        txt += '            fi\n'
        txt += '        else\n'
        txt += '            copy_exit_status=60302\n'
        txt += '            echo "StageOutExitStatus = $copy_exit_status" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '            echo "StageOutExitStatusReason = file to copy not found" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '        fi\n'
        txt += '    done\n'
        txt += '    if [ $copy_exit_status -ne 0 ]; then\n'
        txt += '        SE=""\n'
        txt += '        SE_PATH=""\n'
        txt += '        job_exit_code=$copy_exit_status\n'
        txt += '    fi\n'
        txt += 'fi\n'
        txt += 'export TIME_STAGEOUT_END=`date +%s` \n'
        txt += 'let "TIME_STAGEOUT = TIME_STAGEOUT_END - TIME_STAGEOUT_INI" \n'

        return txt

    def wsCopyOutput(self):
        pool=None
        txt=self.wsCopyOutput_tmp(pool)
        return txt
