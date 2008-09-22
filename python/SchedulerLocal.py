from Scheduler import Scheduler
from crab_exceptions import *
from crab_logger import Logger
from crab_util import getLocalDomain 
import common
from PhEDExDatasvcInfo import PhEDExDatasvcInfo

import os,string

# Base class for all local scheduler

class SchedulerLocal(Scheduler) :

    def configure(self, cfg_params):

        self.cfg_params = cfg_params
        self.jobtypeName = cfg_params['CRAB.jobtype']

        name=string.upper(self.name())
        self.queue = cfg_params.get(name+'.queue',None)

        self.res = cfg_params.get(name+'.resource',None)

        if (cfg_params.has_key(self.name()+'.env_id')): self.environment_unique_identifier = cfg_params[self.name()+'.env_id']

        self._taskId=str("_".join(common._db.queryTask('name').split('_')[:-1]))

        self.return_data = int(cfg_params.get('USER.return_data',0))
        self.copy_data = int(cfg_params.get('USER.copy_data',0))


        if ( self.return_data == 0 and self.copy_data == 0 ):
           msg = 'Error: return_data = 0 and copy_data = 0 ==> your exe output will be lost\n'
           msg = msg + 'Please modify return_data and copy_data value in your crab.cfg file\n'
           raise CrabException(msg)

        if ( self.return_data == 1 and self.copy_data == 1 ):
           msg = 'Error: return_data and copy_data cannot be set both to 1\n'
           msg = msg + 'Please modify return_data or copy_data value in your crab.cfg file\n'
           raise CrabException(msg)

        self.publish_data = cfg_params.get("USER.publish_data",0)
        if int(self.publish_data) == 1 and common.scheduler.name().upper() not in ['CAF']:
            msg = "Error. User data publication not allowed while running on %s"%common.scheduler.name().upper()
            raise CrabException(msg)

        ## is this ok?
        localDomainName = getLocalDomain(self)
        if not cfg_params.has_key('EDG.se_white_list'):
            cfg_params['EDG.se_white_list']=localDomainName
            common.logger.message("Your domain name is "+str(localDomainName)+": only local dataset will be considered")
        else:
            common.logger.message("Your se_white_list is set to "+str(cfg_params['EDG.se_white_list'])+": only local dataset will be considered")

        Scheduler.configure(self,cfg_params)
        return

    def userName(self):
        """ return the user name """
        import pwd,getpass
        tmp=pwd.getpwnam(getpass.getuser())[4]
        return "/CN="+tmp.strip()

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        if not self.environment_unique_identifier:
            raise CrabException('environment_unique_identifier not set')

        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        # start with wrapper timing
        txt  = 'export TIME_WRAP_INI=`date +%s` \n'
        txt += 'export TIME_STAGEOUT=-2 \n\n'

        txt += '# '+self.name()+' specific stuff\n'
        txt += '# strip arguments\n'
        txt += 'echo "strip arguments"\n'
        txt += 'args=("$@")\n'
        txt += 'nargs=$#\n'
        txt += 'shift $nargs\n'
        txt += "# job number (first parameter for job wrapper)\n"
        txt += "NJob=${args[0]}; export NJob\n"

        txt += "out_files=out_files_${NJob}; export out_files\n"
        txt += "echo $out_files\n"
        txt += jbt.outList()

        txt += 'SyncGridJobId=`echo '+self.environment_unique_identifier+'`\n'
        txt += 'MonitorJobID=`echo ${NJob}_${SyncGridJobId}`\n'
        txt += 'MonitorID=`echo ' + self._taskId + '`\n'

        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=`echo $SyncGridJobId`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += 'echo "SyncCE='+self.name()+'.`hostname -d`" | tee -a $RUNTIME_AREA/$repo \n'

        txt += 'middleware='+self.name().upper()+' \n'

        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'

        txt += 'InputSandBox=${args[3]}\n'

        txt += '\n\n'

        return txt

    def wsCopyOutput_comm(self, pool=None):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        txt = '\n'
        if int(self.copy_data) == 1:

            stageout = PhEDExDatasvcInfo(self.cfg_params)
            endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()

            txt += '#\n'
            txt += '# COPY OUTPUT FILE TO '+SE_PATH+ '\n'
            txt += '#\n\n'

            txt += 'export SE'+SE+'\n'
            txt += 'echo "SE = $SE"\n'
            txt += 'export SE_PATH='+SE_PATH+'\n'
            txt += 'echo "SE_PATH = $SE_PATH"\n'
            txt += 'export LFNBaseName='+lfn+'\n'
            txt += 'echo LFNBaseName = $LFNBaseName\n'
            txt += 'export USER='+user+'\n'
            txt += 'echo "USER = $USER\n'
            txt += 'export endpoint='+endpoint+'\n'
            txt += 'echo "endpoint = $endpoint\n'
            ### Needed i.e. for caf  
            if (pool):
                txt += 'export STAGE_SVCCLASS='+str(pool)+'\n'
                
            txt += 'echo ">>> Copy output files from WN = `hostname` to $SE_PATH :"\n'
            txt += 'export TIME_STAGEOUT_INI=`date +%s` \n'
            txt += 'copy_exit_status=0\n'
            txt += 'python cmscp.py --dest $endpoint --inputFileList $file_list --middleware $middleware '+self.debugWrap+'\n'
            txt += 'source cmscpReport.sh'
            if self.debug_wrapper: 
                txt += '########### details of SE interaction\n'
                txt += 'cat .SEinteraction.log\n'   
                txt += '########### \n'
            txt += 'if [ $StageOutExitStatus -ne 0 ]; then\n'
            txt += '    echo "Problem copying file to $SE $SE_PATH"\n'
            txt += '    copy_exit_status=$StageOutExitStatus \n'
         #  txt += '    SE=""\n'
         #  txt += '    SE_PATH=""\n'
            txt += '    job_exit_code=$StageOutExitStatus\n'
            txt += 'fi\n'
            txt += 'export TIME_STAGEOUT_END=`date +%s` \n'
            txt += 'let "TIME_STAGEOUT = TIME_STAGEOUT_END - TIME_STAGEOUT_INI" \n'
        else:
            txt += 'export TIME_STAGEOUT=-1 \n'
        return txt

    def wsExitFunc_comm(self):
        """
        """
        txt = ''
        txt += '    if [ $PYTHONPATH ]; then \n'
        txt += '       if [ ! -s $RUNTIME_AREA/fillCrabFjr.py ]; then \n'
        txt += '           echo "WARNING: it is not possible to create crab_fjr.xml to final report" \n'
        txt += '       else \n'
        txt += '           python $RUNTIME_AREA/fillCrabFjr.py $RUNTIME_AREA/crab_fjr_$NJob.xml --errorcode $job_exit_code $executable_exit_status \n'
        txt += '       fi\n'
        txt += '    fi\n'
        txt += '    cd $RUNTIME_AREA  \n'   
        txt += '    for file in $filesToCheck ; do\n'
        txt += '        if [ -e $file ]; then\n'
        txt += '            echo "tarring file $file in  $out_files"\n'
        txt += '        else\n'
        txt += '            echo "WARNING: output file $file not found!"\n'
        txt += '        fi\n'
        txt += '    done\n'
        txt += '    export TIME_WRAP_END=`date +%s`\n'
        txt += '    let "TIME_WRAP = TIME_WRAP_END - TIME_WRAP_END_INI" \n'
        txt += '    if [ $PYTHONPATH ]; then \n'
        txt += '       if [ ! -s $RUNTIME_AREA/fillCrabFjr.py ]; then \n'
        txt += '           echo "WARNING: it is not possible to create crab_fjr.xml to final report" \n'
        txt += '       else \n'
        # call timing FJR filling
        txt += '           python $RUNTIME_AREA/fillCrabFjr.py $RUNTIME_AREA/crab_fjr_$NJob.xml --timing $TIME_WRAP $TIME_EXE $TIME_STAGEOUT \n'
        txt += '           echo "CrabWrapperTime=$TIME_WRAP" >> $RUNTIME_AREA/$repo \n' 
        txt += '           if [ $TIME_STAGEOUT -lt 0 ]; then \n'
        txt += '               export TIME_STAGEOUT=NULL \n'
        txt += '           fi\n'
        txt += '           echo "CrabStageoutTime=$TIME_STAGEOUT" >> $RUNTIME_AREA/$repo \n'
        txt += '       fi\n'
        txt += '    fi\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo \n\n'
        txt += '    echo "JOB_EXIT_STATUS = $job_exit_code"\n'
        txt += '    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'

        return txt

