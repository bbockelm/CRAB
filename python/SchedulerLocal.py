from Scheduler import Scheduler
from crab_exceptions import *
from crab_util import getLocalDomain
import common
from PhEDExDatasvcInfo import PhEDExDatasvcInfo

import os,string

# Base class for all local scheduler

class SchedulerLocal(Scheduler) :

    def configure(self, cfg_params):
        self.environment_unique_identifier = None
        self.cfg_params = cfg_params
        Scheduler.configure(self,cfg_params)
        self.jobtypeName = cfg_params['CRAB.jobtype']

        name=string.upper(self.name())
        self.queue = cfg_params.get(name+'.queue',None)

        self.res = cfg_params.get(name+'.resource',None)

        # minimal padding time for jobs. For local schedulers is disabled.
        # Added for alignment purpose only (and for test) with Grid schedulers
        self.minimal_job_duration = 0

        if (cfg_params.has_key(self.name()+'.env_id')): self.environment_unique_identifier = cfg_params[self.name()+'.env_id']
        ## is this ok?
        localDomainName = getLocalDomain(self)
        if not cfg_params.has_key('GRID.se_white_list'):
            cfg_params['GRID.se_white_list']=localDomainName
            common.logger.info("Your domain name is "+str(localDomainName)+": only local dataset will be considered")
        else:
            common.logger.info("Your se_white_list is set to "+str(cfg_params['GRID.se_white_list'])+": only local dataset will be considered")
        return

    def userName(self):
        """ return the user name """
        import pwd,getpass
        tmp=pwd.getpwnam(getpass.getuser())[4]
        return "/CN="+tmp.strip()

    def envUniqueID(self):
        return

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        taskId = common._db.queryTask('name')
        if not self.environment_unique_identifier:
            try :
                self.environment_unique_identifier = self.envUniqueID()
            except :
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
        txt += "NResub=${args[1]}; export NResub\n"
        txt += "NRand=`getRandSeed`; export NRand\n"
        txt += 'OutUniqueID=_$NRand\n'
        txt += 'OutUniqueID=_$NResub$OutUniqueID\n'
        txt += 'OutUniqueID=$NJob$OutUniqueID; export OutUniqueID\n'

        txt += "out_files=out_files_${NJob}; export out_files\n"
        txt += "echo $out_files\n"
        txt += jbt.outList()

        txt += 'SyncGridJobId=`echo '+self.environment_unique_identifier+'`\n'
        txt += 'MonitorJobID=`echo ${NJob}_${SyncGridJobId}`\n'
        txt += 'MonitorID=`echo ' + taskId + '`\n'

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
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        txt = '\n'
        if int(self.copy_data) == 1:

            stageout = PhEDExDatasvcInfo(self.cfg_params)
            endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()
            if self.check_RemoteDir == 1 :
                self.checkRemoteDir(endpoint,jbt.outList('list') )

            txt += '#\n'
            txt += '# COPY OUTPUT FILE TO '+SE_PATH+ '\n'
            txt += '#\n\n'

            txt += 'export SE='+SE+'\n'
            txt += 'echo "SE = $SE"\n'
            txt += 'export SE_PATH='+SE_PATH+'\n'
            txt += 'echo "SE_PATH = $SE_PATH"\n'
            txt += 'export LFNBaseName='+lfn+'\n'
            txt += 'echo "LFNBaseName = $LFNBaseName"\n'
            txt += 'export USER='+user+'\n'
            txt += 'echo "USER = $USER"\n'
            txt += 'export endpoint='+endpoint+'\n'
            txt += 'echo "endpoint = $endpoint"\n'
            ### Needed i.e. for caf
            if (pool) and (pool != 'None'):
                txt += 'export STAGE_SVCCLASS='+str(pool)+'\n'

            txt += 'echo ">>> Copy output files from WN = `hostname` to $SE_PATH :"\n'
            txt += 'export TIME_STAGEOUT_INI=`date +%s` \n'
            txt += 'copy_exit_status=0\n'
            cmscp_args = ' --destination $endpoint --inputFileList $file_list'
            ### FEDE FOR MULTI #### 
            #cmscp_args +=' --middleware $middleware --lfn $LFNBaseName %s %s '%(self.loc_stage_out,self.debugWrap)
            cmscp_args +=' --middleware $middleware --se_name $SE --for_lfn $LFNBaseName %s %s '%(self.loc_stage_out,self.debugWrap)
            txt += 'echo "python cmscp.py %s "\n'%cmscp_args
            txt += 'python cmscp.py %s \n'%cmscp_args
            if self.debug_wrapper==1:
                txt += 'if [ -f .SEinteraction.log ] ;then\n'
                txt += '########### details of SE interaction\n'
                txt += '    cat .SEinteraction.log\n'
                txt += 'else\n'
                txt += '    echo ".SEinteraction.log file not found"\n'
                txt += 'fi\n'
                txt += '##################################### \n'
            txt += 'if [ -f cmscpReport.sh ] ;then\n'
            txt += '    cat cmscpReport.sh\n'
            txt += '    source cmscpReport.sh\n'
            #### FEDE 
            txt += '    source_result=$? \n'
            txt += '    if [ $source_result -ne 0 ]; then\n'
            txt += '        echo "problem with the source of cmscpReport.sh file"\n'
            txt += '        StageOutExitStatus=60307\n'
            txt += '    fi\n'
            #########
            txt += 'else\n'
            txt += '    echo "cmscpReport.sh file not found"\n'
            txt += '    StageOutExitStatus=60307\n'
            txt += 'fi\n'
            txt += 'if [ $StageOutExitStatus -ne 0 ]; then\n'
            txt += '    echo "Problem copying file to $SE $SE_PATH"\n'
            txt += '    copy_exit_status=$StageOutExitStatus \n'
            if not self.debug_wrapper==1:
                txt += '    ########### details of SE interaction\n'
                txt += '    if [ -f .SEinteraction.log ] ;then\n'
                txt += '        cat .SEinteraction.log\n'
                txt += '    else\n'
                txt += '        echo ".SEinteraction.log file not found"\n'
                txt += '    fi\n'
                txt += '    ##################################### \n'
            txt += '    job_exit_code=$StageOutExitStatus\n'
            txt += 'fi\n'
            txt += 'export TIME_STAGEOUT_END=`date +%s` \n'
            txt += 'let "TIME_STAGEOUT = TIME_STAGEOUT_END - TIME_STAGEOUT_INI" \n'
        else:
            txt += 'export TIME_STAGEOUT=-1 \n'
        return txt

    def wsExitFunc_common(self):
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
        txt += '    if [ $middleware == OSG ]; then\n'
        txt += '        final_list=$filesToCheck\n'
        txt += '        if [ $WORKING_DIR ]; then\n'
        txt += '            remove_working_dir\n'
        txt += '        fi\n'
        txt += '        symlinks -d .\n'
        txt += '    else\n'
        txt += '        final_list=$filesToCheck" .BrokerInfo"\n'
        txt += '    fi\n'
        txt += '    export TIME_WRAP_END=`date +%s`\n'
        txt += '    let "TIME_WRAP = TIME_WRAP_END - TIME_WRAP_END_INI" \n\n'
        # padding for minimal job duration
        txt += '    let "MIN_JOB_DURATION = 60*%d" \n'%self.minimal_job_duration
        txt += '    let "PADDING_DURATION = MIN_JOB_DURATION - TIME_WRAP" \n'
        txt += '    if [ $PADDING_DURATION -gt 0 ]; then \n'
        txt += '        echo ">>> padding time: Sleeping the wrapper for $PADDING_DURATION seconds"\n'
        txt += '        sleep $PADDING_DURATION\n'
        txt += '        TIME_WRAP_END=`date +%s`\n'
        txt += '        let "TIME_WRAP = TIME_WRAP_END - TIME_WRAP_INI" \n'
        txt += '    else \n'
        txt += '        echo ">>> padding time: Wrapper lasting more than $MIN_JOB_DURATION seconds. No sleep required."\n'
        txt += '    fi\n\n'
        # call timing FJR filling
        txt += '    if [ $PYTHONPATH ]; then \n'
        txt += '       if [ ! -s $RUNTIME_AREA/fillCrabFjr.py ]; then \n'
        txt += '           echo "WARNING: it is not possible to create crab_fjr.xml to final report" \n'
        txt += '       else \n'
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

