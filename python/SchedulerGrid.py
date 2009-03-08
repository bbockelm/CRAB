"""
Base class for all grid schedulers
"""

__revision__ = "$Id: SchedulerGrid.py,v 1.99 2009/03/07 16:37:15 spiga Exp $"
__version__ = "$Revision: 1.99 $"

from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser
import common
from PhEDExDatasvcInfo import PhEDExDatasvcInfo
from JobList import JobList

import os, sys, time

class SchedulerGrid(Scheduler):

    def __init__(self, name):
        Scheduler.__init__(self,name)
        self.states = [ "Acl", "cancelReason", "cancelling","ce_node","children", \
                      "children_hist","children_num","children_states","condorId","condor_jdl", \
                      "cpuTime","destination", "done_code","exit_code","expectFrom", \
                      "expectUpdate","globusId","jdl","jobId","jobtype", \
                      "lastUpdateTime","localId","location", "matched_jdl","network_server", \
                      "owner","parent_job", "reason","resubmitted","rsl","seed",\
                      "stateEnterTime","stateEnterTimes","subjob_failed", \
                      "user tags" , "status" , "status_code","hierarchy"]
        return

    def configure(self, cfg_params):
        self.cfg_params = cfg_params
        self.jobtypeName   = cfg_params.get('CRAB.jobtype','')
        self.schedulerName = cfg_params.get('CRAB.scheduler','')
        Scheduler.configure(self,cfg_params)

        self.proxyValid=0
        self.dontCheckProxy=int(cfg_params.get("EDG.dont_check_proxy",0))

        self.proxyServer = cfg_params.get("EDG.proxy_server",'myproxy.cern.ch')
        common.logger.debug(5,'Setting myproxy server to '+self.proxyServer)

        self.group = cfg_params.get("EDG.group", None)
        self.role = cfg_params.get("EDG.role", None)

        removeT1bL = cfg_params.get("EDG.remove_default_blacklist", 0 )

        T1_BL = ["fnal.gov", "gridka.de" ,"w-ce01.grid.sinica.edu.tw", "w-ce02.grid.sinica.edu.tw", "lcg00125.grid.sinica.edu.tw",\
                  "gridpp.rl.ac.uk" , "cclcgceli03.in2p3.fr","cclcgceli04.in2p3.fr" , "pic.es", "cnaf"]
        if int(removeT1bL) == 1:
            T1_BL = []
        self.EDG_ce_black_list = cfg_params.get('EDG.ce_black_list',None)
        if (self.EDG_ce_black_list):
            self.EDG_ce_black_list = string.split(self.EDG_ce_black_list,',') + T1_BL
        else :
            if int(removeT1bL) == 0: self.EDG_ce_black_list = T1_BL
        self.EDG_ce_white_list = cfg_params.get('EDG.ce_white_list',None)
        if (self.EDG_ce_white_list): self.EDG_ce_white_list = string.split(self.EDG_ce_white_list,',')

        self.VO = cfg_params.get('EDG.virtual_organization','cms')
        self.EDG_requirements = cfg_params.get('EDG.requirements',None)
        self.EDG_addJdlParam = cfg_params.get('EDG.additional_jdl_parameters',None)

        if (self.EDG_addJdlParam): self.EDG_addJdlParam = string.split(self.EDG_addJdlParam,';')

        self.EDG_retry_count = cfg_params.get('EDG.retry_count',0)
        self.EDG_shallow_retry_count= cfg_params.get('EDG.shallow_retry_count',-1)
        self.EDG_clock_time = cfg_params.get('EDG.max_wall_clock_time',None)

        # Default minimum CPU time to >= 130 minutes
        self.EDG_cpu_time = cfg_params.get('EDG.max_cpu_time', '130')

        # Add EDG_WL_LOCATION to the python path
        if not os.environ.has_key('EDG_WL_LOCATION'):
            msg = "Error: the EDG_WL_LOCATION variable is not set."
            raise CrabException(msg)
        path = os.environ['EDG_WL_LOCATION']

        libPath=os.path.join(path, "lib")
        sys.path.append(libPath)
        libPath=os.path.join(path, "lib", "python")
        sys.path.append(libPath)

        self.checkProxy()
        return

    def rb_configure(self, RB):
        """
        Return a requirement to be add to Jdl to select a specific RB/WMS:
        return None if RB=None
        To be re-implemented in concrete scheduler
        """
        return None

    def sched_fix_parameter(self):
        """
        Returns string with requirements and scheduler-specific parameters
        """
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        req = ''
        req = req + jbt.getRequirements()

        if self.EDG_requirements:
            if (not req == ' '):
                req = req +  ' && '
            req = req + self.EDG_requirements

        taskReq = {'jobType':req}
        common._db.updateTask_(taskReq)

    def listMatch(self, dest, full):
        matching='fast'
        ces=Scheduler.listMatch(self, dest, full)
        sites=[]
        for ce in ces:
            site=ce.split(":")[0]
            if site not in sites:
                sites.append(site)
            pass
        if full == True: matching='full'
        common.logger.write("list of available site ( "+str(matching) +" matching ) : "+str(sites))
        return sites


    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        taskId =common._db.queryTask('name')
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        if not self.environment_unique_identifier:
            raise CrabException('environment_unique_identifier not set')

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

        txt += 'MonitorJobID=${NJob}_'+self.environment_unique_identifier+'\n'
        txt += 'SyncGridJobId='+self.environment_unique_identifier+'\n'
        txt += 'MonitorID='+taskId+'\n'
        txt += 'echo "MonitorJobID=$MonitorJobID" > $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=$SyncGridJobId" >> $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=$MonitorID" >> $RUNTIME_AREA/$repo\n'

        txt += 'echo ">>> GridFlavour discovery: " \n'
        txt += 'if [ $OSG_APP ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        SyncCE="$OSG_JOB_CONTACT"; \n'
        txt += '        echo "SyncCE=$SyncCE" >> $RUNTIME_AREA/$repo ;\n'
        txt += '    else\n'
        txt += '        echo "not reporting SyncCE";\n'
        txt += '    fi\n';
        txt += '    echo "GridFlavour=$middleware" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "source OSG GRID setup script" \n'
        txt += '    source $OSG_GRID/setup.sh \n'

        txt += 'elif [ $VO_CMS_SW_DIR ]; then \n'
        txt += '    middleware=LCG \n'
        txt += '    echo "SyncCE=`glite-brokerinfo getCE`" >> $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=$middleware" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'else \n'
        txt += '    echo "ERROR ==> GridFlavour not identified" \n'
        txt += '    job_exit_code=10030 \n'
        txt += '    func_exit \n'
        txt += 'fi \n'

        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '\n\n'

        txt += 'export VO='+self.VO+'\n'
        txt += 'if [ $middleware == LCG ]; then\n'
        txt += '    CloseCEs=`glite-brokerinfo getCE`\n'
        txt += '    echo "CloseCEs = $CloseCEs"\n'
        txt += '    CE=`echo $CloseCEs | sed -e "s/:.*//"`\n'
        txt += '    echo "CE = $CE"\n'
        txt += 'elif [ $middleware == OSG ]; then \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        CE=`echo $OSG_JOB_CONTACT | /usr/bin/awk -F\/ \'{print $1}\'` \n'
        txt += '    else \n'
        txt += '        echo "ERROR ==> OSG mode in setting CE name from OSG_JOB_CONTACT" \n'
        txt += '        job_exit_code=10099\n'
        txt += '        func_exit\n'
        txt += '    fi \n'
        txt += 'fi \n'

        return txt

    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()

        txt = '\n'

        txt += '#\n'
        txt += '# COPY OUTPUT FILE TO SE\n'
        txt += '#\n\n'

        if int(self.copy_data) == 1:
            stageout = PhEDExDatasvcInfo(self.cfg_params)
            endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()
            if self.check_RemoteDir == 1 : 
                self.checkRemoteDir(endpoint,jbt.outList('list') )
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

            txt += 'echo ">>> Copy output files from WN = `hostname` to $SE_PATH :"\n'
            txt += 'export TIME_STAGEOUT_INI=`date +%s` \n'
            txt += 'copy_exit_status=0\n'
            cmscp_args = ' --destination $endpoint --inputFileList $file_list'
            cmscp_args +=' --middleware $middleware --lfn $LFNBaseName %s %s '%(self.loc_stage_out,self.debugWrap)
            txt += 'echo "python cmscp.py %s "\n'%cmscp_args
            txt += 'python cmscp.py %s \n'%cmscp_args
            if self.debug_wrapper==1:
                txt += 'echo "which lcg-ls"\n'
                txt += 'which lcg-ls\n'
                txt += 'echo "########### details of SE interaction"\n'
                txt += 'if [ -f .SEinteraction.log ] ;then\n'
                txt += '    cat .SEinteraction.log\n'
                txt += 'else\n'
                txt += '    echo ".SEinteraction.log file not found"\n'
                txt += 'fi\n'
                txt += 'echo "#####################################"\n'

            txt += 'if [ -f cmscpReport.sh ] ;then\n'
            txt += '    cat cmscpReport.sh\n'
            txt += '    source cmscpReport.sh\n'
            txt += 'else\n'
            txt += '    echo "cmscpReport.sh file not found"\n' 
            txt += '    StageOutExitStatus=60307\n'
            txt += 'fi\n'
            txt += 'if [ $StageOutExitStatus -ne 0 ]; then\n'
            txt += '    echo "Problem copying file to $SE $SE_PATH"\n'
            txt += '    copy_exit_status=$StageOutExitStatus \n'
            if not self.debug_wrapper==1:
                txt += 'if [ -f .SEinteraction.log ] ;then\n'
                txt += '    echo "########## contents of SE interaction"\n'
                txt += '    cat .SEinteraction.log\n'
                txt += '    echo "#####################################"\n'
                txt += 'else\n'
                txt += '    echo ".SEinteraction.log file not found"\n'
                txt += 'fi\n'
            txt += '    job_exit_code=$StageOutExitStatus\n'
            txt += 'fi\n'
            txt += 'export TIME_STAGEOUT_END=`date +%s` \n'
            txt += 'let "TIME_STAGEOUT = TIME_STAGEOUT_END - TIME_STAGEOUT_INI" \n'
        else:
            # set stageout timing to a fake value
            txt += 'export TIME_STAGEOUT=-1 \n'
        return txt

    def userName(self):
        """ return the user name """
        tmp=runCommand("voms-proxy-info -identity 2>/dev/null")
        return tmp.strip()

    def configOpt_(self):
        edg_ui_cfg_opt = ' '
        if self.edg_config:
            edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        if self.edg_config_vo:
            edg_ui_cfg_opt += ' --config-vo ' + self.edg_config_vo + ' '
        return edg_ui_cfg_opt



    def tags(self):
        task=common._db.getTask()
        tags_tmp=string.split(task['jobType'],'"')
        tags=[str(tags_tmp[1]),str(tags_tmp[3])]
        return tags

