from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from BlackWhiteListParser import BlackWhiteListParser
import common
from LFNBaseName import *
from JobList import JobList

import os, sys, time

#
# Base class for all grid scheduler
#

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
        Scheduler.configure(self,cfg_params)

        # init BlackWhiteListParser
        self.blackWhiteListParser = BlackWhiteListParser(cfg_params)

        self.proxyValid=0
        self.dontCheckProxy=int(cfg_params.get("EDG.dont_check_proxy",0))

#        self.rb_param_file=None
#        if (cfg_params.has_key('EDG.rb')):
#            self.rb_param_file=self.rb_configure(cfg_params.get("EDG.rb"))

        self.proxyServer = cfg_params.get("EDG.proxy_server",'myproxy.cern.ch')
        common.logger.debug(5,'Setting myproxy server to '+self.proxyServer)

        self.group = cfg_params.get("EDG.group", None)

        self.role = cfg_params.get("EDG.role", None)

        self.EDG_ce_black_list = cfg_params.get('EDG.ce_black_list',None)
        if (self.EDG_ce_black_list): self.EDG_ce_black_list = string.split(self.EDG_ce_black_list,',')

        self.EDG_ce_white_list = cfg_params.get('EDG.ce_white_list',None)
        if (self.EDG_ce_white_list): self.EDG_ce_white_list = string.split(self.EDG_ce_white_list,',')

        self.VO = cfg_params.get('EDG.virtual_organization','cms')

        self.return_data = cfg_params.get('USER.return_data',0)

        self.copy_data = cfg_params.get("USER.copy_data",0)
        if int(self.copy_data) == 1:
            self.SE = cfg_params.get('USER.storage_element',None)
            self.SE_PATH = cfg_params.get('USER.storage_path',None)
            self.srm_ver  = cfg_params.get('USER.srm_version',0) ## DS for srmv2
            if not self.SE or not self.SE_PATH:
                msg = "Error. The [USER] section does not have 'storage_element'"
                msg = msg + " and/or 'storage_path' entries, necessary to copy the output"
                common.logger.message(msg)
                raise CrabException(msg)

        if ( int(self.return_data) == 0 and int(self.copy_data) == 0 ):
           msg = 'Error: return_data = 0 and copy_data = 0 ==> your exe output will be lost\n'
           msg = msg + 'Please modify return_data and copy_data value in your crab.cfg file\n'
           raise CrabException(msg)

        if ( int(self.return_data) == 1 and int(self.copy_data) == 1 ):
           msg = 'Error: return_data and copy_data cannot be set both to 1\n'
           msg = msg + 'Please modify return_data or copy_data value in your crab.cfg file\n'
           raise CrabException(msg)

        self.publish_data = cfg_params.get("USER.publish_data",0)
        if int(self.publish_data) == 1:
            self.publish_data_name = cfg_params.get('USER.publish_data_name',None)
            if not self.publish_data_name:
                msg = "Error. The [USER] section does not have 'publish_data_name'"
                raise CrabException(msg)

            ## SL I don't like a direct call to voms-proxy here

            import urllib
            try:
                userdn = runCommand("voms-proxy-info -identity")
                self.userdn = string.strip(userdn)
            except:
                msg = "Error. Problem with voms-proxy-info -identity command"
                raise CrabException(msg)
            try:
                sitedburl="https://cmsweb.cern.ch/sitedb/sitedb/json/index/dnUserName"
                params = urllib.urlencode({'dn': self.userdn })
                f = urllib.urlopen(sitedburl,params)
                udata = f.read()
                userinfo= eval(udata)
                self.hnUserName = userinfo['user']
            except:
                msg = "Error. Problem extracting user name from %s"%sitedburl
                raise CrabException(msg)

            if not self.hnUserName:
                msg = "Error. There is no user name associated to DN %s in %s.You need to register in SiteDB"%(userdn,sitedburl)
                raise CrabException(msg)

        if ( int(self.copy_data) == 0 and int(self.publish_data) == 1 ):
           msg = 'Warning: publish_data = 1 must be used with copy_data = 1\n'
           msg = msg + 'Please modify copy_data value in your crab.cfg file\n'
           common.logger.message(msg)
           raise CrabException(msg)

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

        self._taskId=str("_".join(common._db.queryTask('name').split('_')[:-1]))
        self.jobtypeName = cfg_params.get('CRAB.jobtype','')

        self.schedulerName = cfg_params.get('CRAB.scheduler','')

        return

    def rb_configure(self, RB):
        """
        Return a requirement to be add to Jdl to select a specific RB/WMS:
        return None if RB=None
        To be re-implemented in concrete scheduler
        """
        return None

    def sched_fix_parameter(self):
        return

    def listMatch(self, dest):
        ces=Scheduler.listMatch(self,dest)
        sites=[]
        for ce in ces:
            site=ce.split(":")[0]
            if site not in sites:
                sites.append(site)
            pass

        return sites


    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        if not self.environment_unique_identifier:
            raise CrabException('environment_unique_identifier not set')

        txt = '# '+self.name()+' specific stuff\n'
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

        txt += 'MonitorJobID=${NJob}_$'+self.environment_unique_identifier+'\n'
        txt += 'SyncGridJobId=$'+self.environment_unique_identifier+'\n'
        txt += 'MonitorID='+self._taskId+'\n'
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

    # def wsCopyInput(self):
    #     """
    #     Copy input data from SE to WN
    #     """
    #     txt = ''
    #     if not self.copy_input_data: return txt

    #     ## OLI_Daniele deactivate for OSG (wait for LCG UI installed on OSG)
    #     txt += 'if [ $middleware == OSG ]; then\n'
    #     txt += '   #\n'
    #     txt += '   #   Copy Input Data from SE to this WN deactivated in OSG mode\n'
    #     txt += '   #\n'
    #     txt += '   echo "Copy Input Data from SE to this WN deactivated in OSG mode"\n'
    #     txt += 'elif [ $middleware == LCG ]; then \n'
    #     txt += '   #\n'
    #     txt += '   #   Copy Input Data from SE to this WN\n'
    #     txt += '   #\n'
    #     ### changed by georgia (put a loop copying more than one input files per jobs)
    #     txt += '   for input_file in $cur_file_list \n'
    #     txt += '   do \n'
    #     txt += '      lcg-cp --vo $VO --verbose -t 1200 lfn:$input_lfn/$input_file file:`pwd`/$input_file 2>&1\n'
    #     txt += '      copy_input_exit_status=$?\n'
    #     txt += '      echo "COPY_INPUT_EXIT_STATUS = $copy_input_exit_status"\n'
    #     txt += '      if [ $copy_input_exit_status -ne 0 ]; then \n'
    #     txt += '         echo "Problems with copying to WN" \n'
    #     txt += '      else \n'
    #     txt += '         echo "input copied into WN" \n'
    #     txt += '      fi \n'
    #     txt += '   done \n'
    #     ### copy a set of PU ntuples (same for each jobs -- but accessed randomly)
    #     txt += '   for file in $cur_pu_list \n'
    #     txt += '   do \n'
    #     txt += '      lcg-cp --vo $VO --verbose -t 1200 lfn:$pu_lfn/$file file:`pwd`/$file 2>&1\n'
    #     txt += '      copy_input_pu_exit_status=$?\n'
    #     txt += '      echo "COPY_INPUT_PU_EXIT_STATUS = $copy_input_pu_exit_status"\n'
    #     txt += '      if [ $copy_input_pu_exit_status -ne 0 ]; then \n'
    #     txt += '         echo "Problems with copying pu to WN" \n'
    #     txt += '      else \n'
    #     txt += '         echo "input pu files copied into WN" \n'
    #     txt += '      fi \n'
    #     txt += '   done \n'
    #     txt += '   \n'
    #     txt += '   ### Check SCRATCH space available on WN : \n'
    #     txt += '   df -h \n'
    #     txt += 'fi \n'

    #     return txt

    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        txt = '\n'

        txt += '#\n'
        txt += '# COPY OUTPUT FILE TO SE\n'
        txt += '#\n\n'

        SE_PATH=''
        if int(self.copy_data) == 1:
            if self.SE:
                txt += 'export SE='+self.SE+'\n'
                txt += 'echo "SE = $SE"\n'
            if self.SE_PATH:
                if ( self.SE_PATH[-1] != '/' ) : self.SE_PATH = self.SE_PATH + '/'
                SE_PATH=self.SE_PATH
            if int(self.publish_data) == 1:
                txt += '### publish_data = 1 so the SE path where to copy the output is: \n'
                #path_add = self.hnUserName + '/' + self.publish_data_name +'_${PSETHASH}/'
                path_add = PFNportion(self.publish_data_name) +'_${PSETHASH}/'
                SE_PATH = SE_PATH + path_add
            txt += 'export SE_PATH='+SE_PATH+'\n'
            txt += 'echo "SE_PATH = $SE_PATH"\n'

            txt += 'export SRM_VER='+str(self.srm_ver)+'\n' ## DS for srmVer
            txt += 'echo "SRM_VER = $SRM_VER"\n' ## DS for srmVer

            txt += 'echo ">>> Copy output files from WN = `hostname` to SE = $SE :"\n'
            txt += 'copy_exit_status=0\n'
            txt += 'for out_file in $file_list ; do\n'
            txt += '    if [ -e $SOFTWARE_DIR/$out_file ] ; then\n'
            txt += '        echo "Trying to copy output file $SOFTWARE_DIR/$out_file to $SE"\n'
            txt += '        cmscp $SOFTWARE_DIR/$out_file ${SE} ${SE_PATH} $out_file ${SRM_VER} $middleware\n'
            txt += '        if [ $cmscp_exit_status -ne 0 ]; then\n'
            txt += '            echo "Problem copying $out_file to $SE $SE_PATH"\n'
            txt += '            copy_exit_status=$cmscp_exit_status\n'
            txt += '        else\n'
            txt += '            echo "output copied into $SE/$SE_PATH directory"\n'
            txt += '        fi\n'
            txt += '    else\n'
            txt += '        copy_exit_status=60302\n'
            txt += '        echo "StageOutExitStatus = $copy_exit_status" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '        echo "StageOutExitStatusReason = file to copy not found" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '    fi\n'
            txt += 'done\n'
            txt += 'if [ $copy_exit_status -ne 0 ]; then\n'
            txt += '    SE=""\n'
            txt += '    SE_PATH=""\n'
            txt += '    job_exit_code=$copy_exit_status\n'
            txt += 'fi\n'
            pass
        return txt


    def checkProxy(self):
        """
        Function to check the Globus proxy.
        """
        if (self.proxyValid): return

        ### Just return if asked to do so
        if (self.dontCheckProxy==1):
            self.proxyValid=1
            return

        minTimeLeft=10*3600 # in seconds

        minTimeLeftServer = 100 # in hours

        mustRenew = 0
        timeLeftLocal = runCommand('voms-proxy-info -timeleft 2>/dev/null')
        timeLeftServer = -999
        if not timeLeftLocal or int(timeLeftLocal) <= 0 or not isInt(timeLeftLocal):
            mustRenew = 1
        else:
            timeLeftServer = runCommand('voms-proxy-info -actimeleft 2>/dev/null | head -1')
            if not timeLeftServer or not isInt(timeLeftServer):
                mustRenew = 1
            elif int(timeLeftLocal)<minTimeLeft or int(timeLeftServer)<minTimeLeft:
                mustRenew = 1
            pass
        pass

        if mustRenew:
            common.logger.message( "No valid proxy found or remaining time of validity of already existing proxy shorter than 10 hours!\n Creating a user proxy with default length of 192h\n")
            cmd = 'voms-proxy-init -voms '+self.VO
            if self.group:
                cmd += ':/'+self.VO+'/'+self.group
            if self.role:
                cmd += '/role='+self.role
            cmd += ' -valid 192:00'
            try:
                # SL as above: damn it!
                common.logger.debug(10,cmd)
                out = os.system(cmd)
                if (out>0): raise CrabException("Unable to create a valid proxy!\n")
            except:
                msg = "Unable to create a valid proxy!\n"
                raise CrabException(msg)
            pass

        ## now I do have a voms proxy valid, and I check the myproxy server
        renewProxy = 0
        cmd = 'myproxy-info -d -s '+self.proxyServer
        cmd_out = runCommand(cmd,0,20)
        if not cmd_out:
            common.logger.message('No credential delegated to myproxy server '+self.proxyServer+' will do now')
            renewProxy = 1
        else:
            ## minimum time: 5 days
            minTime = 4 * 24 * 3600
            ## regex to extract the right information
            myproxyRE = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)")
            for row in cmd_out.split("\n"):
                g = myproxyRE.search(row)
                if g:
                    hours = g.group("hours")
                    minutes = g.group("minutes")
                    seconds = g.group("seconds")
                    timeleft = int(hours)*3600 + int(minutes)*60 + int(seconds)
                    if timeleft < minTime:
                        renewProxy = 1
                        common.logger.message('Your proxy will expire in:\n\t'+hours+' hours '+minutes+' minutes '+seconds+' seconds\n')
                        common.logger.message('Need to renew it:')
                    pass
                pass
            pass

        # if not, create one.
        if renewProxy:
            cmd = 'myproxy-init -d -n -s '+self.proxyServer
            out = os.system(cmd)
            if (out>0):
                raise CrabException("Unable to delegate the proxy to myproxyserver "+self.proxyServer+" !\n")
            pass

        # cache proxy validity
        self.proxyValid=1
        return

    def userName(self):
        """ return the user name """
        self.checkProxy()
        tmp=runCommand("voms-proxy-info -identity")
        return tmp.strip()

    def configOpt_(self):
        edg_ui_cfg_opt = ' '
        if self.edg_config:
            edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        if self.edg_config_vo:
            edg_ui_cfg_opt += ' --config-vo ' + self.edg_config_vo + ' '
        return edg_ui_cfg_opt

    def tOut(self, list):
        return 120


    def tags(self):
        task=common._db.getTask()
        tags_tmp=string.split(task['jobType'],'"')
        tags=[str(tags_tmp[1]),str(tags_tmp[3])]
        return tags

