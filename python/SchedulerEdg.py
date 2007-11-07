from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from EdgConfig import *
from BlackWhiteListParser import BlackWhiteListParser
import common

import os, sys, time

class SchedulerEdg(Scheduler):
    def __init__(self):
        Scheduler.__init__(self,"EDG")
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

        # init BlackWhiteListParser
        self.blackWhiteListParser = BlackWhiteListParser(cfg_params)

        self.proxyValid=0
        try: self.dontCheckProxy=int(cfg_params["EDG.dont_check_proxy"])
        except KeyError: self.dontCheckProxy = 0

        try:
            RB=cfg_params["EDG.rb"]
            self.rb_param_file=self.rb_configure(RB)
        except KeyError:
            self.rb_param_file=''
            pass
        try:
            self.proxyServer = cfg_params["EDG.proxy_server"]
        except KeyError:
            self.proxyServer = 'myproxy.cern.ch'
        common.logger.debug(5,'Setting myproxy server to '+self.proxyServer)

        try:
            self.group = cfg_params["EDG.group"]
        except KeyError:
            self.group = None
            
        try:
            self.role = cfg_params["EDG.role"]
        except KeyError:
            self.role = None
            
        #try: self.LCG_version = cfg_params["EDG.lcg_version"]
        #except KeyError: self.LCG_version = '2'

        try: 
            self.EDG_ce_black_list = cfg_params['EDG.ce_black_list']
        except KeyError: 
            self.EDG_ce_black_list  = ''

        try: 
            self.EDG_ce_white_list = cfg_params['EDG.ce_white_list']
        except KeyError: self.EDG_ce_white_list = ''

        try: self.VO = cfg_params['EDG.virtual_organization']
        except KeyError: self.VO = 'cms'

        try: self.copy_input_data = cfg_params["USER.copy_input_data"]
        except KeyError: self.copy_input_data = 0

        try: self.return_data = cfg_params['USER.return_data']
        except KeyError: self.return_data = 0

        try: 
            self.copy_data = cfg_params["USER.copy_data"]
            if int(self.copy_data) == 1:
                try:
                    self.SE = cfg_params['USER.storage_element']
                    self.SE_PATH = cfg_params['USER.storage_path']
                except KeyError:
                    msg = "Error. The [USER] section does not have 'storage_element'"
                    msg = msg + " and/or 'storage_path' entries, necessary to copy the output"
                    common.logger.message(msg)
                    raise CrabException(msg)
        except KeyError: self.copy_data = 0 

        if ( int(self.return_data) == 0 and int(self.copy_data) == 0 ):
           msg = 'Error: return_data = 0 and copy_data = 0 ==> your exe output will be lost\n' 
           msg = msg + 'Please modify return_data and copy_data value in your crab.cfg file\n' 
           raise CrabException(msg)

        if ( int(self.return_data) == 1 and int(self.copy_data) == 1 ):
           msg = 'Error: return_data and copy_data cannot be set both to 1\n'
           msg = msg + 'Please modify return_data or copy_data value in your crab.cfg file\n' 
           raise CrabException(msg)

        ########### FEDE FOR DBS2 ##############################
        try: 
            self.publish_data = cfg_params["USER.publish_data"]
            self.checkProxy()
            if int(self.publish_data) == 1:
                try:
                    self.publish_data_name = cfg_params['USER.publish_data_name']
                except KeyError:
                    msg = "Error. The [USER] section does not have 'publish_data_name'"
                    raise CrabException(msg)
                try:
                    tmp = runCommand("voms-proxy-info -identity")
                    tmp = string.split(tmp,'/')
                    reCN=re.compile(r'CN=')
                    for t in tmp:
                        if reCN.match(t):
                            self.UserGridName=string.strip((t.replace('CN=','')).replace(' ',''))
                        
                    #self.UserGridName = string.strip(runCommand("voms-proxy-info -identity | awk -F\'CN\' \'{print $2$3$4}\' | tr -d \'=/ \'"))
                except:
                    msg = "Error. Problem with voms-proxy-info -identity command"
                    raise CrabException(msg)
        except KeyError: self.publish_data = 0 

        if ( int(self.copy_data) == 0 and int(self.publish_data) == 1 ):
           msg = 'Warning: publish_data = 1 must be used with copy_data = 1\n' 
           msg = msg + 'Please modify copy_data value in your crab.cfg file\n' 
           common.logger.message(msg)
           raise CrabException(msg)
        #################################################

        #try:
        #    self.lfc_host = cfg_params['EDG.lfc_host']
        #except KeyError:
        #    msg = "Error. The [EDG] section does not have 'lfc_host' value"
        #    msg = msg + " it's necessary to know the LFC host name"
        #    common.logger.message(msg)
        #    raise CrabException(msg)
        #try:
        #    self.lcg_catalog_type = cfg_params['EDG.lcg_catalog_type']
        #except KeyError:
        #    msg = "Error. The [EDG] section does not have 'lcg_catalog_type' value"
        #    msg = msg + " it's necessary to know the catalog type"
        #    common.logger.message(msg)
        #    raise CrabException(msg)
        #try:
        #    self.lfc_home = cfg_params['EDG.lfc_home']
        #except KeyError:
        #    msg = "Error. The [EDG] section does not have 'lfc_home' value"
        #    msg = msg + " it's necessary to know the home catalog dir"
        #    common.logger.message(msg)
        #    raise CrabException(msg)
      
        #try: 
        #    self.register_data = cfg_params["USER.register_data"]
        #    if int(self.register_data) == 1:
        #        try:
        #            self.LFN = cfg_params['USER.lfn_dir']
        #        except KeyError:
        #            msg = "Error. The [USER] section does not have 'lfn_dir' value"
        #            msg = msg + " it's necessary for LCF registration"
        #            common.logger.message(msg)
        #            raise CrabException(msg)
        #except KeyError: self.register_data = 0

        #if ( int(self.copy_data) == 0 and int(self.register_data) == 1 ):
        #   msg = 'Warning: register_data = 1 must be used with copy_data = 1\n' 
        #   msg = msg + 'Please modify copy_data value in your crab.cfg file\n' 
        #   common.logger.message(msg)
        #   raise CrabException(msg)

        try: self.EDG_requirements = cfg_params['EDG.requirements']
        except KeyError: self.EDG_requirements = ''

        try: self.EDG_addJdlParam = string.split(cfg_params['EDG.additional_jdl_parameters'],',')
        except KeyError: self.EDG_addJdlParam = []

        try: self.EDG_retry_count = cfg_params['EDG.retry_count']
        except KeyError: self.EDG_retry_count = ''

        try: self.EDG_shallow_retry_count= cfg_params['EDG.shallow_retry_count']
        except KeyError: self.EDG_shallow_retry_count = ''

        try: self.EDG_clock_time = cfg_params['EDG.max_wall_clock_time']
        except KeyError: self.EDG_clock_time= ''

        try: self.EDG_cpu_time = cfg_params['EDG.max_cpu_time']
        except KeyError: self.EDG_cpu_time = ''

        # Add EDG_WL_LOCATION to the python path

        try:
            path = os.environ['EDG_WL_LOCATION']
        except:
            msg = "Error: the EDG_WL_LOCATION variable is not set."
            raise CrabException(msg)

        libPath=os.path.join(path, "lib")
        sys.path.append(libPath)
        libPath=os.path.join(path, "lib", "python")
        sys.path.append(libPath)

        try:
            self._taskId = cfg_params['taskId']
        except:
            self._taskId = ''

        try: self.jobtypeName = cfg_params['CRAB.jobtype']
        except KeyError: self.jobtypeName = ''
 
        try: self.schedulerName = cfg_params['CRAB.scheduler']
        except KeyError: self.scheduler = ''

        return
    

    def rb_configure(self, RB):
        self.edg_config = ''
        self.edg_config_vo = ''
        self.rb_param_file = ''

        edgConfig = EdgConfig(RB)
        self.edg_config = edgConfig.config()
        self.edg_config_vo = edgConfig.configVO()

        if (self.edg_config and self.edg_config_vo != ''):
            self.rb_param_file = 'RBconfig = "'+self.edg_config+'";\nRBconfigVO = "'+self.edg_config_vo+'";\n'
            #print "rb_param_file = ", self.rb_param_file
        return self.rb_param_file
       

    def sched_parameter(self):
        """
        Returns file with requirements and scheduler-specific parameters
        """
        index = int(common.jobDB.nJobs()) - 1
        job = common.job_list[index]
        jbt = job.type()
        
        lastBlock=-1
        first = []
        for n in range(common.jobDB.nJobs()):
            currBlock=common.jobDB.block(n)
            if (currBlock!=lastBlock):
                lastBlock = currBlock
                first.append(n)
  
        req = ''
        req = req + jbt.getRequirements()
    
        if self.EDG_requirements:
            if (req == ' '):
                req = req + self.EDG_requirements
            else:
                req = req +  ' && ' + self.EDG_requirements

        if self.EDG_ce_white_list:
            ce_white_list = string.split(self.EDG_ce_white_list,',')
            for i in range(len(ce_white_list)):
                if i == 0:
                    if (req == ' '):
                        req = req + '((RegExp("' + string.strip(ce_white_list[i]) + '", other.GlueCEUniqueId))'
                    else:
                        req = req +  ' && ((RegExp("' +  string.strip(ce_white_list[i]) + '", other.GlueCEUniqueId))'
                    pass
                else:
                    req = req +  ' || (RegExp("' +  string.strip(ce_white_list[i]) + '", other.GlueCEUniqueId))'
            req = req + ')'
        
        if self.EDG_ce_black_list:
            ce_black_list = string.split(self.EDG_ce_black_list,',')
            for ce in ce_black_list:
                if (req == ' '):
                    req = req + '(!RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId))'
                else:
                    req = req +  ' && (!RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId))'
                pass
        if self.EDG_clock_time:
            if (req == ' '):
                req = req + 'other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time
            else:
                req = req + ' && other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time

        if self.EDG_cpu_time:
            if (req == ' '):
                req = req + ' other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time
            else:
                req = req + ' && other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time
                 
        for i in range(len(first)): # Add loop DS
            groupReq = req
            self.param='sched_param_'+str(i)+'.clad'
            param_file = open(common.work_space.shareDir()+'/'+self.param, 'w')

            itr4=self.findSites_(first[i])
            for arg in itr4:
                groupReq = groupReq + ' && anyMatch(other.storage.CloseSEs, ('+str(arg)+'))'
            param_file.write('Requirements = '+groupReq +';\n')   
   
            if (self.rb_param_file != ''):
                param_file.write(self.rb_param_file)   

            if len(self.EDG_addJdlParam):
                for p in self.EDG_addJdlParam:
                    param_file.write(p)

            param_file.close()   


    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        txt = ''
        txt += '# strip arguments\n'
        txt += 'echo "strip arguments"\n'
        txt += 'args=("$@")\n'
        txt += 'nargs=$#\n'
        txt += 'shift $nargs\n'
        txt += "# job number (first parameter for job wrapper)\n"
        #txt += "NJob=$1\n"
        txt += "NJob=${args[0]}\n"

        txt += '# job identification to DashBoard \n'
        txt += 'MonitorJobID=`echo ${NJob}_$EDG_WL_JOBID`\n'
        txt += 'SyncGridJobId=`echo $EDG_WL_JOBID`\n'
        txt += 'MonitorID=`echo ' + self._taskId + '`\n'
        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=`echo $SyncGridJobId`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'

        txt += 'echo "middleware discovery: " \n'
        txt += 'if [ $GRID3_APP_DIR ]; then\n'
        txt += '    middleware=OSG \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        SyncCE="$OSG_JOB_CONTACT"; \n'
        txt += '        echo "SyncCE=$SyncCE" | tee -a $RUNTIME_AREA/$repo ;\n'
        txt += '    else\n'
        txt += '        echo "not reporting SyncCE";\n'
        txt += '    fi\n';
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo ">>> middleware =$middleware" \n'
        txt += 'elif [ $OSG_APP ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        SyncCE="$OSG_JOB_CONTACT"; \n'
        txt += '        echo "SyncCE=$SyncCE" | tee -a $RUNTIME_AREA/$repo ;\n'
        txt += '    else\n'
        txt += '        echo "not reporting SyncCE";\n'
        txt += '    fi\n';
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo ">>> middleware =$middleware" \n'
        txt += 'elif [ $VO_CMS_SW_DIR ]; then \n'
        txt += '    middleware=LCG \n'
   #     txt += '    echo "SyncCE=`edg-brokerinfo getCE`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "SyncCE=`glite-brokerinfo getCE`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo ">>> middleware =$middleware" \n'
        txt += 'else \n'
        txt += '    echo "SET_CMS_ENV 10030 ==> middleware not identified" \n'
        txt += '    echo "JOB_EXIT_STATUS = 10030" \n'
        txt += '    echo "JobExitCode=10030" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '    exit 1 \n'
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
        txt += '        echo "SET_CMS_ENV 10099 ==> OSG mode: ERROR in setting CE name from OSG_JOB_CONTACT" \n'
        txt += '        echo "JOB_EXIT_STATUS = 10099" \n'
        txt += '        echo "JobExitCode=10099" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '        dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '        exit 1 \n'
        txt += '    fi \n'
        txt += 'fi \n' 

        return txt

    def wsCopyInput(self):
        """
        Copy input data from SE to WN     
        """
        txt = ''
        if not self.copy_input_data: return txt

        ## OLI_Daniele deactivate for OSG (wait for LCG UI installed on OSG)
        txt += 'if [ $middleware == OSG ]; then\n' 
        txt += '   #\n'
        txt += '   #   Copy Input Data from SE to this WN deactivated in OSG mode\n'
        txt += '   #\n'
        txt += '   echo "Copy Input Data from SE to this WN deactivated in OSG mode"\n'
        txt += 'elif [ $middleware == LCG ]; then \n'
        txt += '   #\n'
        txt += '   #   Copy Input Data from SE to this WN\n'
        txt += '   #\n'
        ### changed by georgia (put a loop copying more than one input files per jobs)           
        txt += '   for input_file in $cur_file_list \n'
        txt += '   do \n'
        txt += '      lcg-cp --vo $VO --verbose -t 1200 lfn:$input_lfn/$input_file file:`pwd`/$input_file 2>&1\n'
        txt += '      copy_input_exit_status=$?\n'
        txt += '      echo "COPY_INPUT_EXIT_STATUS = $copy_input_exit_status"\n'
        txt += '      if [ $copy_input_exit_status -ne 0 ]; then \n'
        txt += '         echo "Problems with copying to WN" \n'
        txt += '      else \n'
        txt += '         echo "input copied into WN" \n'
        txt += '      fi \n'
        txt += '   done \n'
        ### copy a set of PU ntuples (same for each jobs -- but accessed randomly)
        txt += '   for file in $cur_pu_list \n'
        txt += '   do \n'
        txt += '      lcg-cp --vo $VO --verbose -t 1200 lfn:$pu_lfn/$file file:`pwd`/$file 2>&1\n'
        txt += '      copy_input_pu_exit_status=$?\n'
        txt += '      echo "COPY_INPUT_PU_EXIT_STATUS = $copy_input_pu_exit_status"\n'
        txt += '      if [ $copy_input_pu_exit_status -ne 0 ]; then \n'
        txt += '         echo "Problems with copying pu to WN" \n'
        txt += '      else \n'
        txt += '         echo "input pu files copied into WN" \n'
        txt += '      fi \n'
        txt += '   done \n'
        txt += '   \n'
        txt += '   ### Check SCRATCH space available on WN : \n'
        txt += '   df -h \n'
        txt += 'fi \n' 
           
        return txt

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
                path_add = self.UserGridName + '/' + self.publish_data_name +'_${PSETHASH}/'
                SE_PATH = SE_PATH + path_add
            txt += 'export SE_PATH='+SE_PATH+'\n'
            txt += 'echo "SE_PATH = $SE_PATH"\n'
            
            txt += 'echo ">>> Copy output files from WN = `hostname` to SE = $SE :"\n'
            
            txt += 'if [ $output_exit_status -eq 60302 ]; then\n'
            txt += '    echo "--> No output file to copy to $SE"\n'
            txt += '    copy_exit_status=$output_exit_status\n'
            txt += '    echo "COPY_EXIT_STATUS = $copy_exit_status"\n'
            txt += 'else\n'
            txt += '    for out_file in $file_list ; do\n'
            txt += '        echo "Trying to copy output file to $SE"\n'
            txt += '        cmscp $out_file ${SE} ${SE_PATH} $out_file $middleware\n'
            txt += '        copy_exit_status=$?\n'
            txt += '        echo "COPY_EXIT_STATUS = $copy_exit_status"\n'
            txt += '        echo "STAGE_OUT = $copy_exit_status"\n'
            txt += '        if [ $copy_exit_status -ne 0 ]; then\n'
            txt += '            echo "Problem copying $out_file to $SE $SE_PATH"\n'
            txt += '            echo "StageOutExitStatus = $copy_exit_status " | tee -a $RUNTIME_AREA/$repo\n'
            txt += '            copy_exit_status=60307\n'
            txt += '        else\n'
            txt += '            echo "StageOutSE = $SE" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '            echo "StageOutCatalog = " | tee -a $RUNTIME_AREA/$repo\n'
            txt += '            echo "output copied into $SE/$SE_PATH directory"\n'
            txt += '            echo "StageOutExitStatus = 0" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '        fi\n'
            txt += '    done\n'
            txt += '    if [ $copy_exit_status -ne 0 ]; then\n'
            txt += '        SE=""\n'
            txt += '        echo "SE = $SE"\n'
            txt += '        SE_PATH=""\n'
            txt += '        echo "SE_PATH = $SE_PATH"\n'
            txt += '    fi\n'
            txt += 'fi\n'
            txt += 'exit_status=$copy_exit_status\n'
            pass
        return txt

    def loggingInfo(self, id):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        self.checkProxy()
        cmd = 'edg-job-get-logging-info -v 2 ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """
        cmd = 'edg-job-status '+id
        cmd_out = runCommand(cmd)
        return cmd_out

    def findSites_(self, n):
        itr4 =[] 

        sites = common.jobDB.destination(n)

        if len(sites)>0 and sites[0]=="":
            return itr4

        itr = ''
        if sites != [""]:#CarlosDaniele 
            ##Addedd Daniele
            replicas = self.blackWhiteListParser.checkBlackList(sites,n)
            if len(replicas)!=0:
                replicas = self.blackWhiteListParser.checkWhiteList(replicas,n)
              
            if len(replicas)==0:
                itr = itr + 'target.GlueSEUniqueID=="NONE" '
                #msg = 'No sites remaining that host any part of the requested data! Exiting... '
                #raise CrabException(msg)
            #####         
           # for site in sites: 
            for site in replicas: 
                #itr = itr + 'target.GlueSEUniqueID==&quot;'+site+'&quot; || '
                itr = itr + 'target.GlueSEUniqueID=="'+site+'" || '
            itr = itr[0:-4]
            itr4.append( itr )
        return itr4

    def createXMLSchScript(self, nj, argsList):
       
        """
        Create a XML-file for BOSS4.
        """
  #      job = common.job_list[nj]
        """
        INDY
        [begin] FIX-ME:
        I would pass jobType instead of job
        """
        index = nj - 1
        job = common.job_list[index]
        jbt = job.type()
        
        inp_sandbox = jbt.inputSandbox(index)
        #out_sandbox = jbt.outputSandbox(index)
        """
        [end] FIX-ME
        """

        
        title = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
        jt_string = ''
        
        xml_fname = str(self.jobtypeName)+'.xml'
        xml = open(common.work_space.shareDir()+'/'+xml_fname, 'a')

        #TaskName   
        dir = string.split(common.work_space.topDir(), '/')
        taskName = dir[len(dir)-2]
  
        to_write = ''

        req=' '
        req = req + jbt.getRequirements()

        if self.EDG_requirements:
            if (req == ' '):
                req = req + self.EDG_requirements
            else:
                req = req +  ' && ' + self.EDG_requirements
        if self.EDG_ce_white_list:
            ce_white_list = string.split(self.EDG_ce_white_list,',')
            for i in range(len(ce_white_list)):
                if i == 0:
                    if (req == ' '):
                        req = req + '((RegExp("' + ce_white_list[i] + '", other.GlueCEUniqueId))'
                    else:
                        req = req +  ' && ((RegExp("' + ce_white_list[i] + '", other.GlueCEUniqueId))'
                    pass
                else:
                    req = req +  ' || (RegExp("' + ce_white_list[i] + '", other.GlueCEUniqueId))'
            req = req + ')'
        
        if self.EDG_ce_black_list:
            ce_black_list = string.split(self.EDG_ce_black_list,',')
            for ce in ce_black_list:
                if (req == ' '):
                    req = req + '(!RegExp("' + ce + '", other.GlueCEUniqueId))'
                else:
                    req = req +  ' && (!RegExp("' + ce + '", other.GlueCEUniqueId))'
                pass
        if self.EDG_clock_time:
            if (req == ' '):
                req = req + 'other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time
            else:
                req = req + ' && other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time

        if self.EDG_cpu_time:
            if (req == ' '):
                req = req + ' other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time
            else:
                req = req + ' && other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time
                                                                                           
        if ( self.EDG_retry_count ):               
            to_write = to_write + 'RetryCount = "'+self.EDG_retry_count+'"\n'
            pass

        if ( self.EDG_shallow_retry_count ):               
            to_write = to_write + 'ShallowRetryCount = "'+self.EDG_shallow_retry_count+'"\n'
            pass

        to_write = to_write + 'MyProxyServer = "&quot;' + self.proxyServer + '&quot;"\n'
        to_write = to_write + 'VirtualOrganisation = "&quot;' + self.VO + '&quot;"\n'

        #TaskName   
        dir = string.split(common.work_space.topDir(), '/')
        taskName = dir[len(dir)-2]

        xml.write(str(title))
        #xml.write('<task name="' +str(taskName)+'" sub_path="' +common.work_space.pathForTgz() + 'share/.boss_cache">\n') 

        #xml.write('<task name="' +str(taskName)+ '" sub_path="' +common.work_space.pathForTgz() + 'share/.boss_cache"' + '" task_info="' + os.path.expandvars('X509_USER_PROXY') + '">\n')
        x509_cmd = 'ls /tmp/x509up_u`id -u`'
        x509=runCommand(x509_cmd).strip()
        xml.write('<task name="' +str(taskName)+ '" sub_path="' +common.work_space.pathForTgz() + 'share/.boss_cache"' + ' task_info="' + str(x509) + '">\n')
        xml.write(jt_string)
        
        if (to_write != ''):
            xml.write('<extraTags\n')
            xml.write(to_write)
            xml.write('/>\n')
            pass

        xml.write('<iterator>\n')
        xml.write('\t<iteratorRule name="ITR1">\n')
        xml.write('\t\t<ruleElement> 1:'+ str(nj) + ' </ruleElement>\n')
        xml.write('\t</iteratorRule>\n')
        xml.write('\t<iteratorRule name="ITR2">\n')
        for arg in argsList:
            xml.write('\t\t<ruleElement> <![CDATA[\n'+ arg + '\n\t\t]]> </ruleElement>\n')
            pass
        xml.write('\t</iteratorRule>\n')
        #print jobList
        xml.write('\t<iteratorRule name="ITR3">\n')
        xml.write('\t\t<ruleElement> 1:'+ str(nj) + ':1:6 </ruleElement>\n')
        xml.write('\t</iteratorRule>\n')

        '''
        indy: here itr4
        '''
        
        xml.write('<chain name="' +str(taskName)+'__ITR1_" scheduler="'+str(self.schedulerName)+'">\n') 
       # xml.write('<chain scheduler="'+str(self.schedulerName)+'">\n')
        xml.write(jt_string)

        #executable

        """
        INDY
        script depends on jobType: it should be probably get in a different way
        """        
        script = job.scriptFilename()
        xml.write('<program>\n')
        xml.write('<exec> ' + os.path.basename(script) +' </exec>\n')
        xml.write(jt_string)

        xml.write('<args> <![CDATA[\n _ITR2_ \n]]> </args>\n')
        xml.write('<program_types> crabjob </program_types>\n')
        inp_box = common.work_space.pathForTgz() + 'job/' + jbt.scriptName + ','

        if inp_sandbox != None:
            for fl in inp_sandbox:
                inp_box = inp_box + '' + fl + ','
                pass
            pass

#        if (not jbt.additional_inbox_files == []):
#            inp_box = inp_box + ','
#            for addFile in jbt.additional_inbox_files:
#                #addFile = os.path.abspath(addFile)
#                inp_box = inp_box+''+addFile+','
#                pass

        if inp_box[-1] == ',' : inp_box = inp_box[:-1]
        inp_box = '<infiles> <![CDATA[\n' + inp_box + '\n]]> </infiles>\n'
        xml.write(inp_box)
        
        base = jbt.name()
        stdout = base + '__ITR3_.stdout'
        stderr = base + '__ITR3_.stderr'
        
        xml.write('<stderr> ' + stderr + '</stderr>\n')
        xml.write('<stdout> ' + stdout + '</stdout>\n')
        

        out_box = stdout + ',' + \
                  stderr + ',.BrokerInfo,'

        """
        if int(self.return_data) == 1:
            if out_sandbox != None:
                for fl in out_sandbox:
                    out_box = out_box + '' + fl + ','
                    pass
                pass
            pass
        """

        """
        INDY
        something similar should be also done for infiles (if it makes sense!)
        """
        # Stuff to be returned _always_ via sandbox
        for fl in jbt.output_file_sandbox:
            out_box = out_box + '' + jbt.numberFile_(fl, '_ITR1_') + ','
            pass
        pass

        # via sandbox iif required return_data
        if int(self.return_data) == 1:
            for fl in jbt.output_file:
                out_box = out_box + '' + jbt.numberFile_(fl, '_ITR1_') + ','
                pass
            pass

        if out_box[-1] == ',' : out_box = out_box[:-1]
        out_box = '<outfiles> <![CDATA[\n' + out_box + '\n]]></outfiles>\n'
        xml.write(out_box)
 
        xml.write('<BossAttr> crabjob.INTERNAL_ID=_ITR1_ </BossAttr>\n')

        xml.write('</program>\n')
        xml.write('</chain>\n')

        xml.write('</iterator>\n')
        xml.write('</task>\n')

        xml.close()
       

        return

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
            elif timeLeftLocal<minTimeLeft or timeLeftServer<minTimeLeft:
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

    def configOpt_(self):
        edg_ui_cfg_opt = ' '
        if self.edg_config:
            edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        if self.edg_config_vo: 
            edg_ui_cfg_opt += ' --config-vo ' + self.edg_config_vo + ' '
        return edg_ui_cfg_opt

    def tOut(self, list):
        return 120
 

