from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from EdgConfig import *
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

        try:
            RB = cfg_params["EDG.rb"]
            edgConfig = EdgConfig(RB)
            self.edg_config = edgConfig.config()
            self.edg_config_vo = edgConfig.configVO()
        except KeyError:
            self.edg_config = ''
            self.edg_config_vo = ''

        try:
            self.proxyServer = cfg_params["EDG.proxy_server"]
        except KeyError:
            self.proxyServer = 'myproxy.cern.ch'
        common.logger.debug(5,'Setting myproxy server to '+self.proxyServer)

        try:
            self.role = cfg_params["EDG.role"]
        except KeyError:
            self.role = None
            
        try: self.LCG_version = cfg_params["EDG.lcg_version"]
        except KeyError: self.LCG_version = '2'

        try: self.EDG_requirements = cfg_params['EDG.requirements']
        except KeyError: self.EDG_requirements = ''

        try: self.EDG_retry_count = cfg_params['EDG.retry_count']
        except KeyError: self.EDG_retry_count = ''

        try: 
            self.EDG_ce_black_list = cfg_params['EDG.ce_black_list']
            #print "self.EDG_ce_black_list = ", self.EDG_ce_black_list
        except KeyError: 
            self.EDG_ce_black_list  = ''

        try: 
            self.EDG_ce_white_list = cfg_params['EDG.ce_white_list']
            #print "self.EDG_ce_white_list = ", self.EDG_ce_white_list
        except KeyError: self.EDG_ce_white_list = ''

        try: self.VO = cfg_params['EDG.virtual_organization']
        except KeyError: self.VO = 'cms'

        try: self.return_data = cfg_params['USER.return_data']
        except KeyError: self.return_data = 1

        try:
             self.copy_input_data = common.analisys_common_info['copy_input_data']
             #print "self.copy_input_data = ", self.copy_input_data
        except KeyError: self.copy_input_data = 0

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
           msg = 'Warning: return_data = 0 and copy_data = 0 ==> your exe output will be lost\n' 
           msg = msg + 'Please modify return_data and copy_data value in your crab.cfg file\n' 
           raise CrabException(msg)

        try:
            self.lfc_host = cfg_params['EDG.lfc_host']
        except KeyError:
            msg = "Error. The [EDG] section does not have 'lfc_host' value"
            msg = msg + " it's necessary to know the LFC host name"
            common.logger.message(msg)
            raise CrabException(msg)
        try:
            self.lcg_catalog_type = cfg_params['EDG.lcg_catalog_type']
        except KeyError:
            msg = "Error. The [EDG] section does not have 'lcg_catalog_type' value"
            msg = msg + " it's necessary to know the catalog type"
            common.logger.message(msg)
            raise CrabException(msg)
        try:
            self.lfc_home = cfg_params['EDG.lfc_home']
        except KeyError:
            msg = "Error. The [EDG] section does not have 'lfc_home' value"
            msg = msg + " it's necessary to know the home catalog dir"
            common.logger.message(msg)
            raise CrabException(msg)
      
        try: 
            self.register_data = cfg_params["USER.register_data"]
            if int(self.register_data) == 1:
                try:
                    self.LFN = cfg_params['USER.lfn_dir']
                except KeyError:
                    msg = "Error. The [USER] section does not have 'lfn_dir' value"
                    msg = msg + " it's necessary for LCF registration"
                    common.logger.message(msg)
                    raise CrabException(msg)
        except KeyError: self.register_data = 0

        if ( int(self.copy_data) == 0 and int(self.register_data) == 1 ):
           msg = 'Warning: register_data = 1 must be used with copy_data = 1\n' 
           msg = msg + 'Please modify copy_data value in your crab.cfg file\n' 
           common.logger.message(msg)
           raise CrabException(msg)

        try: self.EDG_requirements = cfg_params['EDG.requirements']
        except KeyError: self.EDG_requirements = ''
                                                                                                                                                             
        try: self.EDG_retry_count = cfg_params['EDG.retry_count']
        except KeyError: self.EDG_retry_count = ''
                                                                                                                                                             
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

        self.proxyValid=0

        try:
            self._taskId = cfg_params['taskId']
        except:
            self._taskId = ''

        try: self.jobtypeName = cfg_params['CRAB.jobtype']
        except KeyError: self.jobtypeName = ''
 
        try: self.schedulerName = cfg_params['CRAB.scheduler']
        except KeyError: self.scheduler = ''

        return
    

    def sched_parameter(self):
        """
        Returns file with scheduler-specific parameters
        """
       
        if (self.edg_config and self.edg_config_vo != ''):
            self.param='sched_param.clad'
            param_file = open(common.work_space.shareDir()+'/'+self.param, 'w')
            param_file.write('RBconfig = "'+self.edg_config+'";\n')   
            param_file.write('RBconfigVO = "'+self.edg_config_vo+'";')
            param_file.close()   
            return 1
        else:
            return 0 

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

        txt += 'echo "middleware discovery " \n'
        txt += 'if [ $GRID3_APP_DIR ]; then\n'
        txt += '    middleware=OSG \n'
        txt += '    echo "SyncCE=`echo $EDG_WL_LOG_DESTINATION`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'elif [ $OSG_APP ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    echo "SyncCE=`echo $EDG_WL_LOG_DESTINATION`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'elif [ $VO_CMS_SW_DIR ]; then \n'
        txt += '    middleware=LCG \n'
        txt += '    echo "SyncCE=`edg-brokerinfo getCE`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'else \n'
        txt += '    echo "SET_CMS_ENV 10030 ==> middleware not identified" \n'
        txt += '    echo "JOB_EXIT_STATUS = 10030" \n'
        txt += '    echo "JobExitCode=10030" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '    rm -f $RUNTIME_AREA/$repo \n'
        txt += '    echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    exit 1 \n'
        txt += 'fi \n'

        txt += '# report first time to DashBoard \n'
        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'
        txt += 'rm -f $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        
        txt += '\n\n'

        if int(self.copy_data) == 1:
           if self.SE:
              txt += 'export SE='+self.SE+'\n'
              txt += 'echo "SE = $SE"\n'
           if self.SE_PATH:
              if ( self.SE_PATH[-1] != '/' ) : self.SE_PATH = self.SE_PATH + '/'
              txt += 'export SE_PATH='+self.SE_PATH+'\n'
              txt += 'echo "SE_PATH = $SE_PATH"\n'

        txt += 'export VO='+self.VO+'\n'
        ### FEDE: add some line for LFC catalog setting 
        txt += 'if [ $middleware == LCG ]; then \n'
        txt += '    if [[ $LCG_CATALOG_TYPE != \''+self.lcg_catalog_type+'\' ]]; then\n'
        txt += '        export LCG_CATALOG_TYPE='+self.lcg_catalog_type+'\n'
        txt += '    fi\n'
        txt += '    if [[ $LFC_HOST != \''+self.lfc_host+'\' ]]; then\n'
        txt += '        export LFC_HOST='+self.lfc_host+'\n'
        txt += '    fi\n'
        txt += '    if [[ $LFC_HOME != \''+self.lfc_home+'\' ]]; then\n'
        txt += '        export LFC_HOME='+self.lfc_home+'\n'
        txt += '    fi\n'
        txt += 'elif [ $middleware == OSG ]; then\n'
        txt += '    echo "LFC catalog setting to be implemented for OSG"\n'
        txt += 'fi\n'
        #####
        if int(self.register_data) == 1:
           txt += 'if [ $middleware == LCG ]; then \n'
           txt += '    export LFN='+self.LFN+'\n'
           txt += '    lfc-ls $LFN\n' 
           txt += '    result=$?\n' 
           txt += '    echo $result\n' 
           ### creation of LFN dir in LFC catalog, under /grid/cms dir  
           txt += '    if [ $result != 0 ]; then\n'
           txt += '       lfc-mkdir $LFN\n'
           txt += '       result=$?\n' 
           txt += '       echo $result\n' 
           txt += '    fi\n'
           txt += 'elif [ $middleware == OSG ]; then\n'
           txt += '    echo " Files registration to be implemented for OSG"\n'
           txt += 'fi\n'
           txt += '\n'

           if self.VO:
              txt += 'export VO='+self.VO+'\n'
           if self.LFN:
              txt += 'if [ $middleware == LCG ]; then \n'
              txt += '    export LFN='+self.LFN+'\n'
              txt += 'fi\n'
              txt += '\n'

        txt += 'if [ $middleware == LCG ]; then\n' 
        txt += '    CloseCEs=`edg-brokerinfo getCE`\n'
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
        txt += '        rm -f $RUNTIME_AREA/$repo \n'
        txt += '        echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '        echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '        exit 1 \n'
        txt += '    fi \n'
        txt += 'fi \n' 

        return txt

    def wsCopyInput(self):
        """
        Copy input data from SE to WN     
        """
        txt = ''
        try:
            self.copy_input_data = common.analisys_common_info['copy_input_data']
        except KeyError: self.copy_input_data = 0
        if int(self.copy_input_data) == 1:
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
        txt = ''
        if int(self.copy_data) == 1:
           txt += '#\n'
           txt += '#   Copy output to SE = $SE\n'
           txt += '#\n'
           txt += '    if [ $middleware == OSG ]; then\n'
           txt += '        echo "X509_USER_PROXY = $X509_USER_PROXY"\n'
           txt += '        echo "source $OSG_APP/glite/setup_glite_ui.sh"\n'
           txt += '        source $OSG_APP/glite/setup_glite_ui.sh\n'
           txt += '        export X509_CERT_DIR=$OSG_APP/glite/etc/grid-security/certificates\n'
           txt += '        echo "export X509_CERT_DIR=$X509_CERT_DIR"\n'
           txt += '    fi \n'
           txt += '    for out_file in $file_list ; do\n'
           txt += '        echo "Trying to copy output file to $SE using lcg-cp"\n'
           if common.logger.debugLevel() >= 5:
               txt += '        echo "lcg-cp --vo $VO -t 2400 --verbose file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file"\n'
               txt += '        exitstring=`lcg-cp --vo $VO -t 2400 --verbose file://\`pwd\`/$out_file gsiftp://${SE}${SE_PATH}$out_file 2>&1`\n'
           else:
               txt += '        echo "lcg-cp --vo $VO -t 2400 file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file"\n'
               txt += '        exitstring=`lcg-cp --vo $VO -t 2400 file://\`pwd\`/$out_file gsiftp://${SE}${SE_PATH}$out_file 2>&1`\n'
           txt += '        copy_exit_status=$?\n'
           txt += '        echo "COPY_EXIT_STATUS for lcg-cp = $copy_exit_status"\n'
           txt += '        echo "STAGE_OUT = $copy_exit_status"\n'
           txt += '        if [ $copy_exit_status -ne 0 ]; then\n'
           txt += '            echo "Possible problem with SE = $SE"\n'
           txt += '            echo "StageOutExitStatus = 198" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '            echo "StageOutExitStatusReason = $exitstring" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '            echo "lcg-cp failed.  For verbose lcg-cp output, use command line option -debug 5."\n'
           txt += '            echo "lcg-cp failed, attempting srmcp"\n'
           txt += '            echo "mkdir -p $HOME/.srmconfig"\n'
           txt += '            mkdir -p $HOME/.srmconfig\n'
           txt += '            if [ $middleware == LCG ]; then\n'
           txt += '               echo "srmcp -retry_num 5 -retry_timeout 480000 file:////`pwd`/$out_file srm://${SE}:8443${SE_PATH}$out_file"\n'
           txt += '               exitstring=`srmcp -retry_num 5 -retry_timeout 480000 file:////\`pwd\`/$out_file srm://${SE}:8443${SE_PATH}$out_file 2>&1`\n'
           txt += '            elif [ $middleware == OSG ]; then\n'
           txt += '               echo "srmcp -retry_num 5 -retry_timeout 240000 -x509_user_trusted_certificates $OSG_APP/glite/etc/grid-security/certificates file:////`pwd`/$out_file srm://${SE}:8443${SE_PATH}$out_file"\n'
           txt += '               exitstring=`srmcp -retry_num 5 -retry_timeout 240000 -x509_user_trusted_certificates $OSG_APP/glite/etc/grid-security/certificates file:////\`pwd\`/$out_file srm://${SE}:8443${SE_PATH}$out_file 2>&1`\n'
           txt += '            fi \n'
           txt += '            copy_exit_status=$?\n'
           txt += '            echo "COPY_EXIT_STATUS for srm = $copy_exit_status"\n'
           txt += '            echo "STAGE_OUT = $copy_exit_status"\n'
           txt += '            if [ $copy_exit_status -ne 0 ]; then\n'
           txt += '               echo "Problems with SE = $SE"\n'
           txt += '               echo "StageOutExitStatus = 198" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '               echo "StageOutExitStatusReason = $exitstring" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '               echo "lcg-cp and srm failed"\n'
           txt += '               echo "If storage_path in your config file contains a ? you may need a \? instead."\n'
           txt += '            else\n'
           txt += '               echo "StageOutSE = $SE" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '               echo "StageOutCatalog = " | tee -a $RUNTIME_AREA/$repo\n'
           txt += '               echo "output copied into $SE/$SE_PATH directory"\n'
           txt += '               echo "StageOutExitStatus = 0" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '               echo "srmcp succeeded"\n'
           txt += '            fi\n'
           txt += '        else\n'
           txt += '            echo "StageOutSE = $SE" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '            echo "StageOutCatalog = " | tee -a $RUNTIME_AREA/$repo\n'
           txt += '            echo "output copied into $SE/$SE_PATH directory"\n'
           txt += '            echo "StageOutExitStatus = 0" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '            echo "lcg-cp succeeded"\n'
           txt += '         fi\n'
           txt += '     done\n'
        return txt

    def wsRegisterOutput(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """

        txt = ''
        if int(self.register_data) == 1:
        ## OLI_Daniele deactivate for OSG (wait for LCG UI installed on OSG)
           txt += 'if [ $middleware == OSG ]; then\n' 
           txt += '   #\n'
           txt += '   #   Register output to LFC deactivated in OSG mode\n'
           txt += '   #\n'
           txt += '   echo "Register output to LFC deactivated in OSG mode"\n'
           txt += 'elif [ $middleware == LCG ]; then \n'
           txt += '#\n'
           txt += '#  Register output to LFC\n'
           txt += '#\n'
           txt += '   if [ $copy_exit_status -eq 0 ]; then\n'
           txt += '      for out_file in $file_list ; do\n'
           txt += '         echo "Trying to register the output file into LFC"\n'
           txt += '         echo "lcg-rf -l $LFN/$out_file --vo $VO -t 1200 sfn://$SE$SE_PATH/$out_file 2>&1"\n'
           txt += '         lcg-rf -l $LFN/$out_file --vo $VO -t 1200 sfn://$SE$SE_PATH/$out_file 2>&1 \n'
           txt += '         register_exit_status=$?\n'
           txt += '         echo "REGISTER_EXIT_STATUS = $register_exit_status"\n'
           txt += '         echo "STAGE_OUT = $register_exit_status"\n'
           txt += '         if [ $register_exit_status -ne 0 ]; then \n'
           txt += '            echo "Problems with the registration to LFC" \n'
           txt += '            echo "Try with srm protocol" \n'
           txt += '            echo "lcg-rf -l $LFN/$out_file --vo $VO -t 1200 srm://$SE$SE_PATH/$out_file 2>&1"\n'
           txt += '            lcg-rf -l $LFN/$out_file --vo $VO -t 1200 srm://$SE$SE_PATH/$out_file 2>&1 \n'
           txt += '            register_exit_status=$?\n'
           txt += '            echo "REGISTER_EXIT_STATUS = $register_exit_status"\n'
           txt += '            echo "STAGE_OUT = $register_exit_status"\n'
           txt += '            if [ $register_exit_status -ne 0 ]; then \n'
           txt += '               echo "Problems with the registration into LFC" \n'
           txt += '            fi \n'
           txt += '         else \n'
           txt += '            echo "output registered to LFC"\n'
           txt += '         fi \n'
           txt += '         echo "StageOutExitStatus = $register_exit_status" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '      done\n'
           txt += '   else \n'
           txt += '      echo "Trying to copy output file to CloseSE"\n'
           txt += '      CLOSE_SE=`edg-brokerinfo getCloseSEs | head -1`\n'
           txt += '      for out_file in $file_list ; do\n'
           txt += '         echo "lcg-cr -v -l lfn:${LFN}/$out_file -d $CLOSE_SE -P $LFN/$out_file --vo $VO file://$RUNTIME_AREA/$out_file 2>&1" \n'
           txt += '         lcg-cr -v -l lfn:${LFN}/$out_file -d $CLOSE_SE -P $LFN/$out_file --vo $VO file://$RUNTIME_AREA/$out_file 2>&1 \n'
           txt += '         register_exit_status=$?\n'
           txt += '         echo "REGISTER_EXIT_STATUS = $register_exit_status"\n'
           txt += '         echo "STAGE_OUT = $register_exit_status"\n'
           txt += '         if [ $register_exit_status -ne 0 ]; then \n'
           txt += '            echo "Problems with CloseSE or Catalog" \n'
           txt += '         else \n'
           txt += '            echo "The program was successfully executed"\n'
           txt += '            echo "SE = $CLOSE_SE"\n'
           txt += '            echo "LFN for the file is LFN=${LFN}/$out_file"\n'
           txt += '         fi \n'
           txt += '         echo "StageOutExitStatus = $register_exit_status" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '      done\n'
           txt += '   fi \n'
           txt += '   exit_status=$register_exit_status\n'
           txt += 'fi \n'
        return txt

    def loggingInfo(self, id):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        self.checkProxy()
        cmd = 'edg-job-get-logging-info -v 2 ' + id
        #cmd_out = os.popen(cmd) 
        cmd_out = runCommand(cmd)
        return cmd_out

    def getExitStatus(self, id):
        return self.getStatusAttribute_(id, 'exit_code')

    def queryStatus(self, id):
        return self.getStatusAttribute_(id, 'status')

    def queryDest(self, id):  
        return self.getStatusAttribute_(id, 'destination')


    def getStatusAttribute_(self, id, attr):
        """ Query a status of the job with id """

        self.checkProxy()
        hstates = {}
        Status = importName('edg_wl_userinterface_common_LbWrapper', 'Status')
        # Bypass edg-job-status interfacing directly to C++ API
        # Job attribute vector to retrieve status without edg-job-status
        level = 0
        # Instance of the Status class provided by LB API
        jobStat = Status()
        st = 0
        jobStat.getStatus(id, level)
        err, apiMsg = jobStat.get_error()
        if err:
            common.logger.debug(5,'Error caught' + apiMsg) 
            return None
        else:
            for i in range(len(self.states)):
                # Fill an hash table with all information retrieved from LB API
                hstates[ self.states[i] ] = jobStat.loadStatus(st)[i]
            result = jobStat.loadStatus(st)[self.states.index(attr)]
            return result

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """
        cmd = 'edg-job-status '+id
        cmd_out = runCommand(cmd)
        return cmd_out

    ##### FEDE ######         
    def findSites_(self, n_tot_job):
        itr4 = []
       # print "n_tot_job = ", n_tot_job
        for n in range(n_tot_job):
            sites = common.jobDB.destination(n)
            if len(sites)>0 and sites[0]=="Any": continue

            #job = common.job_list[n]
            #jbt = job.type()
           # print "common.jobDB.destination(n) = ", common.jobDB.destination(n)
           # print "sites = ", sites
            itr = ''
            if sites != [""]:#CarlosDaniele 
                for site in sites: 
                    #itr = itr + 'target.GlueSEUniqueID==&quot;'+site+'&quot; || '
                    itr = itr + 'target.GlueSEUniqueID=="'+site+'" || '
                    pass
                # remove last ||
                itr = itr[0:-4]
                itr4.append( itr )
        # remove last , 
       # print "itr4 = ", itr4
        return itr4

    def createXMLSchScript(self, nj, argsList):
   # def createXMLSchScript(self, nj):
        """
        Create a XML-file for BOSS4.
        """
  #      job = common.job_list[nj]
        """
        INDY
        [begin] da rivedere:
        in particolare passerei il jobType ed eliminerei le dipendenze da job
        """
        index = nj - 1
        job = common.job_list[index]
        jbt = job.type()
        
        inp_sandbox = jbt.inputSandbox(index)
        out_sandbox = jbt.outputSandbox(index)
        """
        [end] da rivedere
        """

        
        title = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
        jt_string = ''
        
        xml_fname = str(self.jobtypeName)+'.xml'
        xml = open(common.work_space.shareDir()+'/'+xml_fname, 'a')

        #TaskName   
        dir = string.split(common.work_space.topDir(), '/')
        taskName = dir[len(dir)-2]
  
        to_writeReq = ''
        to_write = ''

        req=' '
        req = req + jbt.getRequirements()


        #sites = common.jobDB.destination(nj)
        #if len(sites)>0 and sites[0]!="Any":
        #    req = req + ' && anyMatch(other.storage.CloseSEs, (_ITR4_))'
        #req = req     
    
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

        to_write = to_write + 'MyProxyServer = "&quot;' + self.proxyServer + '&quot;"\n'
        to_write = to_write + 'VirtualOrganisation = "&quot;' + self.VO + '&quot;"\n'


        #TaskName   
        dir = string.split(common.work_space.topDir(), '/')
        taskName = dir[len(dir)-2]

        xml.write(str(title))
        xml.write('<task name="' +str(taskName)+'">\n')
        xml.write(jt_string)

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
        indy: qui sotto ci sta itr4
        '''
        
        itr4=self.findSites_(nj)
        #print "--->>> itr4 = ", itr4
        if (itr4 != []):
            xml.write('\t<iteratorRule name="ITR4">\n')
        #print argsList
            for arg in itr4:
                xml.write('\t\t<ruleElement> <![CDATA[\n'+ arg + '\n\t\t]]> </ruleElement>\n')
                pass
            xml.write('\t</iteratorRule>\n')
            req = req + ' && anyMatch(other.storage.CloseSEs, (_ITR4_))'
            pass
        #    print "--->>> req= ", req         
        
        if (to_write != ''):
            xml.write('<extraTags\n')
            xml.write(to_write)
            xml.write('/>\n')
            pass

        xml.write('<chain scheduler="'+str(self.schedulerName)+'">\n')
        xml.write(jt_string)

        if (req != ' '):
            req = req + '\n'
            xml.write('<extraTags>\n')
            xml.write('<Requirements>\n')
            xml.write('<![CDATA[\n')
            xml.write(req)
            xml.write(']]>\n')
            xml.write('</Requirements>\n')
            xml.write('</extraTags>\n')
            pass

        #executable

        """
        INDY
        script dipende dal jobType: dovrebbe essere semplice tirarlo fuori in altro modo
        """        
        script = job.scriptFilename()
        xml.write('<program>\n')
        xml.write('<exec> ' + os.path.basename(script) +' </exec>\n')
        xml.write(jt_string)
    
           
        ### only one .sh  JDL has arguments:
        ### Fabio
#        xml.write('args = "' + str(nj+1)+' '+ jbt.getJobTypeArguments(nj, "EDG") +'"\n')
        xml.write('<args> <![CDATA[\n _ITR2_ \n]]> </args>\n')
        xml.write('<program_types> crabjob </program_types>\n')
        inp_box = script + ','

        if inp_sandbox != None:
            for fl in inp_sandbox:
                inp_box = inp_box + '' + fl + ','
                pass
            pass

        inp_box = inp_box + os.path.abspath(os.environ['CRABDIR']+'/python/'+'report.py') + ',' +\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'DashboardAPI.py') + ','+\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'Logger.py') + ','+\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'ProcInfo.py') + ','+\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'apmon.py') 

        if (not jbt.additional_inbox_files == []):
            inp_box = inp_box + ','
            for addFile in jbt.additional_inbox_files:
                addFile = os.path.abspath(addFile)
                inp_box = inp_box+''+addFile+','
                pass

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
        qualcosa del genere andrebbe fatta per gli infiles
        """        
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
        timeleft = -999
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
            common.logger.message( "No valid proxy found or remaining time of validity of already existing proxy shorter than 10 hours!\n Creating a user proxy with default length of 96h\n")
            cmd = 'voms-proxy-init -voms '+self.VO+' -valid 96:00'
            if self.role:
                cmd = 'voms-proxy-init -voms '+self.VO+':/'+self.VO+'/role='+self.role+' -valid 96:00'
            try:
                # SL as above: damn it!
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
            # if myproxy exist but not long enough, renew
            reTime = re.compile( r'timeleft: (\d+)' )
            #print "<"+str(reTime.search( cmd_out ).group(1))+">"
            if reTime.match( cmd_out ):
                time = reTime.search( line ).group(1)
                if time < minTimeLeftServer:
                    renewProxy = 1
                    common.logger.message('No credential delegation will expire in '+time+' hours: renew it')
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
