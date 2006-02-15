from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
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

        try: self.edg_config = cfg_params["EDG.config"]
        except KeyError: self.edg_config = ''

        try: self.edg_config_vo = cfg_params["EDG.config_vo"]
        except KeyError: self.edg_config_vo = ''

        try: self.LCG_version = cfg_params["EDG.lcg_version"]
        except KeyError: self.LCG_version = '2'

        try: self.EDG_requirements = cfg_params['EDG.requirements']
        except KeyError: self.EDG_requirements = ''

        try: self.EDG_retry_count = cfg_params['EDG.retry_count']
        except KeyError: self.EDG_retry_count = ''

        try: self.VO = cfg_params['EDG.virtual_organization']
        except KeyError: self.VO = 'cms'

        try: self.return_data = cfg_params['USER.return_data']
        except KeyError: self.return_data = 1

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
            self.register_data = cfg_params["USER.register_data"]
            if int(self.register_data) == 1:
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
        if int(self.copy_data) == 1:
           if self.SE:
              txt += 'export SE='+self.SE+'\n'
              txt += 'echo "SE = $SE"\n'
           if self.SE_PATH:
              if ( self.SE_PATH[-1] != '/' ) : self.SE_PATH = self.SE_PATH + '/'
              txt += 'export SE_PATH='+self.SE_PATH+'\n'
              txt += 'echo "SE_PATH = $SE_PATH"\n'
                                                                                                                                                             
        if int(self.register_data) == 1:
           txt += 'export VO='+self.VO+'\n'
           ### FEDE: add some line for LFC catalog setting 
           txt += 'if [[ $LCG_CATALOG_TYPE != \''+self.lcg_catalog_type+'\' ]]; then\n'
           txt += '   export LCG_CATALOG_TYPE='+self.lcg_catalog_type+'\n'
           txt += 'fi\n'
           txt += 'if [[ $LFC_HOST != \''+self.lfc_host+'\' ]]; then\n'
           txt += 'export LFC_HOST='+self.lfc_host+'\n'
           txt += 'fi\n'
           txt += 'if [[ $LFC_HOME != \''+self.lfc_home+'\' ]]; then\n'
           txt += 'export LFC_HOME='+self.lfc_home+'\n'
           txt += 'fi\n'
           #####
           txt += 'export LFN='+self.LFN+'\n'
           txt += 'lfc-ls $LFN\n' 
           txt += 'result=$?\n' 
           txt += 'echo $result\n' 
           ### creation of LFN dir in LFC catalog, under /grid/cms dir  
           txt += 'if [ $result != 0 ]; then\n'
           txt += '   lfc-mkdir $LFN\n'
           txt += '   result=$?\n' 
           txt += '   echo $result\n' 
           txt += 'fi\n'
           txt += '\n'
        txt += 'CloseCEs=`edg-brokerinfo getCE`\n'
        txt += 'echo "CloseCEs = $CloseCEs"\n'
        txt += 'CE=`echo $CloseCEs | sed -e "s/:.*//"`\n'
        txt += 'echo "CE = $CE"\n'
        return txt

    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        txt = ''
        if int(self.copy_data) == 1:
           copy = 'globus-url-copy file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file'
           txt += '#\n'
           txt += '#   Copy output to SE = $SE\n'
           txt += '#\n'
           txt += 'if [ $exe_result -eq 0 ]; then\n'
           txt += '    for out_file in $file_list ; do\n'
           txt += '        echo "Trying to copy output file to $SE "\n'
           txt += '        echo "lcg-cp --vo cms -t 30 file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file"\n'
#           txt += '        echo "globus-url-copy file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file"\n'
           txt += '        exitstring=`lcg-cp --vo cms -t 30 file://\`pwd\`/$out_file gsiftp://${SE}${SE_PATH}$out_file 2>&1`\n'
#           txt += '        exitstring=`globus-url-copy file://\`pwd\`/$out_file gsiftp://${SE}${SE_PATH}$out_file 2>&1`\n'
           txt += '        copy_exit_status=$?\n'
           txt += '        echo "COPY_EXIT_STATUS = $copy_exit_status"\n'
           txt += '        echo "STAGE_OUT = $copy_exit_status"\n'
           txt += '        if [ $copy_exit_status -ne 0 ]; then\n'
           txt += '            echo "Problems with SE = $SE"\n'
           txt += '            echo "StageOutExitStatus = 198" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '            echo "StageOutExitStatusReason = $exitstring" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '        else\n'
           txt += '            echo "StageOutSE = $SE" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '            echo "StageOutCatalog = " | tee -a $RUNTIME_AREA/$repo\n'
           txt += '            echo "output copied into $SE/$SE_PATH directory"\n'
           txt += '            echo "StageOutExitStatus = 0" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '         fi\n'
           txt += '     done\n'
           txt += 'fi\n'
        return txt

    def wsRegisterOutput(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """

        txt = ''
        if int(self.register_data) == 1:
           txt += '#\n'
           txt += '#  Register output to LFC\n'
           txt += '#\n'
           txt += 'if [[ $exe_result -eq 0 && $copy_exit_status -eq 0 ]]; then\n'
           txt += '   for out_file in $file_list ; do\n'
           txt += '      echo "Trying to register the output file into LFC"\n'
           txt += '      echo "lcg-rf -l $LFN/$out_file --vo $VO sfn://$SE$SE_PATH/$out_file"\n'
           txt += '      lcg-rf -l $LFN/$out_file --vo $VO sfn://$SE$SE_PATH/$out_file 2>&1 \n'
           txt += '      register_exit_status=$?\n'
           txt += '      echo "REGISTER_EXIT_STATUS = $register_exit_status"\n'
           txt += '      echo "STAGE_OUT = $register_exit_status"\n'
           txt += '      if [ $register_exit_status -ne 0 ]; then \n'
           txt += '         echo "Problems with the registration to LFC" \n'
           txt += '         echo "Try with srm protocol" \n'
           txt += '         echo "lcg-rf -l $LFN/$out_file --vo $VO srm://$SE$SE_PATH/$out_file"\n'
           txt += '         lcg-rf -l $LFN/$out_file --vo $VO srm://$SE$SE_PATH/$out_file 2>&1 \n'
           txt += '         register_exit_status=$?\n'
           txt += '         echo "REGISTER_EXIT_STATUS = $register_exit_status"\n'
           txt += '         echo "STAGE_OUT = $register_exit_status"\n'
           txt += '         if [ $register_exit_status -ne 0 ]; then \n'
           txt += '            echo "Problems with the registration into LFC" \n'
           txt += '         fi \n'
           txt += '      else \n'
           txt += '         echo "output registered to LFC"\n'
           txt += '      fi \n'
           txt += '      echo "StageOutExitStatus = $register_exit_status" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '   done\n'
           txt += 'elif [[ $exe_result -eq 0 && $copy_exit_status -ne 0 ]]; then \n'
           txt += '   echo "Trying to copy output file to CloseSE"\n'
           txt += '   CLOSE_SE=`edg-brokerinfo getCloseSEs | head -1`\n'
           txt += '   for out_file in $file_list ; do\n'
           txt += '      echo "lcg-cr -v -l lfn:${LFN}/$out_file -d $CLOSE_SE -P $LFN/$out_file --vo $VO file://`pwd`/$out_file" \n'
           txt += '      lcg-cr -v -l lfn:${LFN}/$out_file -d $CLOSE_SE -P $LFN/$out_file --vo $VO file://`pwd`/$out_file 2>&1 \n'
           txt += '      register_exit_status=$?\n'
           txt += '      echo "REGISTER_EXIT_STATUS = $register_exit_status"\n'
           txt += '      echo "STAGE_OUT = $register_exit_status"\n'
           txt += '      if [ $register_exit_status -ne 0 ]; then \n'
           txt += '         echo "Problems with CloseSE" \n'
           txt += '      else \n'
           txt += '         echo "The program was successfully executed"\n'
           txt += '         echo "SE = $CLOSE_SE"\n'
           txt += '         echo "LFN for the file is LFN=${LFN}/$out_file"\n'
           txt += '      fi \n'
           txt += '      echo "StageOutExitStatus = $register_exit_status" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '   done\n'
           txt += 'else\n'
           txt += '   echo "Problem with the executable"\n'
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

    def listMatch(self, nj):
        """
        Check the compatibility of available resources
        """
        self.checkProxy()
        jdl = common.job_list[nj].jdlFilename()
        cmd = 'edg-job-list-match ' + self.configOpt_() + jdl 
        cmd_out = runCommand(cmd,0,10)
        return self.parseListMatch_(cmd_out, jdl)

    def parseListMatch_(self, out, jdl):
        """
        Parse the f* output of edg-list-match and produce something sensible
        """
        reComment = re.compile( r'^\**$' )
        reEmptyLine = re.compile( r'^$' )
        reVO = re.compile( r'Selected Virtual Organisation name.*' )
        reLine = re.compile( r'.*')
        reCE = re.compile( r'(.*:.*)')
        reCEId = re.compile( r'CEId.*')
        reNO = re.compile( r'No Computing Element matching' )
        reRB = re.compile( r'Connecting to host' )
        next = 0
        CEs=[]
        Match=0

        #print out
        lines = reLine.findall(out)

        i=0
        CEs=[]
        for line in lines:
            string.strip(line)
            #print line
            if reNO.match( line ):
                common.logger.debug(5,line)
                return 0
                pass
            if reVO.match( line ):
                VO =reVO.match( line ).group()
                common.logger.debug(5,"VO "+VO)
                pass

            if reRB.match( line ):
                RB = reRB.match(line).group()
                common.logger.debug(5,"RB "+RB)
                pass

            if reCEId.search( line ):
                for lineCE in lines[i:-1]:
                    if reCE.match( lineCE ):
                        CE = string.strip(reCE.search(lineCE).group(1))
                        CEs.append(CE.split(':')[0])
                        pass 
                    pass
                pass
            i=i+1
            pass

        common.logger.debug(5,"All CE :"+str(CEs))

        sites = []
        [sites.append(it) for it in CEs if not sites.count(it)]

        common.logger.debug(5,"All Sites :"+str(sites))
        return len(sites)

    def noMatchFound_(self, jdl):
        reReq = re.compile( r'Requirements' )
        reString = re.compile( r'"\S*"' )
        f = file(jdl,'r')
        for line in f.readlines():
            line= line.strip()
            if reReq.match(line):
                for req in reString.findall(line):
                    if re.search("VO",req):
                        common.logger.message( "SW required: "+req)
                        continue
                    if re.search('"\d+',req):
                        common.logger.message("Other req  : "+req)
                        continue
                    common.logger.message( "CE required: "+req)
                break
            pass
        raise CrabException("No compatible resources found!")

    def submit(self, nj):
        """
        Submit one EDG job.
        """

        self.checkProxy()
        jid = None
        jdl = common.job_list[nj].jdlFilename()

        cmd = 'edg-job-submit ' + self.configOpt_() + jdl 
        cmd_out = runCommand(cmd)
        if cmd_out != None:
            reSid = re.compile( r'https.+' )
            jid = reSid.search(cmd_out).group()
            pass
        return jid

    def resubmit(self, nj_list):
        """
        Prepare jobs to be submit
        """
        return

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
            result = jobStat.loadStatus(st)[ self.states.index(attr) ]
            return result

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """
        cmd = 'edg-job-status '+id
        cmd_out = runCommand(cmd)
        return cmd_out

    def getOutput(self, id):
        """
        Get output for a finished job with id.
        Returns the name of directory with results.
        """

        self.checkProxy()
        cmd = 'edg-job-get-output --dir ' + common.work_space.resDir() + ' ' + id
        cmd_out = runCommand(cmd)

        # Determine the output directory name
        dir = common.work_space.resDir()
        dir += os.getlogin()
        dir += '_' + os.path.basename(id)
        return dir

    def cancel(self, id):
        """ Cancel the EDG job with id """
        self.checkProxy()
        cmd = 'edg-job-cancel --noint ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

    def createSchScript(self, nj):
        """
        Create a JDL-file for EDG.
        """

        job = common.job_list[nj]
        jbt = job.type()
        inp_sandbox = jbt.inputSandbox(nj)
        out_sandbox = jbt.outputSandbox(nj)
        inp_storage_subdir = ''
        
        title = '# This JDL was generated by '+\
                common.prog_name+' (version '+common.prog_version_str+')\n'
        jt_string = ''


        
        SPL = inp_storage_subdir
        if ( SPL and SPL[-1] != '/' ) : SPL = SPL + '/'

        jdl_fname = job.jdlFilename()
        jdl = open(jdl_fname, 'w')
        jdl.write(title)

        script = job.scriptFilename()
        jdl.write('Executable = "' + os.path.basename(script) +'";\n')
        jdl.write(jt_string)

        ### only one .sh  JDL has arguments:
        firstEvent = common.jobDB.firstEvent(nj)
        maxEvents = common.jobDB.maxEvents(nj)
        jdl.write('Arguments = "' + str(nj+1)+' '+str(firstEvent)+' '+str(maxEvents)+'";\n')

        inp_box = 'InputSandbox = { '
        inp_box = inp_box + '"' + script + '",'

        if inp_sandbox != None:
            for fl in inp_sandbox:
                inp_box = inp_box + ' "' + fl + '",'
                pass
            pass

        #if common.use_jam:
        #   inp_box = inp_box+' "'+common.bin_dir+'/'+common.run_jam+'",'
        # Marco (VERY TEMPORARY ML STUFF)
        inp_box = inp_box+' "' + os.path.abspath(os.environ['CRABDIR']+'/python/'+'report.py') + '", "' +\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'Logger.py') + '", "'+\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'ProcInfo.py') + '", "'+\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'apmon.py') + '"'
        # End Marco

        for addFile in jbt.additional_inbox_files:
            addFile = os.path.abspath(addFile)
            inp_box = inp_box+' "'+addFile+'",'
            pass

        if inp_box[-1] == ',' : inp_box = inp_box[:-1]
        inp_box = inp_box + ' };\n'
        jdl.write(inp_box)

        jdl.write('StdOutput     = "' + job.stdout() + '";\n')
        jdl.write('StdError      = "' + job.stderr() + '";\n')
        
        
        if job.stdout() == job.stderr():
          out_box = 'OutputSandbox = { "' + \
                    job.stdout() + '", ".BrokerInfo",'
        else:
          out_box = 'OutputSandbox = { "' + \
                    job.stdout() + '", "' + \
                    job.stderr() + '", ".BrokerInfo",'

        if int(self.return_data) == 1:
            if out_sandbox != None:
                for fl in out_sandbox:
                    out_box = out_box + ' "' + fl + '",'
                    pass
                pass
            pass
                                                                                                                                                             
        if out_box[-1] == ',' : out_box = out_box[:-1]
        out_box = out_box + ' };'
        jdl.write(out_box+'\n')

        ### if at least a CE exists ...
        if common.analisys_common_info['sites']:
            if common.analisys_common_info['sw_version']:
                req='Requirements = '
                req=req + 'Member("VO-cms-' + \
                     common.analisys_common_info['sw_version'] + \
                     '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'
            if len(common.analisys_common_info['sites'])>0:
                req = req + ' && ('
                for i in range(len(common.analisys_common_info['sites'])):
                    req = req + 'other.GlueCEInfoHostName == "' \
                         + common.analisys_common_info['sites'][i] + '"'
                    if ( i < (int(len(common.analisys_common_info['sites']) - 1)) ):
                        req = req + ' || '
            req = req + ')'

            #### and USER REQUIREMENT
            if self.EDG_requirements:
                req = req +  ' && ' + self.EDG_requirements
            if self.EDG_clock_time:
                req = req + ' && other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time
            if self.EDG_cpu_time:
                req = req + ' && other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time
            req = req + ';\n'
            jdl.write(req)
                                                                                                                                                             
        jdl.write('VirtualOrganisation = "' + self.VO + '";\n')

        if ( self.EDG_retry_count ):               
            jdl.write('RetryCount = '+self.EDG_retry_count+';\n')
            pass

        jdl.close()
        return

    def checkProxy(self):
        """
        Function to check the Globus proxy.
        """
        if (self.proxyValid): return
        timeleft = -999
        minTimeLeft=10 # in hours
        cmd = 'grid-proxy-info -e -v '+str(minTimeLeft)+':00'
        # SL Here I have to use os.system since the stupid command exit with >0 if no valid proxy is found
        cmd_out = os.system(cmd)
        if (cmd_out>0):
            common.logger.message( "No valid proxy found or timeleft too short!\n Creating a user proxy with default length of 100h\n")
            cmd = 'grid-proxy-init -valid 100:00'
            try:
                # SL as above: damn it!
                out = os.system(cmd)
                if (out>0): raise CrabException("Unable to create a valid proxy!\n")
            except:
                msg = "Unable to create a valid proxy!\n"
                raise CrabException(msg)
            # cmd = 'grid-proxy-info -timeleft'
            # cmd_out = runCommand(cmd,0,20)
            pass
        self.proxyValid=1
        return
    
    def configOpt_(self):
        edg_ui_cfg_opt = ' '
        if self.edg_config:
          edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        if self.edg_config_vo: 
          edg_ui_cfg_opt += ' --config-vo ' + self.edg_config_vo + ' '
        return edg_ui_cfg_opt
