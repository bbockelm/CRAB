from Scheduler import Scheduler
from JobList import JobList
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from GridCatService import GridCatHostService
import time
import common
import popen2

import os, sys, time

class SchedulerCondor_g(Scheduler):
    def __init__(self):
        Scheduler.__init__(self,"CONDOR_G")
        self.states = [ "Acl", "cancelReason", "cancelling","ce_node","children", \
                      "children_hist","children_num","children_states","condorId","condor_jdl", \
                      "cpuTime","destination", "done_code","exit_code","expectFrom", \
                      "expectUpdate","globusId","jdl","jobId","jobtype", \
                      "lastUpdateTime","localId","location", "matched_jdl","network_server", \
                      "owner","parent_job", "reason","resubmitted","rsl","seed",\
                      "stateEnterTime","stateEnterTimes","subjob_failed", \
                      "user tags" , "status" , "status_code","hierarchy"]

        # check for locally running condor scheduler
        cmd = 'ps xau | grep -i condor_schedd | grep -v grep'
        cmd_out = runCommand(cmd)
        if cmd_out == None:
            msg  = '[Condor-G Scheduler]: condor_schedd is not running on this machine.\n'
            msg += '[Condor-G Scheduler]: Please use another machine with installed condor and running condor_schedd or change the Scheduler in your crab.cfg.'
            common.logger.message(msg)
            raise CrabException(msg)

        self.checkExecutableInPath('condor_q')
        self.checkExecutableInPath('condor_submit')
        self.checkExecutableInPath('condor_version')

        # get version number
        cmd = 'condor_version'
        cmd_out = runCommand(cmd)
        if cmd != None :
            tmp = cmd_out.find('CondorVersion') + 15
            version = cmd_out[tmp:tmp+6].split('.')
            version_master = int(version[0])
            version_major  = int(version[1])
            version_minor  = int(version[2])
        else :
            msg  = '[Condor-G Scheduler]: condor_version was not able to determine the installed condor version.\n'
            msg += '[Condor-G Scheduler]: Please use another machine with properly installed condor or change the Scheduler in your crab.cfg.'
            common.logger.message(msg)
            raise CrabException(msg)

        self.checkExecutableInPath('condor_config_val')

        self.checkCondorVariablePointsToFile('GRIDMANAGER')

        if version_master >= 6 and version_major >= 7 and version_minor >= 11 :
            self.checkCondorVariablePointsToFile('GT2_GAHP')
        elif version_master >=6 and version_major < 8 :
            self.checkCondorVariablePointsToFile('GAHP')

        self.checkCondorVariablePointsToFile('GRID_MONITOR')

        self.checkCondorVariableIsTrue('ENABLE_GRID_MONITOR')

        max_submit = self.queryCondorVariable('GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE',100).strip()
        max_pending = self.queryCondorVariable('GRIDMANAGER_MAX_PENDING_SUBMITS_PER_RESOURCE','Unlimited').strip()

        msg  = '[Condor-G Scheduler]\n'
        msg += 'Maximal number of jobs submitted to the grid   : GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE  = '+max_submit+'\n'
        msg += 'Maximal number of parallel submits to the grid : GRIDMANAGER_MAX_PENDING_SUBMITS_PER_RESOURCE = '+max_pending+'\n'
        msg += 'Ask the administrator of your local condor installation to increase these variables to enable more jobs to be executed on the grid in parallel.\n'
        common.logger.debug(2,msg)

        # Very bad. Needed to get CE from the SE provided by DLS.
        # GridCat currently doesn't have the capability to provide this.
        self.mapSEtoCE = {"cmssrm.hep.wisc.edu":"cmsgrid02.hep.wisc.edu", \
                          "dcache.rcac.purdue.edu":"lepton.rcac.purdue.edu", \
                          "ufdcache.phys.ufl.edu":"ufloridapg.phys.ufl.edu", \
                          "thpc-1.unl.edu":"red.unl.edu", \
                          "cithep59.ultralight.org":"cit-gatekeeper.ultralight.org", \
                          "t2data2.t2.ucsd.edu":"osg-gw-2.t2.ucsd.edu", \
                          "cmssrm.fnal.gov":"cmsosgce.fnal.gov", \
                          "se01.cmsaf.mit.edu":"ce01.cmsaf.mit.edu", \
                          "spraid.if.usp.br":"spgrid.if.usp.br"}
        
        return

    def checkExecutableInPath(self, name):
        # check if executable is in PATH
        cmd = 'which '+name
        cmd_out = runCommand(cmd)
        if cmd_out == None:
            msg  = '[Condor-G Scheduler]: '+name+' is not in the $PATH on this machine.\n'
            msg += '[Condor-G Scheduler]: Please use another machine with installed condor or change the Scheduler in your crab.cfg.'
            common.logger.message(msg)
            raise CrabException(msg)

    def checkCondorVariablePointsToFile(self, name):
        ## check for condor variable
        cmd = 'condor_config_val '+name
        cmd_out = runCommand(cmd)
        if os.path.isfile(cmd_out) > 0 :
            msg  = '[Condor-G Scheduler]: the variable '+name+' is not properly set for the condor installation on this machine.\n'
            msg += '[Condor-G Scheduler]: Please ask the administrator of the local condor installation to set the variable '+name+' properly,',
            'use another machine with properly installed condor or change the Scheduler in your crab.cfg.'
            common.logger.message(msg)
            raise CrabException(msg)

    def checkCondorVariableIsTrue(self, name):
        ## check for condor variable
        cmd = 'condor_config_val '+name
        cmd_out = runCommand(cmd)
        if cmd_out == 'TRUE' :
            msg  = '[Condor-G Scheduler]: the variable '+name+' is not set to true for the condor installation on this machine.\m'
            msg += '[Condor-G Scheduler]: Please ask the administrator of the local condor installation to set the variable '+name+' to true,',
            'use another machine with properly installed condor or change the Scheduler in your crab.cfg.'
            common.logger.message(msg)
            raise CrabException(msg)

    def queryCondorVariable(self, name, default):
        ## check for condor variable
        cmd = 'condor_config_val '+name
        out = popen2.Popen3(cmd,1)
        exit_code = out.wait()
        cmd_out = out.fromchild.readline().strip()
        if exit_code != 0 :
            cmd_out = str(default)

        return cmd_out

    def configure(self, cfg_params):

        try:
            self.role = cfg_params["EDG.role"]
        except KeyError:
            self.role = None

        self.copy_input_data = 0

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
      
        try: self.VO = cfg_params['EDG.virtual_organization']
        except KeyError: self.VO = 'cms'

        try: self.EDG_clock_time = cfg_params['EDG.max_wall_clock_time']
        except KeyError: self.EDG_clock_time= ''
                                                                                                                                                             
        self.register_data = 0

        # check if one and only one entry is in $CE_WHITELIST

        try:
            tmpGood = string.split(cfg_params['EDG.ce_white_list'],',')
        except KeyError:
            msg  = '[Condor-G Scheduler]: destination site is not selected properly.\n'
            msg += '[Condor-G Scheduler]: Please select your destination site and only your destination site in the CE_white_list variable of the [EDG] section in your crab.cfg.'
            common.logger.message(msg)
            raise CrabException(msg)
            
        if len(tmpGood) != 1 :
            msg  = '[Condor-G Scheduler]: destination site is not selected properly.\n'
            msg += '[Condor-G Scheduler]: Please select your destination site and only your destination site in the CE_white_list variable of the [EDG] section in your crab.cfg.'
            common.logger.message(msg)
            raise CrabException(msg)

        try:
            self.UseGT4 = cfg_params['USER.use_gt_4'];
        except KeyError:
            self.UseGT4 = 0;

        self.proxyValid=0
        # added here because checklistmatch is not used
        self.checkProxy()

        self._taskId = cfg_params['taskId']
                
        try: self.jobtypeName = cfg_params['CRAB.jobtype']
        except KeyError: self.jobtypeName = ''
 
        try: self.schedulerName = cfg_params['CRAB.scheduler']
        except KeyError: self.scheduler = ''

        return
    

    def sched_parameter(self):
        """
        Returns file with scheduler-specific parameters
        """
        return 0
       
    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        txt = ''

        txt = ''
        txt += '# strip arguments\n'
        txt += 'echo "strip arguments"\n'
        txt += 'args=("$@")\n'
        txt += 'nargs=$#\n'
        txt += 'shift $nargs\n'
        txt += "# job number (first parameter for job wrapper)\n"
        txt += "NJob=${args[0]}\n"

        # create hash of cfg file
        hash = makeCksum(common.work_space.cfgFileName())
        txt += 'MonitorJobID=`echo ${NJob}_'+hash+'_$GLOBUS_GRAM_JOB_CONTACT`\n'
        txt += 'SyncGridJobId=`echo $GLOBUS_GRAM_JOB_CONTACT`\n'
        txt += 'MonitorID=`echo ' + self._taskId + '`\n'
        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=`echo $SyncGridJobId`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'

        txt += 'echo "middleware discovery " \n'
        txt += 'if [ $GRID3_APP_DIR ]; then\n'
        txt += '    middleware=OSG \n'
        txt += '    echo "SyncCE=`echo $GLOBUS_GRAM_JOB_CONTACT | cut -d/ -f3 | cut -d: -f1`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'elif [ $OSG_APP ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    echo "SyncCE=`echo $GLOBUS_GRAM_JOB_CONTACT | cut -d/ -f3 | cut -d: -f1`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'elif [ $VO_CMS_SW_DIR ]; then\n'
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
        txt += 'fi\n'

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
        txt += 'CE=${args[3]}\n'
        txt += 'echo "CE = $CE"\n'
        return txt

    def wsCopyInput(self):
        """
        Copy input data from SE to WN     
        """
        txt = '\n'
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
           txt += '\n'
           txt += 'which lcg-cp\n'
           txt += 'lcgcp_location_exit_code=$?\n'
           txt += '\n'
           txt += 'if [ $lcgcp_location_exit_code -eq 1 ]; then\n'
           txt += ''
           txt += '    echo "X509_USER_PROXY = $X509_USER_PROXY"\n'
           txt += '    echo "source $OSG_APP/glite/setup_glite_ui.sh"\n'
           txt += '    source $OSG_APP/glite/setup_glite_ui.sh\n'
           txt += '    export X509_CERT_DIR=$OSG_APP/glite/etc/grid-security/certificates\n'
           txt += '    echo "export X509_CERT_DIR=$X509_CERT_DIR"\n'
           txt += 'else\n'
           txt += '    echo "X509_USER_PROXY = $X509_USER_PROXY"\n'
           txt += '    export X509_CERT_DIR=/etc/grid-security/certificates\n'
           txt += '    echo "export X509_CERT_DIR=$X509_CERT_DIR"\n'
           txt += 'fi\n'
           txt += '    for out_file in $file_list ; do\n'
           txt += '        echo "Trying to copy output file to $SE using srmcp"\n'
           txt += '        echo "mkdir -p $HOME/.srmconfig"\n'
           txt += '        mkdir -p $HOME/.srmconfig\n'
           txt += '        echo "srmcp -retry_num 3 -retry_timeout 480000 -x509_user_trusted_certificates $X509_CERT_DIR file:////`pwd`/$out_file srm://${SE}:8443${SE_PATH}$out_file"\n'
           txt += '        exitstring=`srmcp -retry_num 3 -retry_timeout 480000 -x509_user_trusted_certificates $X509_CERT_DIR file:////\`pwd\`/$out_file srm://${SE}:8443${SE_PATH}$out_file 2>&1`\n'
           txt += '        copy_exit_status=$?\n'
           txt += '        echo "COPY_EXIT_STATUS for srm = $copy_exit_status"\n'
           txt += '        echo "STAGE_OUT = $copy_exit_status"\n'
           txt += '        if [ $copy_exit_status -ne 0 ]; then\n'
           txt += '           echo "Possible problems with SE = $SE"\n'
           txt += '           echo "StageOutExitStatus = 198" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '           echo "StageOutExitStatusReason = $exitstring" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '           echo "srmcp failed, attempting lcg-cp"\n'
           txt += '           echo "Trying to copy output file to $SE using lcg-cp"\n'
           if common.logger.debugLevel() >= 5:
               txt += '           echo "lcg-cp --vo $VO --verbose -t 2400 file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file"\n'
               txt += '           exitstring=`lcg-cp --vo $VO --verbose -t 2400 file://\`pwd\`/$out_file gsiftp://${SE}${SE_PATH}$out_file 2>&1`\n'
           else:              
               txt += '           echo "lcg-cp --vo $VO -t 2400 file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file"\n'
               txt += '           exitstring=`lcg-cp --vo $VO -t 2400 file://\`pwd\`/$out_file gsiftp://${SE}${SE_PATH}$out_file 2>&1`\n' 
           txt += '           copy_exit_status=$?\n'
           txt += '           echo "COPY_EXIT_STATUS for lcg-cp = $copy_exit_status"\n'
           txt += '           echo "STAGE_OUT = $copy_exit_status"\n'
           txt += '           if [ $copy_exit_status -ne 0 ]; then\n'
           txt += '              echo "Problems with SE = $SE"\n'
           txt += '              echo "StageOutExitStatus = 198" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '              echo "StageOutExitStatusReason = $exitstring" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '          echo "lcg-cp failed.  For verbose lcg-cp output, use command line option -debug 5."\n'
           txt += '              echo "lcg-cp and srmcp failed!"\n'
           txt += '           else\n'
           txt += '              echo "StageOutSE = $SE" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '              echo "StageOutCatalog = " | tee -a $RUNTIME_AREA/$repo\n'
           txt += '              echo "output copied into $SE/$SE_PATH directory"\n'
           txt += '              echo "StageOutExitStatus = 0" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '              echo "lcg-cp succeeded"\n'
           txt += '           fi\n'
           txt += '        else\n'
           txt += '           echo "StageOutSE = $SE" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '           echo "StageOutCatalog = " | tee -a $RUNTIME_AREA/$repo\n'
           txt += '           echo "output copied into $SE/$SE_PATH directory"\n'
           txt += '           echo "StageOutExitStatus = 0" | tee -a $RUNTIME_AREA/$repo\n'
           txt += '           echo "srmcp succeeded"\n'
           txt += '        fi\n'
           txt += '     done\n'

        return txt

    def wsRegisterOutput(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """

        txt = ''
        return txt

    def loggingInfo(self, id):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        schedd    = id.split('//')[0]
        condor_id = id.split('//')[1]
        cmd = 'condor_q -l -name ' + schedd + ' ' + condor_id
        cmd_out = runCommand(cmd)
        common.logger.debug(5,"Condor-G loggingInfo cmd: "+cmd)
        common.logger.debug(5,"Condor-G loggingInfo cmd_out: "+cmd_out)
        return cmd_out

    def listMatch(self, nj):
        """
        Check the compatibility of available resources
        """
        self.checkProxy()
        return 1

    def submit(self, nj):
        """
        Submit one OSG job.
        """
        self.checkProxy()

        jid = None
        jdl = common.job_list[nj].jdlFilename()

        cmd = 'condor_submit ' + jdl 
        cmd_out = runCommand(cmd)
        if cmd_out != None:
            tmp = cmd_out.find('submitted to cluster') + 21
            jid = cmd_out[tmp:].replace('.','')
            jid = jid.replace('\n','')
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

        result = ''
        
        if ( attr == 'exit_code' ) :
            jobnum_str = '%06d' % (int(id))
            # opts = common.work_space.loadSavedOptions()
            base = string.upper(opts['-jobtype']) 
            log_file = common.work_space.resDir() + base + '_' + jobnum_str + '.stdout'
            logfile = open(log_file)
            log_line = logfile.readline()
            while log_line :
                log_line = log_line.strip()
                if log_line.startswith('JOB_EXIT_STATUS') :
                    log_line_split = log_line.split()
                    result = log_line_split[2]
                    pass
                log_line = logfile.readline()
            result = ''
        elif ( attr == 'status' ) :
            schedd    = id.split('//')[0]
            condor_id = id.split('//')[1]
            cmd = 'condor_q -name ' + schedd + ' ' + condor_id
            cmd_out = runCommand(cmd)
            if cmd_out != None:
                status_flag = 0
                for line in cmd_out.splitlines() :
                    if line.strip().startswith(condor_id.strip()) :
                        status = line.strip().split()[5]
                        if ( status == 'I' ):
                            result = 'Scheduled'
                            break
                        elif ( status == 'U' ) :
                            result = 'Ready'
                            break
                        elif ( status == 'H' ) :
                            result = 'Hold'
                            break
                        elif ( status == 'R' ) :
                            result = 'Running'
                            break
                        elif ( status == 'X' ) :
                            result = 'Cancelled'
                            break
                        elif ( status == 'C' ) :
                            result = 'Done'
                            break
                        else :
                            result = 'Done'
                            break
                    else :
                        result = 'Done'
            else :
                result = 'Done'
        elif ( attr == 'destination' ) :
            seSite = common.jobDB.destination(int(id)-1)[0]
            oneSite = self.mapSEtoCE[seSite]
            result = oneSite
        elif ( attr == 'reason' ) :
            result = 'status query'
        elif ( attr == 'stateEnterTime' ) :
            result = time.asctime(time.gmtime())
        return result

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """
        user = os.environ['USER']
        cmd = 'condor_q -submitter ' + user
        cmd_out = runCommand(cmd)
        return cmd_out

    def getOutput(self, id):
        """
        Get output for a finished job with id.
        Returns the name of directory with results.
        not needed for condor-g
        """
        self.checkProxy()
        return ''

    def cancel(self, id):
        """ Cancel the condor job with id """
        self.checkProxy()
        # query for schedd
        user = os.environ['USER']
        cmd = 'condor_q -submitter ' + user
        cmd_out = runCommand(cmd)
        schedd=''
        if cmd_out != None:
            for line in cmd_out.splitlines() :
                if line.strip().startswith('--') :
                    schedd = line.strip().split()[6]
                if line.strip().startswith(id.strip()) :
                    status = line.strip().split()[5]
                    break
        cmd = 'condor_rm -name ' + schedd + ' ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

    def createSchScript(self, nj):
        """
        Create a JDL-file for condor
        """

        job = common.job_list[nj]
        jbt = job.type()
        inp_sandbox = jbt.inputSandbox(nj)
        out_sandbox = jbt.outputSandbox(nj)

        # write EDG style JDL

        title = '# This JDL was generated by '+\
                common.prog_name+' (version '+common.prog_version_str+')\n'
        
        jdl_fname = job.jdlFilename()
        jdl = open(jdl_fname, 'w')
        jdl.write(title)
        
        jdl.write('universe = "globus";\n')
        
        # use gridcat to query site
        seSite = common.jobDB.destination(nj)[0]
        oneSite = self.mapSEtoCE[seSite]
        common.logger.message("Job "+str(nj+1)+" will run at ['"+str(oneSite)+"']")
        gridcat_service_url = "http://osg-cat.grid.iu.edu/services.php"
        hostSvc = ''
        try:
            hostSvc = GridCatHostService(gridcat_service_url,oneSite)
        except StandardError, ex:
            gridcat_service_url = "http://osg-itb.ivdgl.org/gridcat/services.php"
            try:
                hostSvc = GridCatHostService(gridcat_service_url,oneSite)
            except StandardError, ex:
                msg  = '[Condor-G Scheduler]: selected site: '+oneSite+' is not an OSG site!\n'
                msg += '[Condor-G Scheduler]: Direct Condor-G submission to LCG sites is not possible!\n'
                common.logger.message(msg)
                raise CrabException(msg)

        try:
            batchsystem = hostSvc.batchSystem()
        except:
            raise CrabException("Quitting: unable to find jobmanager for site "+str(oneSite))
        if batchsystem <> '' : batchsystem='-'+batchsystem
        jdl_globusscheduler = 'globusscheduler = "' + oneSite + '/jobmanager' + batchsystem + '";\n'
        jdl.write(jdl_globusscheduler)
        if ( self.EDG_clock_time != '' ) :
            jdl.write('globusrsl = "(maxWalltime='+self.EDG_clock_time+')";\n')

        jdl.write('ENABLE_GRID_MONITOR = "TRUE";\n')

        script = job.scriptFilename()
        jdl.write('Executable = "' + os.path.basename(script) + '";\n')
    
        jdl.write('should_transfer_files = "YES";\n')
        jdl.write('when_to_transfer_output = "ON_EXIT";\n')

        inp_box = 'InputSandbox = { '
        inp_box = inp_box + '"' + script + '" ,'

        if inp_sandbox != None:
            for fl in inp_sandbox:
                inp_box = inp_box + '"' + fl + '" ,'
                pass
            pass

        for addFile in jbt.additional_inbox_files:
            addFile = os.path.abspath(addFile)
            inp_box = inp_box + '"' + addFile + '" ,'
            pass

        inp_box = inp_box+' "' + os.path.abspath(os.environ['CRABDIR']+'/python/'+'report.py') + '", "' +\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'DashboardAPI.py') + '", "'+\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'Logger.py') + '", "'+\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'ProcInfo.py') + '", "'+\
                  os.path.abspath(os.environ['CRABDIR']+'/python/'+'apmon.py') + '"'

        if inp_box[-1] == ',' : inp_box = inp_box[:-1]
        inp_box = inp_box + ' };\n'
        jdl.write(inp_box)

        out_box = 'OutputSandbox = { '
        out_box = out_box + '"' + job.stdout() + '" ,'
        out_box = out_box + '"' + job.stderr() + '" ,'

        if int(self.return_data) == 1:
            if out_sandbox != None:
                for fl in out_sandbox:
                    out_box = out_box + '"' + fl + '" ,'
                    pass
                pass
            pass

        if out_box[-1] == ',' : out_box = out_box[:-1]
        out_box = out_box + ' };\n'
        jdl.write(out_box)

        #firstEvent = common.jobDB.firstEvent(nj)
        #maxEvents = common.jobDB.maxEvents(nj)
        jdl.write('Arguments = "' + str(nj+1)+' '+jbt.getJobTypeArguments(nj, "CONDOR") + '";\n')

        jdl.write('StdOutput = "' + job.stdout() + '";\n')
        jdl.write('stream_output = "false";\n')
        jdl.write('StdError  = "' + job.stderr() + '";\n')
        jdl.write('stream_error = "false";\n')
        logentry = 'Log    = "' + job.stderr() + '";\n'
        logentry = logentry.replace('stderr','log')
        jdl.write(logentry)
        jdl.write('notification="never";\n')
        jdl.write('QUEUE = "1";\n')
        jdl.close()

        return

    def createXMLSchScript(self, nj, argsList):
        """
        Create a XML-file for BOSS4.
        """

        # job steering
        index = nj - 1
        job = common.job_list[index]
        jbt = job.type()

        # input and output sandboxes
        inp_sandbox = jbt.inputSandbox(index)
        out_sandbox = jbt.outputSandbox(index)

        # title
        title     = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
        jt_string = ''
        
        xml_fname = str(self.jobtypeName)+'.xml'
        xml       = open(common.work_space.shareDir()+'/'+xml_fname, 'a')

        # TaskName   
        dir      = string.split(common.work_space.topDir(), '/')
        taskName = dir[len(dir)-2]
  
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
        xml.write('\t<iteratorRule name="ITR3">\n')
        xml.write('\t\t<ruleElement> 1:'+ str(nj) + ':1:6 </ruleElement>\n')
        xml.write('\t</iteratorRule>\n')

        xml.write('<chain scheduler="'+str(self.schedulerName)+'">\n')
        xml.write(jt_string)


        #executable

        """
        INDY
        script dipende dal jobType: dovrebbe essere semplice tirarlo fuori in altro modo
        """        
        script = job.scriptFilename()
        xml.write('<program>\n')
        xml.write('<exec> ' + os.path.basename(script) +' </exec>\n')
        xml.write(jt_string)
    
        xml.write('<args> <![CDATA[\n _ITR2_ \n]]> </args>\n')
        xml.write('<program_types> crabjob </program_types>\n')

        # input sanbox
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
            inp_box = inp_box + ', '
            for addFile in jbt.additional_inbox_files:
                addFile = os.path.abspath(addFile)
                inp_box = inp_box+''+addFile+','
                pass

        if inp_box[-1] == ',' : inp_box = inp_box[:-1]
        inp_box = '<infiles> <![CDATA[\n' + inp_box + '\n]]> </infiles>\n'
        xml.write(inp_box)
        
        # stdout and stderr
        base = jbt.name()
        stdout = base + '__ITR3_.stdout'
        stderr = base + '__ITR3_.stderr'

        xml.write('<stderr> ' + stderr + '</stderr>\n')
        xml.write('<stdout> ' + stdout + '</stdout>\n')
        
        # output sanbox
        out_box = stdout + ',' + stderr + ',' 

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

        # start writing of extraTags
        to_write = ''

        # extraTag universe
        to_write += 'universe = "&quot;globus&quot;"\n'

        # extraTag globusscheduler

        # use gridcat to query site
        seSite = common.jobDB.destination(nj-1)[0]
        oneSite = self.mapSEtoCE[seSite]
        gridcat_service_url = "http://osg-cat.grid.iu.edu/services.php"
        hostSvc = ''
        try:
            hostSvc = GridCatHostService(gridcat_service_url,oneSite)
        except StandardError, ex:
            gridcat_service_url = "http://osg-itb.ivdgl.org/gridcat/services.php"
            try:
                hostSvc = GridCatHostService(gridcat_service_url,oneSite)
            except StandardError, ex:
                    msg  = '[Condor-G Scheduler]: selected site: '+oneSite+' is not an OSG site!\n'
                    msg += '[Condor-G Scheduler]: Direct Condor-G submission to LCG sites is not possible!\n'
                    common.logger.message(msg)
                    raise CrabException(msg)

        try:
            batchsystem = hostSvc.batchSystem()
        except:
            raise CrabException("Quitting: unable to find jobmanager for site "+str(oneSite))
        if batchsystem <> '' : batchsystem='-'+batchsystem
        to_write += 'globusscheduler = "&quot;' + str(oneSite) + '/jobmanager' + batchsystem + '&quot;"\n'

        # extraTag globusrsl
        if ( self.EDG_clock_time != '' ) :
            to_write += 'globusrsl = "&quot;(maxWalltime='+self.EDG_clock_time+')&quot;"\n'

        # extraTag condor transfer file flag
        to_write += 'should_transfer_files = "&quot;YES&quot;"\n'

        # extraTag when to write output
        to_write += 'when_to_transfer_output = "&quot;ON_EXIT&quot;"\n'

        # extraTag switch off streaming of stdout
        to_write += 'stream_output = "&quot;false&quot;"\n'

        # extraTag switch off streaming of stderr
        to_write += 'stream_error = "&quot;false&quot;"\n'

        # extraTag condor logfile
        condor_log = jbt.name() + '__ITR3_.log'
        to_write += 'Log    = "&quot;' + condor_log + '&quot;"\n'

        # extraTag condor notification
        to_write += 'notification="&quot;never&quot;"\n'

        # extraTag condor queue statement
        to_write += 'QUEUE = "&quot;1&quot;"\n'

        if (to_write != ''):
            xml.write('<extraTags\n')
            xml.write(to_write)
            xml.write('/>\n')
            pass

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

        self.proxyValid=1
        return
