from Scheduler import Scheduler
from JobList import JobList
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from GridCatService import GridCatHostService
import time
import common

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
            print '[Condor-G Scheduler]: condor_schedd is not running on this machine.'
            print '[Condor-G Scheduler]: Please use another machine with installed condor and running condor_schedd or change the Scheduler in your crab.cfg.'
            sys.exit(1)

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
            print '[Condor-G Scheduler]: condor_version was not able to determine the installed condor version.'
            print '[Condor-G Scheduler]: Please use another machine with properly installed condor or change the Scheduler in your crab.cfg.'
            sys.exit(1)

        self.checkExecutableInPath('condor_config_val')

        self.checkCondorVariablePointsToFile('GRIDMANAGER')

        if version_master >= 6 and version_major >= 7 and version_minor >= 11 :
            self.checkCondorVariablePointsToFile('GT2_GAHP')
        else :
            self.checkCondorVariablePointsToFile('GAHP')

        self.checkCondorVariablePointsToFile('GRID_MONITOR')

        self.checkCondorVariableIsTrue('ENABLE_GRID_MONITOR')

        max_submit = self.queryCondorVariable('GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE')
        max_pending = self.queryCondorVariable('GRIDMANAGER_MAX_PENDING_SUBMITS_PER_RESOURCE')

        print '[Condor-G Scheduler]'
        print 'Maximal number of jobs submitted to the grid   : GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE  = ',max_submit
        print 'Maximal number of parallel submits to the grid : GRIDMANAGER_MAX_PENDING_SUBMITS_PER_RESOURCE = ',max_pending
        print 'Ask the administrator of your local condor installation to increase these variables to enable more jobs to be executed on the grid in parallel.\n'
        
        return

    def checkExecutableInPath(self, name):
        # check if executable is in PATH
        cmd = 'which '+name
        cmd_out = runCommand(cmd)
        if cmd_out == None:
            print '[Condor-G Scheduler]: ',name,' is not in the $PATH on this machine.'
            print '[Condor-G Scheduler]: Please use another machine with installed condor or change the Scheduler in your crab.cfg.'
            sys.exit(1)

    def checkCondorVariablePointsToFile(self, name):
        ## check for condor variable
        cmd = 'condor_config_val '+name
        cmd_out = runCommand(cmd)
        if os.path.isfile(cmd_out) > 0 :
            print '[Condor-G Scheduler]: the variable ',name,' is not properly set for the condor installation on this machine.'
            print '[Condor-G Scheduler]: Please ask the administrator of the local condor installation to set the variable ',name,' properly,',
            'use another machine with properly installed condor or change the Scheduler in your crab.cfg.'
            sys.exit(1)

    def checkCondorVariableIsTrue(self, name):
        ## check for condor variable
        cmd = 'condor_config_val '+name
        cmd_out = runCommand(cmd)
        if cmd_out == 'TRUE' :
            print '[Condor-G Scheduler]: the variable ',name,' is not set to true for the condor installation on this machine.'
            print '[Condor-G Scheduler]: Please ask the administrator of the local condor installation to set the variable ',name,' to true,',
            'use another machine with properly installed condor or change the Scheduler in your crab.cfg.'
            sys.exit(1)

    def queryCondorVariable(self, name):
        ## check for condor variable
        cmd = 'condor_config_val '+name
        return runCommand(cmd)

    def configure(self, cfg_params):

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
            print '[Condor-G Scheduler]: destination site is not selected properly.'
            print '[Condor-G Scheduler]: Please select your destination site and only your destination site in the CE_white_list variable of the [EDG] section in your crab.cfg.'
            sys.exit(1)
            
        if len(tmpGood) != 1 :
            print '[Condor-G Scheduler]: destination site is not selected properly.'
            print '[Condor-G Scheduler]: Please select your destination site and only your destination site in the CE_white_list variable of the [EDG] section in your crab.cfg.'
            sys.exit(1)

        # activate Boss per default
        try:
            self.UseBoss = cfg_params['CRAB.use_boss'];
        except KeyError:
            self.UseBoss = '1';

        try:
            self.UseGT4 = cfg_params['USER.use_gt_4'];
        except KeyError:
            self.UseGT4 = 0;

        self.proxyValid=0
        # added here because checklistmatch is not used
        self.checkProxy()

        self._taskId = cfg_params['taskId']
                
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
        txt += "# job number (first parameter for job wrapper)\n"
        txt += "NJob=$1\n"

        # create hash of cfg file
        hash = makeCksum(common.work_space.cfgFileName())
        txt += 'echo "MonitorJobID=`echo ${NJob}_'+hash+'_$GLOBUS_GRAM_JOB_CONTACT`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += 'echo "SyncGridJobId=`echo $GLOBUS_GRAM_JOB_CONTACT`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += 'echo "MonitorID=`echo ' + self._taskId + '`" | tee -a $RUNTIME_AREA/$repo\n'

        txt += 'echo "middleware discovery " \n'
        txt += 'if [ $VO_CMS_SW_DIR ]; then\n'
        txt += '    middleware=LCG \n'
        txt += '    echo "SyncCE=`edg-brokerinfo getCE`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'elif [ $GRID3_APP_DIR ]; then\n'
        txt += '    middleware=OSG \n'
        txt += '    echo "SyncCE=`echo $hostname`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'elif [ $OSG_APP ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    echo "SyncCE=`echo $hostname`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'else \n'
        txt += '    echo "SET_CMS_ENV 10030 ==> middleware not identified" \n'
        txt += '    echo "JOB_EXIT_STATUS = 10030" \n'
        txt += '    echo "JobExitCode=10030" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '    exit 1 \n'
        txt += 'fi\n'

        txt += '# report first time to DashBoard \n'
        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'

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
        txt += 'CE=$4\n'
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
           txt += 'if [ $exe_result -eq 0 ]; then\n'
           txt += '    for out_file in $file_list ; do\n'
           txt += '        echo "Trying to copy output file to $SE "\n'
#           txt += '        echo "lcg-cp --vo cms -t 30 file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file"\n'
           txt += '        echo "globus-url-copy file://`pwd`/$out_file gsiftp://${SE}${SE_PATH}$out_file"\n'
#           txt += '        exitstring=`lcg-cp --vo cms -t 30 file://\`pwd\`/$out_file gsiftp://${SE}${SE_PATH}$out_file 2>&1`\n'
           txt += '        exitstring=`globus-url-copy file://\`pwd\`/$out_file gsiftp://${SE}${SE_PATH}$out_file 2>&1`\n'
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
        return txt

    def loggingInfo(self, id):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        self.checkProxy()
        cmd = 'condor_q -l -analyze ' + id
        cmd_out = os.popen(cmd) 
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

        self.checkProxy()
        result = ''
        
        if ( attr == 'exit_code' ) :
            for i in range(common.jobDB.nJobs()) :
                if ( id == common.jobDB.jobId(i) ) :
                    jobnum_str = '%06d' % (int(i)+1)
                    opts = common.work_space.loadSavedOptions()
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
        elif ( attr == 'status' ) :
            user = os.environ['USER']
            cmd = 'condor_q -submitter ' + user
            cmd_out = runCommand(cmd)
            if cmd_out != None:
                for line in cmd_out.splitlines() :
                    if line.strip().startswith(id.strip()) :
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
            for i in range(common.jobDB.nJobs()) :
                if ( id == common.jobDB.jobId(i) ) :
                    jobnum_str = '%06d' % (int(i)+1)
                    opts = common.work_space.loadSavedOptions()
                    base = string.upper(opts['-jobtype']) 
                    log_file = common.work_space.resDir() + base + '_' + jobnum_str + '.stdout'
                    logfile = open(log_file)
                    log_line = logfile.readline()
                    while log_line :
                        log_line = log_line.strip()
                        if log_line.startswith('GridJobId') :
                            log_line_split = log_line.split()
                            result = os.path.split(log_line_split[2])[0]
                            pass
                        log_line = logfile.readline()
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

        # write EDG style JDL if UseBoss == 1

        if  self.UseBoss == '1'  :

            title = '# This JDL was generated by '+\
                    common.prog_name+' (version '+common.prog_version_str+')\n'
            
            jdl_fname = job.jdlFilename()
            jdl = open(jdl_fname, 'w')
            jdl.write(title)
            
            jdl.write('universe = "globus";\n')
            
            # use gridcat to query site
            gridcat_service_url = "http://osg-cat.grid.iu.edu/services.php"
            hostSvc = ''
            try:
                hostSvc = GridCatHostService(gridcat_service_url,common.analisys_common_info['sites'][0])
            except StandardError, ex:
                gridcat_service_url = "http://osg-itb.ivdgl.org/gridcat/services.php"
                try:
                    hostSvc = GridCatHostService(gridcat_service_url,common.analisys_common_info['sites'][0])
                except StandardError, ex:
                    print '[Condor-G Scheduler]: selected site: ',common.analisys_common_info['sites'][0],' is not an OSG site!\n'
                    print '[Condor-G Scheduler]: Direct Condor-G submission to LCG sites is not possible!\n'
                    sys.exit(1)

            batchsystem = hostSvc.batchSystem()
            if batchsystem <> '' : batchsystem='-'+batchsystem
            jdl_globusscheduler = 'globusscheduler = "' + common.analisys_common_info['sites'][0] + '/jobmanager' + batchsystem + '";\n'
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

        else :

            title = '# This JDL was generated by '+\
                    common.prog_name+' (version '+common.prog_version_str+')\n'
            
            jdl_fname = job.jdlFilename()
            jdl = open(jdl_fname, 'w')
            jdl.write(title)
            
            
            # use gridcat to query site
            gridcat_service_url = "http://osg-cat.grid.iu.edu/services.php"
            hostSvc = ''
            try:
                hostSvc = GridCatHostService(gridcat_service_url,common.analisys_common_info['sites'][0])
            except StandardError, ex:
                gridcat_service_url = "http://osg-itb.ivdgl.org/gridcat/services.php"
                try:
                    hostSvc = GridCatHostService(gridcat_service_url,common.analisys_common_info['sites'][0])
                except StandardError, ex:
                    print '[Condor-G Scheduler]: selected site: ',common.analisys_common_info['sites'][0],' is not an OSG site!\n'
                    print '[Condor-G Scheduler]: Direct Condor-G submission to LCG sites is not possible!\n'
                    sys.exit(1)

            batchsystem = hostSvc.batchSystem()

            if  self.UseGT4 == '1'  :

                jdl.write('universe = grid\n')
                jdl.write('grid_type = gt4\n')
                jdl_globusscheduler = 'globusscheduler = ' + common.analisys_common_info['sites'][0] + '\n'
                jdl.write(jdl_globusscheduler)
                jdl_jobmanager = 'jobmanager_type = ' + batchsystem + '\n'
                jdl.write(jdl_jobmanager)
                if ( self.EDG_clock_time != '' ) :
                    jdl.write('globusrsl = (maxWalltime='+self.EDG_clock_time+')\n')

            else :

                if batchsystem <> '' : batchsystem='-'+batchsystem
                jdl.write('universe = globus\n')
                jdl_globusscheduler = 'globusscheduler = ' + common.analisys_common_info['sites'][0] + '/jobmanager' + batchsystem + '\n'
                jdl.write(jdl_globusscheduler)
                if ( self.EDG_clock_time != '' ) :
                    jdl.write('globusrsl = (maxWalltime='+self.EDG_clock_time+')\n')

            jdl.write('ENABLE_GRID_MONITOR = TRUE\n')

            script = job.scriptFilename()
            jdl.write('Executable = ' + common.work_space.jobDir() + '/' + os.path.basename(script) + '\n')
        
            jdl.write('should_transfer_files = YES\n')
            jdl.write('when_to_transfer_output = ON_EXIT\n')

            inp_box = 'transfer_input_files ='
            inp_box = inp_box + script + ','

            if inp_sandbox != None:
                for fl in inp_sandbox:
                    inp_box = inp_box + fl + ','
                    pass
                pass

            for addFile in jbt.additional_inbox_files:
                addFile = os.path.abspath(addFile)
                inp_box = inp_box + addFile + ','
                pass

            inp_box = inp_box+ os.path.abspath(os.environ['CRABDIR']+'/python/'+'report.py') + ',' +\
                      os.path.abspath(os.environ['CRABDIR']+'/python/'+'Logger.py') + ','+\
                      os.path.abspath(os.environ['CRABDIR']+'/python/'+'ProcInfo.py') + ','+\
                      os.path.abspath(os.environ['CRABDIR']+'/python/'+'apmon.py')

            if inp_box[-1] == ',' : inp_box = inp_box[:-1]
            inp_box = inp_box + '\n'
            jdl.write(inp_box)

            out_box = 'transfer_output_files = '

            if int(self.return_data) == 1:
                if out_sandbox != None:
                    for fl in out_sandbox:
                        out_box = out_box + common.work_space.resDir() + '/' + fl + ','
                        pass
                    pass
                pass

            if out_box[-1] == ',' : out_box = out_box[:-1]
            out_box = out_box + '\n'
            jdl.write(out_box)

            firstEvent = common.jobDB.firstEvent(nj)
            maxEvents = common.jobDB.maxEvents(nj)
            jdl.write('Arguments = ' + str(nj+1)+' '+str(firstEvent)+' '+str(maxEvents)+' '+common.analisys_common_info['sites'][0]+'\n')
            # arguments for Boss
            jdl.write('Output = ' + common.work_space.resDir() + '/' + job.stdout() + '\n')
            jdl.write('stream_output = false\n')
            jdl.write('Error  = ' + common.work_space.resDir() + '/' + job.stderr() + '\n')
            jdl.write('stream_error = false\n')
            logentry = 'Log    = ' + common.work_space.resDir() + '/' + job.stderr() + '\n'
            logentry = logentry.replace('stderr','log')
            jdl.write(logentry)
            jdl.write('notification=never\n')
            jdl.write('QUEUE 1\n')
            jdl.close()
        return

    def checkProxy(self):
        """
        Function to check the Globus proxy.
        """
        if (self.proxyValid): return
        timeleft = -999
        minTimeLeft=10 # in hours
        cmd = 'voms-proxy-info -exists -valid '+str(minTimeLeft)+':00'
        # SL Here I have to use os.system since the stupid command exit with >0 if no valid proxy is found
        cmd_out = os.system(cmd)
        if (cmd_out>0):
            common.logger.message( "No valid proxy found or remaining time of validity of already existing proxy shorter than 10 hours!\n Creating a user proxy with default length of 96h\n")
            cmd = 'voms-proxy-init -voms cms -valid 96:00'
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
