from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import os, sys, tempfile

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

        try:
            self.VO = cfg_params['EDG.virtual_organization']
        except KeyError:
            self.VO = 'cms'

        # Add EDG_WL_LOCATION to the python path

        try:
            path = os.environ['GLITE_WMS_LOCATION']
#            path = os.environ['EDG_WL_LOCATION']
        except:
            msg = "Error: the GLITE_WMS_LOCATION variable is not set."
#            msg = "Error: the EDG_WL_LOCATION variable is not set."
            raise CrabException(msg)

        libPath=os.path.join(path, "lib")
        sys.path.append(libPath)
        libPath=os.path.join(path, "lib", "python")
        sys.path.append(libPath)

        self.checkProxy_()
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
        txt = '\n'
        # MARCO
        txt += 'CloseCEs=`glite-brokerinfo getCE`\n'
        # MARCO
        txt += 'echo "CloseCEs = $CloseCEs"\n'
        txt += 'CE=`echo $CloseCEs | sed -e "s/:.*//"`\n'
        txt += 'echo "CE = $CE"\n'
        return txt

    def loggingInfo(self, nj):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        id = common.jobDB.jobId(nj)
        edg_ui_cfg_opt = ''
        if self.edg_config:
          edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        cmd = 'edg-job-get-logging-info -v 2 ' + edg_ui_cfg_opt + id
        print cmd
        myCmd = os.popen(cmd)
        cmd_out = myCmd.readlines()
        myCmd.close()
        return cmd_out

    def listMatch(self, nj):
        """
        Check the compatibility of available resources
        """
        jdl = common.job_list[nj].jdlFilename()
        edg_ui_cfg_opt = ''
        if self.edg_config:
          edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        if self.edg_config_vo: 
          edg_ui_cfg_opt += ' --config-vo ' + self.edg_config_vo + ' '
        cmd = 'edg-job-list-match ' + edg_ui_cfg_opt + jdl 
        myCmd = os.popen(cmd)
        cmd_out = myCmd.readlines()
        myCmd.close()
        return self.parseListMatch_(cmd_out, jdl)

    def parseListMatch_(self, out, jdl):
        reComment = re.compile( r'^\**$' )
        reEmptyLine = re.compile( r'^$' )
        reVO = re.compile( r'Selected Virtual Organisation name.*' )
        reCE = re.compile( r'CEId' )
        reNO = re.compile( r'No Computing Element matching' )
        reRB = re.compile( r'Connecting to host' )
        next = 0
        CEs=[]
        Match=0
        for line in out:
            line = line.strip()
            if reComment.match( line ): 
                next = 0
                continue
            if reEmptyLine.match(line):
                continue
            if reVO.match( line ):
                VO =line.split()[-1]
                common.logger.debug(5, 'VO           :'+VO)
                pass
            if reRB.match( line ):
                RB =line.split()[3]
                common.logger.debug(5, 'Using RB     :'+RB)
                pass
            if reCE.search( line ):
                next = 1
                continue
            if next:
                CE=line.split(':')[0]
                CEs.append(CE)
                common.logger.debug(5, 'Matched CE   :'+CE)
                Match=Match+1
                pass 
            if reNO.match( line ):
                common.logger.debug(5,line)
                self.noMatchFound_(jdl)
                Match=0
                pass
        return Match

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

        jid = None
        jdl = common.job_list[nj].jdlFilename()
        id_tmp = tempfile.mktemp()
        edg_ui_cfg_opt = ' '
        if self.edg_config:
          edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        if self.edg_config_vo: 
          edg_ui_cfg_opt += ' --config-vo ' + self.edg_config_vo + ' '
        cmd = 'edg-job-submit -o ' + id_tmp + edg_ui_cfg_opt + jdl 
        cmd_out = runCommand(cmd)
        if cmd_out != None:
            idfile = open(id_tmp)
            jid_line = idfile.readline()
            while jid_line[0] == '#':
                jid_line = idfile.readline()
                pass
            jid = string.strip(jid_line)
            os.unlink(id_tmp)
            pass
        return jid

    def getExitStatus(self, id):
        return self.getStatusAttribute_(id, 'exit_code')

    def queryStatus(self, id):
        return self.getStatusAttribute_(id, 'status')

    def queryDest(self, id):  
        return self.getStatusAttribute_(id, 'destination')


    def getStatusAttribute_(self, id, attr):
        """ Query a status of the job with id """

        hstates = {}
#        Status = importName('edg_wl_userinterface_common_LbWrapper', 'Status')
        Status = importName('glite_wmsui_LbWrapper', 'Status')
        # Bypass edg-job-status interfacing directly to C++ API
        # Job attribute vector to retrieve status without edg-job-status
        level = 0
        # Instance of the Status class provided by LB API
        jobStat = Status()
        st = 0
        jobStat.getStatus(id, level)
        err, apiMsg = jobStat.get_error()
        if err:
            print 'Error caught', apiMsg 
            common.log.message(apiMsg)
            return None
        else:
            for i in range(len(self.states)):
                #print "states = ", states
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

        cmd = 'edg-job-get-output --dir ' + common.work_space.resDir() + ' ' + id
        cmd_out = runCommand(cmd)

        # Determine the output directory name
        dir = common.work_space.resDir()
        dir += os.getlogin()
        dir += '_' + os.path.basename(id)
        return dir

    def cancel(self, id):
        """ Cancel the EDG job with id """
        cmd = 'edg-job-cancel --noint ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

    def checkProxy_(self):
        """
        Function to check the Globus proxy.
        """
        cmd = 'grid-proxy-info -timeleft'
        cmd_out = runCommand(cmd)
        ok = 1
        timeleft = -999
        try: timeleft = int(cmd_out)
        except ValueError: ok=0
        except TypeError: ok=0
        if timeleft < 1:  ok=0

        if ok==0:
            print "No valid proxy found !\n"
            print "Creating a user proxy with default length of 100h\n"
            msg = "Unable to create a valid proxy!\n"
            if os.system("grid-proxy-init -valid 100:00"):
                raise CrabException(msg)
        return
    
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

        for addFile in jbt.additional_inbox_files:
            addFile = os.path.abspath(addFile)
            inp_box = inp_box+' "'+addFile+'",'
            pass

        if inp_box[-1] == ',' : inp_box = inp_box[:-1]
        inp_box = inp_box + ' };\n'
        jdl.write(inp_box)

        jdl.write('StdOutput     = "' + job.stdout() + '";\n')
        jdl.write('StdError      = "' + job.stderr() + '";\n')

        #if common.flag_return_data :
        #    for fl in job.outputDataFiles():
        #        out_box = out_box + ' "' + fl + '",'
        #        pass
        #    pass

        out_box = 'OutputSandbox = { '
        if out_sandbox != None:
            for fl in out_sandbox:
                out_box = out_box + ' "' + fl + '",'
                pass
            pass

        if out_box[-1] == ',' : out_box = out_box[:-1]
        out_box = out_box + ' };'
        jdl.write(out_box+'\n')

        # If CloseCE is used ...
        #if common.flag_usecloseCE and job.inputDataFiles():
        #    indata = 'InputData = { '
        #    for fl in job.inputDataFiles():
        #       indata = indata + ' "lfn:' + SPL + fl + '",'
        #    if indata[-1] == ',' : indata = indata[:-1]
        #    indata = indata + ' };'
        #    jdl.write(indata+'\n')
        #    jdl.write('DataAccessProtocol = { "gsiftp" };\n')

        if common.analisys_common_info['sites']:
           if common.analisys_common_info['sw_version']:

             req='Requirements = '
         ### First ORCA version
             req=req + 'Member("VO-cms-' + \
                 common.analisys_common_info['sw_version'] + \
                 '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'
         ## then sites
             if len(common.analisys_common_info['sites'])>0:
               req = req + ' && ('
             for i in range(len(common.analisys_common_info['sites'])):
                req = req + 'other.GlueCEInfoHostName == "' \
                     + common.analisys_common_info['sites'][i] + '"'
                if ( i < (int(len(common.analisys_common_info['sites']) - 1)) ):
                    req = req + ' || '
             req = req + ')'
         ## then user requirement
             if self.EDG_requirements:
               req = req +  ' && ' + self.EDG_requirements
             req = req + ';\n' 
        jdl.write(req)

        jdl.write('VirtualOrganisation = "' + self.VO + '";\n')

        if ( self.EDG_retry_count ):               
            jdl.write('RetryCount = '+self.EDG_retry_count+';\n')
            pass

        jdl.close()
        return
