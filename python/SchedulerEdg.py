from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import os, sys, tempfile

class SchedulerEdg(Scheduler):
    def __init__(self):
        Scheduler.__init__(self,"EDG")
        return

    def configure(self, cfg_params):

        try: self.edg_ui_cfg = cfg_params["EDG.rb_config"]
        except KeyError: self.edg_ui_cfg = ''

        try: self.edg_config = cfg_params["EDG.config"]
        except KeyError: self.edg_config = ''

        try: self.edg_config_vo = cfg_params["EDG.config_vo"]
        except KeyError: self.edg_config_vo = 'cms'

        try: self.LCG_version = cfg_params["EDG.lcg_version"]
        except KeyError: self.LCG_version = '2'

        try: self.EDG_requirements = cfg_params['EDG.requirements']
        except KeyError: self.EDG_requirements = ''

        try: self.EDG_retry_count = cfg_params['EDG.retry_count']
        except KeyError: self.EDG_retry_count = ''

        try:
            self.VO = cfg_params['EDG.virtual_organization']
        except KeyError:
            msg = 'EDG.virtual_organization is mandatory.'
            raise CrabException(msg)

        
        #self.scripts_dir = common.bin_dir + '/scripts'
        #self.cmd_prefix = 'edg'
        #if common.LCG_version == '0' : self.cmd_prefix = 'dg'

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

        return
    

    def submit(self, nj):
        """Submit one EDG job."""

        jid = None
        jdl = common.job_list[nj].jdlFilename()
        id_tmp = tempfile.mktemp()
        edg_ui_cfg_opt = ' '
        if self.edg_config:
          edg_ui_cfg_opt = ' -c ' + self.edg_config + ' '
        if self.edg_config_vo: 
          edg_ui_cfg_opt = edg_ui_cfg_opt + ' --config-vo ' + self.edg_config_vo + ' '
        cmd = 'edg-job-submit -o ' + id_tmp + edg_ui_cfg_opt + jdl 
        cmd_out = runCommand(cmd)
        if cmd_out != None:
          idfile = open(id_tmp)
          jid_line = idfile.readline()
          while jid_line[0] == '#':
            jid_line = idfile.readline()
          jid = string.strip(jid_line)
          os.unlink(id_tmp)
          pass
        return jid

    def queryStatus(self, id):
        """ Query a status of the job with id """
        log = Logger.getInstance()
        cmd0 = 'edg-job-status '
        cmd = cmd0 + id
        cmd_out = runCommand(cmd)
        if cmd_out == None:
            log.message('Error. No output from `'+cmd+'`')
            return None
        # parse output
        status_prefix = 'Status                  =    '
        status_index = string.find(cmd_out, status_prefix)
        if status_index == -1:
            log.message('Error. Bad output of `'+cmd0+'`:\n'+cmd_out)
            return None
        status = cmd_out[(status_index+len(status_prefix)):]
        nl = string.find(status,'\n')
        return self.EDG2CMSprodStatus(status[0:nl])

    def EDG2CMSprodStatus(self, edg_status):
        edg_st = string.lower(string.strip(edg_status))
        if edg_st == 'submitted' or edg_st == 'waiting' or \
           edg_st == 'ready' or edg_st == 'scheduled':
            return 'Pending'
        if edg_st == 'running' or edg_st == 'done' or edg_st == 'chkpt':
            return 'Running'
        if edg_st == 'done (cancelled)':
            return 'Canceled'
        if edg_st == 'aborted': return 'Aborted'
        if edg_st == 'outputready': return 'OutputReady'
        if edg_st == 'cleared': return 'Finished'
        return edg_st

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """
        cmd = 'edg-job-status '+id
        cmd_out = runCommand(cmd)
        return cmd_out

    def getOutput(self, id):
        """ Get output for a finished job with id."""
        cmd = 'edg-job-get-output --dir ' + common.res_dir + ' '+id
        cmd_out = runCommand(cmd)
        return cmd_out

    def cancel(self, id):
        """ Cancel the EDG job with id """
        cmd = 'edg-job-cancel --noint ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

    def checkProxy(self):
        """
        Function to check the Globus proxy.
        """
        cmd = 'grid-proxy-info -timeleft'
        cmd_out = runCommand(cmd,0)
        ok = 1
        timeleft = -999
        try: timeleft = int(cmd_out)
        except ValueError: ok=0
        except TypeError: ok=0
        if timeleft < 1:  ok=0

        if ok==0:
            msg = 'No valid proxy found !\n'
            msg += "Please do 'grid-proxy-init'"
            raise CrabException(msg)
        return
    
    def isInputReady(self, nj):
        return 1
    
    def createJDL(self, nj):
        """
        Create a JDL-file for EDG.
        """

        job = common.job_list[nj]
        jbt = job.type()
#        jbt.loadJobInfo()
        inp_sandbox = jbt.inputSandbox(nj)
        out_sandbox = jbt.outputSandbox(nj)
        inp_storage_subdir = ''#jbt.inputStorageSubdir()
        
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

        inp_box = 'InputSandbox = { '
        inp_box = inp_box + '"' + script + '",'

        if inp_sandbox != None:
            for fl in inp_sandbox:
                inp_box = inp_box + ' "' + fl + '",'
                pass
            pass

        #if common.use_jam:
        #   inp_box = inp_box+' "'+common.bin_dir+'/'+common.run_jam+'",'

        # ??? Should be local, i.e. self.additional_inbox_files
        #     and filled in ctor from cfg_params
        #for addFile in common.additional_inbox_files:
        #    addFile = os.path.abspath(addFile)
        #    inp_box = inp_box+' "'+addFile+'",'
        #    pass

        if inp_box[-1] == ',' : inp_box = inp_box[:-1]
        inp_box = inp_box + ' };\n'
        jdl.write(inp_box)

        jdl.write('StdOutput     = "' + job.stdout() + '";\n')
        jdl.write('StdError      = "' + job.stderr() + '";\n')


### SL check if stdout==stderr: in case put just one in the out_box
        if job.stdout() == job.stderr():
          out_box = 'OutputSandbox = { "' + \
                    job.stdout() + '", ".BrokerInfo",'
        else:
          out_box = 'OutputSandbox = { "' + \
                    job.stdout() + '", "' + \
                    job.stderr() + '", ".BrokerInfo",'
          pass

        #if common.flag_return_data :
        #    for fl in job.outputDataFiles():
        #        out_box = out_box + ' "' + fl + '",'
        #        pass
        #    pass

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
