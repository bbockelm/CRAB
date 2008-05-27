from WorkSpace import WorkSpace
from JobList import JobList
from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
import common
import Scram
import string,os

class ScriptWriter:
    def __init__(self, cfg_params, template): 
        # pattern -> action
        self.actions = {
            'title'                       : self.title_,
            'untar_software'              : self.untarSoftware_,
            'setup_scheduler_environment' : self.setupSchedulerEnvironment_,
            'setup_jobtype_environment'   : self.setupJobTypeEnvironment_,
            'copy_input'                  : self.copyInput_,
            'rewrite_cmssw_cfg'           : self.rewriteCMSSWcfg_,
            'build_executable'            : self.buildExe_,
            'run_executable'              : self.runExe_,
            'rename_output'               : self.renameOutput_,
            'copy_output'                 : self.copyOutput_,
            'modify_report'               : self.modifyReport_,
            'func_exit'                   : self.func_exit_
            }

        if os.path.isfile("./"+template):
            self.template = "./"+template
        elif os.getenv('CRABDIR') and os.path.isfile(os.getenv('CRABDIR')+'/python/'+template):
            self.template = os.getenv('CRABDIR')+'/python/'+template
        else:
            raise CrabException("No crab_template.sh found!")
        self.nj = -1     # current job number

        try:
            self.scram = Scram.Scram(None)
            self.CMSSWversion = self.scram.getSWVersion()
            parts = self.CMSSWversion.split('_')
            self.CMSSW_major = int(parts[1])
            self.CMSSW_minor = int(parts[2])
            self.CMSSW_patch = int(parts[3])
        except:
            raise CrabException("Could not determine CMSSW version")
        self.debug_pset=''
        debug = cfg_params.get('USER.debug_pset',False)
        if debug: self.debug_pset='--debug'
 
        return

    def setAction(self, pattern, action):
        self.actions[pattern] = action
        return

    def modifyTemplateScript(self):
        """
        Create a script from scratch.
        """

        tpl = open(self.template, 'r')
        script = open(common._db.queryTask('scriptName'),'w')

        for line in tpl:
            if len(line) > 6 and line[:6] == '#CRAB ':
                act_str = string.strip(line[6:])
                try:
                    action = self.actions[act_str]
                except KeyError:
                    continue

                if action:
                    txt = action()
                    script.write(txt)
                    pass
                else:
                    script.write(line)
                pass
            else:
                script.write(line)
                pass
            pass

        script.close()
        tpl.close()
        return

    def title_(self):
        txt = '# This script was generated by '+common.prog_name
        txt += ' (version '+common.prog_version_str+').\n'
        return txt


    ### FEDE ###

    def untarSoftware_(self):
        """
        Returns part of a job script which untar CMSSW software.
        """
        jbt = common.job_list.type()

        txt = jbt.wsUntarSoftware(self.nj)

        #txt += 'executable='+exe+'\n'
        return txt

    ###########################################

    def setupSchedulerEnvironment_(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        txt = common.scheduler.wsSetupEnvironment()
        return txt

    def setupJobTypeEnvironment_(self):
        """
        Returns part of a job script which does jobtype-specific work.
        """
        jbt = common.job_list.type()
        txt = jbt.wsSetupEnvironment(self.nj)
        return txt

    def buildExe_(self):
        """
        Returns part of a job script which builds the binary executable.
        """
        jbt = common.job_list.type()

        txt = jbt.wsBuildExe(self.nj)

        job = common.job_list[self.nj]
        exe = job.type().executableName()

        txt += 'executable='+exe+'\n'
        return txt

    def runExe_(self):
        """
        Returns part of a job script which executes the application.
        """
        job = common.job_list[self.nj]
        args = job.type().executableArgs()
        return '$executable '+args+'\n'

    def renameOutput_(self):
        """
        Returns part of a job script which renames output files.
        """
        jbt = common.job_list.type()
        txt = '\n'
        txt += jbt.wsRenameOutput(self.nj)
        return txt

    def copyInput_(self):
        """
        Returns part of a job script which copies input files from SE.
        """
        txt = common.scheduler.wsCopyInput()
        return txt

    def copyOutput_(self):
        """
        Returns part of a job script which copies output files to SE.
        """
        txt = common.scheduler.wsCopyOutput()
        return txt

    def modifyReport_(self):
        """
        Returns part of a job script which modifies the FrameworkJobReport.
        """
        jbt = common.job_list.type()
        txt = jbt.modifyReport(self.nj)
        return txt

    def cleanEnv_(self):
        """
        In OSG environment this function removes the WORKING_DIR
        """
        jbt = common.job_list.type()
        txt = jbt.cleanEnv()
        return txt

    def func_exit_(self):
        """
        Returns part of a job script which does scheduler-specific 
        output checks and management.
        """
        txt = common.scheduler.wsExitFunc()
        return txt

    def rewriteCMSSWcfg_(self):
        """
        Returns part of the script that runs writeCfg.py on the WN
        """
        # FUTURE: This function tests the CMSSW version. Can be simplified as we drop support for old versions
        txt = "# Rewrite cfg for this job\n"

        if (self.CMSSW_major >= 2 and self.CMSSW_minor >= 1) or self.CMSSW_major > 2: #  py in,  py out for 2_1_x
          txt += "echo  $RUNTIME_AREA/writeCfg.py "+str(self.debug_pset)+" pset.py pset.py\n"
          txt += "python $RUNTIME_AREA/writeCfg.py "+str(self.debug_pset)+" pset.py pset.py\n"
        elif self.CMSSW_major >= 2:                                                   # cfg in,  py out for 2_0_x
          txt += "echo  $RUNTIME_AREA/writeCfg.py "+str(self.debug_pset)+" pset.cfg pset.py\n"
          txt += "python $RUNTIME_AREA/writeCfg.py "+str(self.debug_pset)+" pset.cfg pset.py\n"
        else:                                                                         # cfg in, cfg out for 1_x_y
          txt += "echo  $RUNTIME_AREA/writeCfg.py "+str(self.debug_pset)+" pset.cfg pset.cfg\n"
          txt += "python $RUNTIME_AREA/writeCfg.py "+str(self.debug_pset)+" pset.cfg pset.cfg\n"

        return txt
