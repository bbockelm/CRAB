from WorkSpace import WorkSpace
from JobList import JobList
from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
import common

import string,os

class ScriptWriter:
    def __init__(self, template, output_troncate_flag): ## added by Matty
        # pattern -> action
        ### FEDE added modify_report FOR DBS OUTPUT PUBLICATION
        self.actions = {
            'title'                       : self.title_,
            'setup_scheduler_environment' : self.setupSchedulerEnvironment_,
            'setup_jobtype_environment'   : self.setupJobTypeEnvironment_,
            'copy_input'                  : self.copyInput_,
            'build_executable'            : self.buildExe_,
            'run_executable'              : self.runExe_,
            'rename_output'               : self.renameOutput_,
            'copy_output'                 : self.copyOutput_,
            #'register_output'             : self.registerOutput_,
            'modify_report'               : self.modifyReport_,
            'clean_env'                   : self.cleanEnv_,
            'check_output_limit'          : self.checkOut_
            }
        
        if os.path.isfile("./"+template):
            self.template = "./"+template
        elif os.getenv('CRABDIR') and os.path.isfile(os.getenv('CRABDIR')+'/python/'+template):
            self.template = os.getenv('CRABDIR')+'/python/'+template
        else:
            raise CrabException("No crab_template.sh found!")
        self.nj = -1     # current job number

        self.output_troncate_flag = output_troncate_flag

        return

    def setAction(self, pattern, action):
        self.actions[pattern] = action
        return
    
    def modifyTemplateScript(self):
        """
        Create a script from scratch.
        """
        
        tpl = open(self.template, 'r')
        script = open(common.taskDB.dict('ScriptName'),'w')

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

    #def registerOutput_(self):
    #    """
    #    Returns part of a job script which registers output files to RLS catalog.
    #    """
    #    txt = ''
    #    return txt

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

    def checkOut_(self):
        """
        With glite check if the output is too big
        """
        txt = "\n"
        if self.output_troncate_flag == 1:
            limit = 55000000 ##105 MB
            jbt = common.job_list.type()
            txt = jbt.checkOut(limit)
        return txt
