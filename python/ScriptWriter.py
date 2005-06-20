from WorkSpace import WorkSpace
from JobList import JobList
from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
import common

import string

class ScriptWriter:
    def __init__(self, template):
        # pattern -> action
        self.actions = {
            'title'                       : self.title_,
            'setup_scheduler_environment' : self.setupSchedulerEnvironment_,
            'setup_jobtype_environment'   : self.setupJobTypeEnvironment_,
            'build_executable'            : self.buildExe_,
            'run_executable'              : self.runExe_,
            'rename_output'               : self.renameOutput_,
            'register_results'            : None
            }
        
        self.template = template
        self.nj = -1     # current job number
        return

    def setAction(self, pattern, action):
        self.actions[pattern] = action
        return
    
    def modifyTemplateScript(self, nj):
        """
        Create a script from scratch.
        """
        self.nj = nj
        
        tpl = open(self.template, 'r')
        script = open(common.job_list[nj].scriptFilename(), 'w')

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
        Returns part of a job script which does scheduler-specific work.
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
        return '$executable\n'

    def renameOutput_(self):
        """
        Returns part of a job script which renames output files.
        """
        jbt = common.job_list.type()
        txt = '\n'
        txt += jbt.wsRenameOutput(self.nj)
        return txt
