"""
Scheduler implementation used by CondorG and Glidein (Common parts)
This class was originally SchedulerCondor_g. For a history of this code, see that file.
"""

from SchedulerGrid import SchedulerGrid
from crab_exceptions import CrabException
from crab_util import runCommand

from ProdCommon.BDII.Bdii import getJobManagerList, listAllCEs
from BlackWhiteListParser import CEBlackWhiteListParser

import Scram
import CondorGLoggingInfo
import common

import popen2
import os
import sha # Good for python 2.4, replaced with hashlib in 2.5

__revision__ = "$Id: SchedulerCondorCommon.py,v 1.24 2008/08/05 19:50:28 ewv Exp $"
__version__ = "$Revision: 1.24 $"

class SchedulerCondorCommon(SchedulerGrid):
    """
    Scheduler implementation used by CondorG and Glidein (Common parts)
    """

    def __init__(self, name):
        SchedulerGrid.__init__(self, name)

        # check for locally running condor scheduler
        cmd = 'ps xau | grep -i condor_schedd | grep -v grep'
        cmd_out = runCommand(cmd)
        if cmd_out == None:
            msg  = '[Condor-G Scheduler]: condor_schedd is not running on this machine.\n'
            msg += '[Condor-G Scheduler]: Please use a machine with condor installed and running condor_schedd\n'
            msg += '[Condor-G Scheduler]: or change the Scheduler in your crab.cfg.'
            common.logger.debug(2, msg)
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
            msg += '[Condor-G Scheduler]: Please use a machine with a properly installed condor\n'
            msg += '[Condor-G Scheduler]: or change the Scheduler in your crab.cfg.'
            common.logger.debug(2, msg)
            raise CrabException(msg)

        self.checkExecutableInPath('condor_config_val')
        self.checkCondorVariablePointsToFile('GRIDMANAGER')
        self.checkCondorVariablePointsToFile('GT2_GAHP', alternate_name='GAHP')
        self.checkCondorVariablePointsToFile('GRID_MONITOR')
        self.checkCondorVariableIsTrue('ENABLE_GRID_MONITOR')

        max_submit = self.queryCondorVariable('GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE', 100).strip()
        max_pending = self.queryCondorVariable('GRIDMANAGER_MAX_PENDING_SUBMITS_PER_RESOURCE', 'Unlimited').strip()

        msg  = '[Condor-G Scheduler]\n'
        msg += 'Maximum number of jobs submitted to the grid   :'
        msg += 'GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE  = '+max_submit+'\n'
        msg += 'Maximum number of parallel submits to the grid :'
        msg += 'GRIDMANAGER_MAX_PENDING_SUBMITS_PER_RESOURCE = '+max_pending+'\n'
        msg += 'Increase these variables to enable more jobs to be executed on the grid in parallel.\n'
        common.logger.debug(2, msg)

        return

    def checkExecutableInPath(self, name):
        """
        check if executable is in PATH
        """

        cmd = 'which '+name
        cmd_out = runCommand(cmd)
        if cmd_out == None:
            msg  = '[Condor-G Scheduler]: '+name+' is not in the $PATH on this machine.\n'
            msg += '[Condor-G Scheduler]: Please use a machine with a properly installed condor\n'
            msg += '[Condor-G Scheduler]: or change the Scheduler in your crab.cfg.'
            common.logger.debug(2, msg)
            raise CrabException(msg)

    def checkCondorVariablePointsToFile(self, name, alternate_name=None):
        """
        check for condor variable
        """

        cmd = 'condor_config_val '+name
        cmd_out = runCommand(cmd)
        if alternate_name and not cmd_out:
            cmd = 'condor_config_val '+alternate_name
            cmd_out = runCommand(cmd)
        if cmd_out:
            cmd_out = cmd_out.strip()
        if not cmd_out or not os.path.isfile(cmd_out) :
            msg  = '[Condor-G Scheduler]: the variable '+name+' is not properly set for the condor installation.\n'
            msg += '[Condor-G Scheduler]: Please ask the administrator of the local condor installation '
            msg += 'to set the variable '+name+' properly, '
            msg += 'use another machine with a properly installed condor or change the Scheduler in your crab.cfg.'
            common.logger.debug(2, msg)
            raise CrabException(msg)

    def checkCondorVariableIsTrue(self, name):
        """
        check for condor variable
        """

        cmd = 'condor_config_val '+name
        cmd_out = runCommand(cmd)
        if cmd_out == 'TRUE' :
            msg  = '[Condor-G Scheduler]: the variable '+name+' is not set to true for the condor installation.\n'
            msg += '[Condor-G Scheduler]: Please ask the administrator of the local condor installation '
            msg += 'to set the variable '+name+' to true, '
            msg += 'use another machine with a properly installed condor or change the Scheduler in your crab.cfg.'
            common.logger.debug(2, msg)
            raise CrabException(msg)

    def queryCondorVariable(self, name, default):
        """
        check for condor variable
        """

        cmd = 'condor_config_val '+name
        out = popen2.Popen3(cmd, 1)
        exit_code = out.wait()
        cmd_out = out.fromchild.readline().strip()
        if exit_code != 0 :
            cmd_out = str(default)

        return cmd_out

    def configure(self, cfg_params):
        """
        Configure the scheduler with the config settings from the user
        """

        SchedulerGrid.configure(self, cfg_params)

        # init BlackWhiteListParser
        self.ceBlackWhiteListParser = CEBlackWhiteListParser(cfg_params)

        try:
            self.GLOBUS_RSL = cfg_params['CONDORG.globus_rsl']
        except KeyError:
            self.GLOBUS_RSL = ''

        # Provide an override for the batchsystem that condor_g specifies as a grid resource.
        # this is to handle the case where the site supports several batchsystem but bdii
        # only allows a site to public one.
        try:
            self.batchsystem = cfg_params['CONDORG.batchsystem']
            msg = '[Condor-G Scheduler]: batchsystem overide specified in your crab.cfg'
            common.logger.debug(2, msg)
        except KeyError:
            self.batchsystem = ''

        taskHash = sha.new(common._db.queryTask('name')).hexdigest()
        self.environment_unique_identifier = 'https://' + self.name() + '/' + taskHash + '/${NJob}'
        msg = 'JobID for ML monitoring is created for OSG scheduler: '+self.environment_unique_identifier
        common.logger.debug(5, msg)
        self.datasetPath = ''

        try:
            tmp =  cfg_params['CMSSW.datasetpath']
            if tmp.lower() == 'none':
                self.datasetPath = None
                self.selectNoInput = 1
            else:
                self.datasetPath = tmp
                self.selectNoInput = 0
        except KeyError:
            msg = "Error: datasetpath not defined "
            raise CrabException(msg)

        return


    def realSchedParams(self, cfg_params):
        """
        Return dictionary with specific parameters, to use
        with real scheduler
        """

        tmpDir = os.path.join(common.work_space.shareDir(),'.condor_temp')
        params = {'tmpDir':tmpDir}
        return  params


    def sched_parameter(self, i, task):
        """
        Returns scheduler-specific parameters
        """
        jobParams = ''

        return jobParams


    def decodeLogInfo(self, theFile):
        """
        Parse logging info file and return main info
        """

        loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
        reason = loggingInfo.decodeReason(theFile)
        return reason


    def listMatch(self, seList, full, onlyOSG=True):
        """
        Check the compatibility of available resources
        """

        scram = Scram.Scram(None)
        versionCMSSW = scram.getSWVersion()
        arch = scram.getArch()

        if self.selectNoInput:
            availCEs = listAllCEs(versionCMSSW, arch, onlyOSG=onlyOSG)
        else:
            seDest = self.blackWhiteListParser.cleanForBlackWhiteList(seList, "list")
            availCEs = getJobManagerList(seDest, versionCMSSW, arch, onlyOSG=onlyOSG)

        uniqCEs = []
        for ce in availCEs:
            if ce not in uniqCEs:
                uniqCEs.append(ce)

        ceDest = self.ceBlackWhiteListParser.cleanForBlackWhiteList(uniqCEs, "list")

        return ceDest

    def userName(self):
        """
        return the user name
        """

        return runCommand("voms-proxy-info -identity")

    def ce_list(self):
        """
        Returns string with requirement CE related, dummy for now
        """

        req = ''

        return req, self.EDG_ce_white_list, self.EDG_ce_black_list

    def seListToCElist(self, seList, onlyOSG=True):
        """
        Convert the list of SEs into a list of CEs
        """

        ceDest = self.listMatch(seList, onlyOSG)

        if (not ceDest):
            msg = 'No sites found hosting the data or all sites blocked by CE/SE white/blacklisting'
            print msg
            raise CrabException(msg)

        return ceDest


    def wsExitFunc(self):
        """
        Returns the part of the job script which runs prior to exit
        """

        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()
        txt += '    echo "JOB_EXIT_STATUS = $job_exit_code"\n'
        txt += '    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'
        return txt
