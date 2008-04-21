from SchedulerGrid import SchedulerGrid
from JobList import JobList
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from osg_bdii import *
import time
import common
import popen2
import os
from BlackWhiteListParser import BlackWhiteListParser
import CondorGLoggingInfo

# This class was originally SchedulerCondor_g. For a history of this code, see that file.

import pdb # Use while debugging

__revision__ = "$Id: SchedulerCondorCommon.py,v 1.2.2.2 2008/04/18 22:01:48 ewv Exp $"
__version__ = "$Revision: 1.2.2.2 $"

class SchedulerCondorCommon(SchedulerGrid):
    def __init__(self,name):
        SchedulerGrid.__init__(self,name)

        # check for locally running condor scheduler
        cmd = 'ps xau | grep -i condor_schedd | grep -v grep'
        cmd_out = runCommand(cmd)
        if cmd_out == None:
            msg  = '[Condor-G Scheduler]: condor_schedd is not running on this machine.\n'
            msg += '[Condor-G Scheduler]: Please use another machine with installed condor and running condor_schedd or change the Scheduler in your crab.cfg.'
            common.logger.debug(2,msg)
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
            common.logger.debug(2,msg)
            raise CrabException(msg)

        self.checkExecutableInPath('condor_config_val')
        self.checkCondorVariablePointsToFile('GRIDMANAGER')
        self.checkCondorVariablePointsToFile('GT2_GAHP',alternate_name='GAHP')
        self.checkCondorVariablePointsToFile('GRID_MONITOR')
        self.checkCondorVariableIsTrue('ENABLE_GRID_MONITOR')

        max_submit = self.queryCondorVariable('GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE',100).strip()
        max_pending = self.queryCondorVariable('GRIDMANAGER_MAX_PENDING_SUBMITS_PER_RESOURCE','Unlimited').strip()

        msg  = '[Condor-G Scheduler]\n'
        msg += 'Maximal number of jobs submitted to the grid   : GRIDMANAGER_MAX_SUBMITTED_JOBS_PER_RESOURCE  = '+max_submit+'\n'
        msg += 'Maximal number of parallel submits to the grid : GRIDMANAGER_MAX_PENDING_SUBMITS_PER_RESOURCE = '+max_pending+'\n'
        msg += 'Ask the administrator of your local condor installation to increase these variables to enable more jobs to be executed on the grid in parallel.\n'
        common.logger.debug(2,msg)

        # create hash
        self.hash = makeCksum(common.work_space.cfgFileName())

        return

    #def getCEfromSE(self, seSite):
      ## returns the ce including jobmanager
      #ces = jm_from_se_bdii(seSite)

      ## mapping ce_hostname to full ce name including jobmanager
      #ce_hostnames = {}
      #for ce in ces :
        #ce_hostnames[ce.split(':')[0].strip()] = ce

      #oneSite=''
      #if len(ce_hostnames.keys()) == 1:
        #oneSite=ce_hostnames[ce_hostnames.keys()[0]]
      #elif len(ce_hostnames.keys()) > 1:
        #if self.EDG_ce_white_list and len(self.EDG_ce_white_list) == 1 and self.EDG_ce_white_list[0] in ce_hostnames.keys():
          #oneSite = self.EDG_ce_white_list[0]
        #else :
          #msg  = '[Condor-G Scheduler]: More than one Compute Element (CE) is available for job submission.\n'
          #msg += '[Condor-G Scheduler]: Please select one of the following CEs:\n'
          #msg += '[Condor-G Scheduler]:'
          #for host in ce_hostnames.keys() :
            #msg += ' ' + host
          #msg += '\n'
          #msg += '[Condor-G Scheduler]: and enter this CE in the ce_white_list variable of the [EDG] section in your crab.cfg.\n'
          #common.logger.debug(2,msg)
          #raise CrabException(msg)
      #else :
        #raise CrabException('[Condor-G Scheduler]: CE hostname(s) for SE '+seSite+' could not be determined from BDII.')

      #return oneSite

    def checkExecutableInPath(self, name):
        # check if executable is in PATH
        cmd = 'which '+name
        cmd_out = runCommand(cmd)
        if cmd_out == None:
            msg  = '[Condor-G Scheduler]: '+name+' is not in the $PATH on this machine.\n'
            msg += '[Condor-G Scheduler]: Please use another machine with installed condor or change the Scheduler in your crab.cfg.'
            common.logger.debug(2,msg)
            raise CrabException(msg)

    def checkCondorVariablePointsToFile(self, name, alternate_name=None):
        ## check for condor variable
        cmd = 'condor_config_val '+name
        cmd_out = runCommand(cmd)
        if alternate_name and not cmd_out:
            cmd = 'condor_config_val '+alternate_name
            cmd_out = runCommand(cmd)
        if cmd_out:
            cmd_out = string.strip(cmd_out)
        if not cmd_out or not os.path.isfile(cmd_out) :
            msg  = '[Condor-G Scheduler]: the variable '+name+' is not properly set for the condor installation on this machine.\n'
            msg += '[Condor-G Scheduler]: Please ask the administrator of the local condor installation to set the variable '+name+' properly, ' + \
            'use another machine with properly installed condor or change the Scheduler in your crab.cfg.'
            common.logger.debug(2,msg)
            raise CrabException(msg)

    def checkCondorVariableIsTrue(self, name):
        ## check for condor variable
        cmd = 'condor_config_val '+name
        cmd_out = runCommand(cmd)
        if cmd_out == 'TRUE' :
            msg  = '[Condor-G Scheduler]: the variable '+name+' is not set to true for the condor installation on this machine.\n'
            msg += '[Condor-G Scheduler]: Please ask the administrator of the local condor installation to set the variable '+name+' to true,',
            'use another machine with properly installed condor or change the Scheduler in your crab.cfg.'
            common.logger.debug(2,msg)
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
        SchedulerGrid.configure(self,cfg_params)

        # init BlackWhiteListParser
        #self.blackWhiteListParser = BlackWhiteListParser(cfg_params)

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
          common.logger.debug(2,msg)
        except KeyError:
          self.batchsystem = ''

        # check if one and only one entry is in $CE_WHITELIST

        # redo this with SchedulerGrid SE list

        #try:
            #tmpGood = string.split(cfg_params['EDG.se_white_list'],',')
        #except KeyError:
            #msg  = '[Condor-G Scheduler]: destination site is not selected properly.\n'
            #msg += '[Condor-G Scheduler]: Please select your destination site and only your destination site in the SE_white_list variable of the [EDG] section in your crab.cfg.'
            #common.logger.debug(2,msg)
            #raise CrabException(msg)

        #if len(tmpGood) != 1 :
            #msg  = '[Condor-G Scheduler]: destination site is not selected properly.\n'
            #msg += '[Condor-G Scheduler]: Please select your destination site and only your destination site in the SE_white_list variable of the [EDG] section in your crab.cfg.'
            #common.logger.debug(2,msg)
            #raise CrabException(msg)

        try:
            self.UseGT4 = cfg_params['USER.use_gt_4'];
        except KeyError:
            self.UseGT4 = 0;

        # added here because checklistmatch is not used
        self.checkProxy()
        self.environment_unique_identifier = 'GLOBUS_GRAM_JOB_CONTACT'

        self.datasetPath = ''
        try:
            tmp =  cfg_params['CMSSW.datasetpath']
            if string.lower(tmp)=='none':
                self.datasetPath = None
                self.selectNoInput = 1
            else:
                self.datasetPath = tmp
                self.selectNoInput = 0
        except KeyError:
            msg = "Error: datasetpath not defined "
            raise CrabException(msg)

        return

    def sched_parameter(self,i,task):
      """
      Returns scheduler-specific parameters
      """
      jobParams = ''
      globusRSL = self.GLOBUS_RSL
      if (self.EDG_clock_time):
        globusRSL += '(maxWalltime='+self.EDG_clock_time+')'
      if (globusRSL != ''):
        jobParams +=  'globusrsl = ' + globusRSL + '; '

      return jobParams

    def decodeLogInfo(self, file):
        """
        Parse logging info file and return main info
        """
        loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
        reason = loggingInfo.decodeReason(file)
        return reason

    def wsSetupEnvironment(self):
      """
      Returns part of a job script which does scheduler-specific work.
      """
      txt = SchedulerGrid.wsSetupEnvironment(self)

      if int(self.copy_data) == 1:
        if self.SE:
          txt += 'export SE='+self.SE+'\n'
          txt += 'echo "SE = $SE"\n'
        if self.SE_PATH:
          if self.SE_PATH[-1] != '/':
            self.SE_PATH = self.SE_PATH + '/'
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

    def listMatch(self, nj):
        """
        Check the compatibility of available resources
        """
        #self.checkProxy()
        return [""]

    def userName(self):
        """ return the user name """
        self.checkProxy()
        return runCommand("voms-proxy-info -identity")

    def ce_list(self):
      """
      Returns string with requirement CE related, dummy for now
      """
      req = ''

      return req,self.EDG_ce_white_list,self.EDG_ce_black_list
