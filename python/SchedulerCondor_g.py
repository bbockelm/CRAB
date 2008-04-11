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

import pdb # Use while debugging

__revision__ = "$Id: SchedulerCondor_g.py,v 1.90 2008/04/10 13:50:41 ewv Exp $"
__version__ = "$Revision: 1.90 $"

class SchedulerCondor_g(SchedulerGrid):
    def __init__(self):
        SchedulerGrid.__init__(self,"CONDOR_G")

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

    def getCEfromSE(self, seSite):
      # returns the ce including jobmanager
      ces = jm_from_se_bdii(seSite)

      # mapping ce_hostname to full ce name including jobmanager
      ce_hostnames = {}
      for ce in ces :
        ce_hostnames[ce.split(':')[0].strip()] = ce

      oneSite=''
      if len(ce_hostnames.keys()) == 1:
        oneSite=ce_hostnames[ce_hostnames.keys()[0]]
      elif len(ce_hostnames.keys()) > 1:
        if self.EDG_ce_white_list and len(self.EDG_ce_white_list) == 1 and self.EDG_ce_white_list[0] in ce_hostnames.keys():
          oneSite = self.EDG_ce_white_list[0]
        else :
          msg  = '[Condor-G Scheduler]: More than one Compute Element (CE) is available for job submission.\n'
          msg += '[Condor-G Scheduler]: Please select one of the following CEs:\n'
          msg += '[Condor-G Scheduler]:'
          for host in ce_hostnames.keys() :
            msg += ' ' + host
          msg += '\n'
          msg += '[Condor-G Scheduler]: and enter this CE in the ce_white_list variable of the [EDG] section in your crab.cfg.\n'
          common.logger.debug(2,msg)
          raise CrabException(msg)
      else :
        raise CrabException('[Condor-G Scheduler]: CE hostname(s) for SE '+seSite+' could not be determined from BDII.')

      return oneSite

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

        try:
            tmpGood = string.split(cfg_params['EDG.se_white_list'],',')
        except KeyError:
            msg  = '[Condor-G Scheduler]: destination site is not selected properly.\n'
            msg += '[Condor-G Scheduler]: Please select your destination site and only your destination site in the SE_white_list variable of the [EDG] section in your crab.cfg.'
            common.logger.debug(2,msg)
            raise CrabException(msg)

        if len(tmpGood) != 1 :
            msg  = '[Condor-G Scheduler]: destination site is not selected properly.\n'
            msg += '[Condor-G Scheduler]: Please select your destination site and only your destination site in the SE_white_list variable of the [EDG] section in your crab.cfg.'
            common.logger.debug(2,msg)
            raise CrabException(msg)

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
      Returns file with scheduler-specific parameters
      """
      lastDest=''
      first = []
      last  = []

      seDest=self.blackWhiteListParser.cleanForBlackWhiteList(eval(task.jobs[i-1]['dlsDestination']))
      ceDest = self.getCEfromSE(seDest)

      jobParams = "globusscheduler = "+ceDest+":2119/jobmanager-condor; "
      globusRSL = self.GLOBUS_RSL
      if (self.EDG_clock_time):
        globusRSL += '(maxWalltime='+self.EDG_clock_time+')'
      if (globusRSL != ''):
        jobParams +=  'globusrsl = ' + globusRSL + '; '

      common._db.updateTask_({'jobType':jobParams})
      return jobParams # Not sure I even need to return anything

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
        #self.checkProxy()
        return 1

    def userName(self):
        """ return the user name """
        self.checkProxy()
        return runCommand("voms-proxy-info -identity")

    #def getAttribute(self, id, attr):
        #return self.getStatusAttribute_(id, attr)

    #def getAttribute(self, id, attr):
        #return self.getStatusAttribute_(id, attr)

    #def getExitStatus(self, id):
        #return self.getStatusAttribute_(id, 'exit_code')

    #def queryStatus(self, id):
        #return self.getStatusAttribute_(id, 'status')

    #def queryDest(self, id):
        #return self.getStatusAttribute_(id, 'destination')


    #def getStatusAttribute_(self, id, attr):
        #""" Query a status of the job with id """

        #result = ''

        #if ( attr == 'exit_code' ) :
            #jobnum_str = '%06d' % (int(id))
            ## opts = common.work_space.loadSavedOptions()
            #base = string.upper(common.taskDB.dict("jobtype"))
            #log_file = common.work_space.resDir() + base + '_' + jobnum_str + '.stdout'
            #logfile = open(log_file)
            #log_line = logfile.readline()
            #while log_line :
                #log_line = log_line.strip()
                #if log_line.startswith('JOB_EXIT_STATUS') :
                    #log_line_split = log_line.split()
                    #result = log_line_split[2]
                    #pass
                #log_line = logfile.readline()
            #result = ''
        #elif ( attr == 'status' ) :
            #schedd    = id.split('//')[0]
            #condor_id = id.split('//')[1]
            #cmd = 'condor_q -name ' + schedd + ' ' + condor_id
            #cmd_out = runCommand(cmd)
            #if cmd_out != None:
                #status_flag = 0
                #for line in cmd_out.splitlines() :
                    #if line.strip().startswith(condor_id.strip()) :
                        #status = line.strip().split()[5]
                        #if ( status == 'I' ):
                            #result = 'Scheduled'
                            #break
                        #elif ( status == 'U' ) :
                            #result = 'Ready'
                            #break
                        #elif ( status == 'H' ) :
                            #result = 'Hold'
                            #break
                        #elif ( status == 'R' ) :
                            #result = 'Running'
                            #break
                        #elif ( status == 'X' ) :
                            #result = 'Cancelled'
                            #break
                        #elif ( status == 'C' ) :
                            #result = 'Done'
                            #break
                        #else :
                            #result = 'Done'
                            #break
                    #else :
                        #result = 'Done'
            #else :
                #result = 'Done'
        #elif ( attr == 'destination' ) :
            #seSite = self.blackWhiteListParser.cleanForBlackWhiteList(common.jobDB.destination(int(id)-1))
            ## if no site was selected during job splitting (datasetPath=None)
            ## set to self.cfg_params['EDG.se_white_list']
            #if self.datasetPath == 'None':
                #seSite = self.cfg_params['EDG.se_white_list']
            #oneSite = self.getCEfromSE(seSite).split(':')[0].strip()
            #result = oneSite
        #elif ( attr == 'reason' ) :
            #result = 'status query'
        #elif ( attr == 'stateEnterTime' ) :
            #result = time.asctime(time.gmtime())
        #return result

    #def queryDetailedStatus(self, id):
        #""" Query a detailed status of the job with id """
        #user = os.environ['USER']
        #cmd = 'condor_q -submitter ' + user
        #cmd_out = runCommand(cmd)
        #return cmd_out

    def ce_list(self):
      """
      Returns string with requirement CE related, dummy for now
      """
      req = ''

      return req,self.EDG_ce_white_list,self.EDG_ce_black_list

    #def createXMLSchScript(self, nj, argsList):
        #"""
        #Create a XML-file for BOSS4.
        #"""

        ## job steering
        #index = nj - 1
        #job = common.job_list[index]
        #jbt = job.type()

        ## input and output sandboxes
        #inp_sandbox = jbt.inputSandbox(index)

        ## title
        #title     = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
        #jt_string = ''

        #xml_fname = str(self.jobtypeName)+'.xml'
        #xml       = open(common.work_space.shareDir()+'/'+xml_fname, 'a')

        ## TaskName
        #dir      = string.split(common.work_space.topDir(), '/')
        #taskName = dir[len(dir)-2]

        #xml.write(str(title))

        ##First check the X509_USER_PROXY. In not there use the default
        #try:
            #x509=os.environ['X509_USER_PROXY']
        #except Exception, ex:
            #import traceback
            #common.logger.debug( 6, str(ex) )
            #common.logger.debug( 6, traceback.format_exc() )
            #x509_cmd = 'ls /tmp/x509up_u`id -u`'
            #x509=runCommand(x509_cmd).strip()
        #xml.write('<task name="' +str(taskName)+ '" sub_path="' +common.work_space.pathForTgz() + 'share/.boss_cache"' + ' task_info="' + str(x509) + '">\n')
        #xml.write(jt_string)

        #xml.write('<iterator>\n')
        #xml.write('\t<iteratorRule name="ITR1">\n')
        #xml.write('\t\t<ruleElement> 1:'+ str(nj) + ' </ruleElement>\n')
        #xml.write('\t</iteratorRule>\n')
        #xml.write('\t<iteratorRule name="ITR2">\n')
        #for arg in argsList:
            #xml.write('\t\t<ruleElement> <![CDATA[\n'+ arg + '\n\t\t]]> </ruleElement>\n')
            #pass
        #xml.write('\t</iteratorRule>\n')
        #xml.write('\t<iteratorRule name="ITR3">\n')
        #xml.write('\t\t<ruleElement> 1:'+ str(nj) + ':1:6 </ruleElement>\n')
        #xml.write('\t</iteratorRule>\n')

        #xml.write('<chain name="' +str(taskName)+'__ITR1_" scheduler="'+str(self.schedulerName)+'">\n')
        ##xmliwrite('<chain scheduler="'+str(self.schedulerName)+'">\n')
        #xml.write(jt_string)


        ##executable

        #script = job.scriptFilename()
        #xml.write('<program>\n')
        #xml.write('<exec> ' + os.path.basename(script) +' </exec>\n')
        #xml.write(jt_string)

        #xml.write('<args> <![CDATA[\n _ITR2_ \n]]> </args>\n')
        #xml.write('<program_types> crabjob </program_types>\n')

        ## input sanbox
        #inp_box = script + ','

        #if inp_sandbox != None:
            #for fl in inp_sandbox:
                #inp_box = inp_box + '' + fl + ','
                #pass
            #pass

        #if inp_box[-1] == ',' : inp_box = inp_box[:-1]
        #inp_box = '<infiles> <![CDATA[\n' + inp_box + '\n]]> </infiles>\n'
        #xml.write(inp_box)

        ## stdout and stderr
        #base = jbt.name()
        #stdout = base + '__ITR3_.stdout'
        #stderr = base + '__ITR3_.stderr'

        #xml.write('<stderr> ' + stderr + '</stderr>\n')
        #xml.write('<stdout> ' + stdout + '</stdout>\n')

        ## output sanbox
        #out_box = stdout + ',' + stderr + ','

        ## Stuff to be returned _always_ via sandbox
        #for fl in jbt.output_file_sandbox:
            #out_box = out_box + '' + jbt.numberFile_(fl, '_ITR1_') + ','
            #pass
        #pass

        #if int(self.return_data) == 1:
            #for fl in jbt.output_file:
                #out_box = out_box + '' + jbt.numberFile_(fl, '_ITR1_') + ','
                #pass
            #pass

        #if out_box[-1] == ',' : out_box = out_box[:-1]
        #out_box = '<outfiles> <![CDATA[\n' + out_box + '\n]]></outfiles>\n'
        #xml.write(out_box)

        #xml.write('<BossAttr> crabjob.INTERNAL_ID=_ITR1_ </BossAttr>\n')

        #xml.write('</program>\n')

        ## start writing of extraTags
        #to_write = ''

        ## extraTag universe
        #to_write += 'universe = "&quot;globus&quot;"\n'

        ## extraTag globusscheduler

        ## use bdii to query ce including jobmanager from site
        ## use first job with non-empty
        #seSite = ''
        #for i in range(nj) :
            #seSite = self.blackWhiteListParser.cleanForBlackWhiteList(common.jobDB.destination(i-1))
            #if seSite != '' :
                #break;
        ## if no site was selected during job splitting (datasetPath=None)
        ## set to self.cfg_params['EDG.se_white_list']
        #if seSite == '' :
            #if self.datasetPath == None :
                #seSite = self.cfg_params['EDG.se_white_list']
            #else :
                #msg  = '[Condor-G Scheduler]: Jobs cannot be submitted to site ' + self.cfg_params['EDG.se_white_list'] + ' because the dataset ' + self.datasetPath + ' is not available at this site.\n'
                #common.logger.debug(2,msg)
                #raise CrabException(msg)

        #oneSite = self.getCEfromSE(seSite)

        ## query if site is in production
        #status = cestate_from_ce_bdii(oneSite.split(':')[0].strip())
        #if status != 'Production' :
            #msg  = '[Condor-G Scheduler]: Jobs cannot be submitted to site ' + oneSite.split(':')[0].strip() + ' because the site has status ' + status + ' and is currently not operational.\n'
            #msg += '[Condor-G Scheduler]: Please choose another site for your jobs.'
            #common.logger.debug(2,msg)
            #raise CrabException(msg)

        #if self.batchsystem != '' :
            #oneSite = oneSite.split('/')[0].strip() + '/' + self.batchsystem

        #to_write += 'globusscheduler = "&quot;' + str(oneSite) + '&quot;"\n'

        ## extraTag condor transfer file flag
        #to_write += 'should_transfer_files = "&quot;YES&quot;"\n'

        ## extraTag when to write output
        #to_write += 'when_to_transfer_output = "&quot;ON_EXIT&quot;"\n'

        ## extraTag switch off streaming of stdout
        #to_write += 'stream_output = "&quot;false&quot;"\n'

        ## extraTag switch off streaming of stderr
        #to_write += 'stream_error = "&quot;false&quot;"\n'

        ## extraTag condor logfile
        #condor_log = jbt.name() + '__ITR3_.log'
        #to_write += 'Log    = "&quot;' + condor_log + '&quot;"\n'

        ## extraTag condor notification
        #to_write += 'notification="&quot;never&quot;"\n'

        ## extraTag condor queue statement
        #to_write += 'QUEUE = "&quot;1&quot;"\n'

        #if (to_write != ''):
            #xml.write('<extraTags\n')
            #xml.write(to_write)
            #xml.write('/>\n')
            #pass

        #xml.write('</chain>\n')

        #xml.write('</iterator>\n')
        #xml.write('</task>\n')

        #xml.close()

        #return

    #def checkProxy(self):
        #"""
        #Function to check the Globus proxy.
        #"""
        #if (self.proxyValid): return
        ##timeleft = -999
        #minTimeLeft=10*3600 # in seconds

        #mustRenew = 0
        #timeLeftLocal = runCommand('voms-proxy-info -timeleft 2>/dev/null')
        #timeLeftServer = -999
        #if not timeLeftLocal or int(timeLeftLocal) <= 0 or not isInt(timeLeftLocal):
            #mustRenew = 1
        #else:
            #timeLeftServer = runCommand('voms-proxy-info -actimeleft 2>/dev/null | head -1')
            #if not timeLeftServer or not isInt(timeLeftServer):
                #mustRenew = 1
            #elif timeLeftLocal<minTimeLeft or timeLeftServer<minTimeLeft:
                #mustRenew = 1
            #pass
        #pass

        #if mustRenew:
            #common.logger.message( "No valid proxy found or remaining time of validity of already existing proxy shorter than 10 hours!\n Creating a user proxy with default length of 192h\n")
            #cmd = 'voms-proxy-init -voms '+self.VO
            #if self.group:
                #cmd += ':/'+self.VO+'/'+self.group
            #if self.role:
                #cmd += '/role='+self.role
            #cmd += ' -valid 192:00'
            #try:
                ## SL as above: damn it!
                #out = os.system(cmd)
                #if (out>0): raise CrabException("Unable to create a valid proxy!\n")
            #except:
                #msg = "Unable to create a valid proxy!\n"
                #raise CrabException(msg)
            #pass

        #self.proxyValid=1
        #return
