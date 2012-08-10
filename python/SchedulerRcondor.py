"""
Implements the vanilla (local) Remote Condor scheduler
"""

from SchedulerGrid  import SchedulerGrid
from crab_exceptions import CrabException
from crab_util import runCommand
#from WMCore.SiteScreening.BlackWhiteListParser import CEBlackWhiteListParser
from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser



import common
import os
import socket

# FUTURE: for python 2.4 & 2.6
try:
    from hashlib import sha1
except:
    from sha import sha as sha1

class SchedulerRcondor(SchedulerGrid) :
    """
    Class to implement the vanilla (local) Condor scheduler
     Naming convention:  Methods starting with 'ws' provide
     the corresponding part of the job script
     ('ws' stands for 'write script').
    """

    def __init__(self):
        SchedulerGrid.__init__(self,"RCONDOR")
        self.rcondorHost   = os.getenv('RCONDOR_HOST')
        if self.rcondorHost == None:
            raise CrabException('FATAL ERROR: env.var RCONDOR_HOST not defined')
        self.datasetPath   = None
        self.selectNoInput = None
        self.OSBsize = 50*1000*1000 # 50 MB

        self.environment_unique_identifier = None
        return


    def configure(self, cfg_params):
        """
        Configure the scheduler with the config settings from the user
        """

        SchedulerGrid.configure(self, cfg_params)

        self.proxyValid=0
        self.dontCheckProxy=int(cfg_params.get("GRID.dont_check_proxy",0))
        self.space_token = cfg_params.get("USER.space_token",None)
        try:
            self.proxyServer = Downloader("http://cmsdoc.cern.ch/cms/LCG/crab/config/").config("myproxy_server.conf")
            self.proxyServer = self.proxyServer.strip()
            if self.proxyServer is None:
                raise CrabException("myproxy_server.conf retrieved but empty")
        except Exception, e:
            common.logger.info("Problem setting myproxy server endpoint: using myproxy.cern.ch")
            common.logger.debug(e)
            self.proxyServer= 'myproxy.cern.ch'
        self.group = cfg_params.get("GRID.group", None)
        self.role = cfg_params.get("GRID.role", None)
        self.VO = cfg_params.get('GRID.virtual_organization','cms')

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

        self.checkProxy()

        return
    
    def userName(self):
        """ return the user name """
        tmp=runCommand("voms-proxy-info -identity 2>/dev/null")
        return tmp.strip()

    def envUniqueID(self):
        taskHash = sha1(common._db.queryTask('name')).hexdigest()
        id = "https://" + socket.gethostname() + '/' + taskHash + "/${NJob}"
        return id

    def sched_parameter(self, i, task):
        """
        Return scheduler-specific parameters. Used at crab -submit time
        """

#SB paste from crab ScheduerGlidein

        jobParams = ""

        seDest = task.jobs[i-1]['dlsDestination']

        if seDest == [''] :
            seDest = self.blackWhiteListParser.expandList("T") # all of SiteDB

        seString=self.blackWhiteListParser.cleanForBlackWhiteList(seDest)

        jobParams += '+DESIRED_SEs = "'+seString+'"; '
        myschedName = self.rcondorHost
        jobParams += '+Glidein_MonitorID = "https://'+ myschedName + '//$(Cluster).$(Process)"; '

        if (self.EDG_clock_time):
            jobParams += '+MaxWallTimeMins = '+self.EDG_clock_time+'; '
        else:
            jobParams += '+MaxWallTimeMins = %d; ' % (60*24)

        common._db.updateTask_({'jobType':jobParams})


        return jobParams


    def realSchedParams(self, cfg_params):
        """
        Return dictionary with specific parameters, to use with real scheduler
        is called when scheduler is initialized in Boss, i.e. at each crab command
        """
        #SB this method is used to pass directory names to Boss Scheduler
        # via params dictionary

        jobDir = common.work_space.jobDir()
        taskDir=common.work_space.topDir().split('/')[-2]
        shareDir = common.work_space.shareDir()
        #SBtmpDir = common.work_space.tmpDir()
        
        params = {'rcondorHost':self.rcondorHost,
                  'shareDir':shareDir,
                  #SB'tmpDir':tmpDir,
                  'jobDir':jobDir,
                  'taskDir':taskDir}

        return params


    def listMatch(self, seList, full):
        """
        Check the compatibility of available resources
        """

        return [True]


    def decodeLogInfo(self, fileName):
        """
        Parse logging info file and return main info
        """

        import CondorGLoggingInfo
        loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
        reason = loggingInfo.decodeReason(fileName)
        return reason


#    def wsCopyOutput(self):
#        """
#        Write a CopyResults part of a job script, e.g.
#        to copy produced output into a storage element.
#        """
#        txt = self.wsCopyOutput()
#        return txt


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

        txt += '    tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    tmp_size=`ls -gGrta ${out_files}.tgz | awk \'{ print $3 }\'`\n'
        txt += '    rm ${out_files}.tgz\n'
        txt += '    size=`expr $tmp_size`\n'
        txt += '    echo "Total Output dimension: $size"\n'
        txt += '    limit='+str(self.OSBsize) +' \n'
        txt += '    echo "WARNING: output files size limit is set to: $limit"\n'
        txt += '    if [ "$limit" -lt "$size" ]; then\n'
        txt += '        exceed=1\n'
        txt += '        job_exit_code=70000\n'
        txt += '        echo "Output Sanbox too big. Produced output is lost "\n'
        txt += '    else\n'
        txt += '        exceed=0\n'
        txt += '        echo "Total Output dimension $size is fine."\n'
        txt += '    fi\n'

        txt += '    echo "JOB_EXIT_STATUS = $job_exit_code"\n'
        txt += '    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    if [ $exceed -ne 1 ]; then\n'
        txt += '        tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    else\n'
        txt += '        tar zcvf ${out_files}.tgz CMSSW_${NJob}.stdout CMSSW_${NJob}.stderr\n'
        txt += '    fi\n'
        txt += '    python $RUNTIME_AREA/fillCrabFjr.py $RUNTIME_AREA/crab_fjr_$NJob.xml --errorcode $job_exit_code \n'

        txt += '    exit $job_exit_code\n'
        txt += '}\n'

        return txt


    def sched_fix_parameter(self):
        """
        Returns string with requirements and scheduler-specific parameters
        """

        if self.EDG_requirements:
            req = self.EDG_requirements
            taskReq = {'commonRequirements':req}
            common._db.updateTask_(taskReq)


