"""
Implements the vanilla (local) Remote Condor scheduler
"""

from SchedulerGrid  import SchedulerGrid
from crab_exceptions import CrabException
from crab_util import runCommand


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
        self.datasetPath   = None
        self.selectNoInput = None
        self.OSBsize = 100*1000*1000 # 100 MB

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
        Return scheduler-specific parameters
        """
        req = ''
        if self.EDG_addJdlParam:
            if self.EDG_addJdlParam[-1] == '':
                self.EDG_addJdlParam = self.EDG_addJdlParam[:-1]
            for p in self.EDG_addJdlParam:
                req += p.strip()+';\n'

        return req


    def realSchedParams(self, cfg_params):
        """
        Return dictionary with specific parameters, to use with real scheduler
        """

        tmpDir = os.path.join(common.work_space.shareDir(),'.condor_temp')
        tmpDir = os.path.join(common.work_space.shareDir(),'.condor_temp')
        jobDir = common.work_space.jobDir()

        taskDir=common.work_space.topDir().split('/')[-2]
        rcondorDir = "/afs/cern.ch/user/b/belforte/w0/crabtest/rc/igor/"
        rcondorDir ='%s/.rcondor/mount/' % os.getenv('HOME')
        tmpDir = os.path.join(rcondorDir,taskDir)
        tmpDir = os.path.join(tmpDir,'condor_temp')
        
        params = {'tmpDir':tmpDir,
                  'jobDir':jobDir}
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

        txt += '    cp  ${out_files}.tgz $_CONDOR_SCRATCH_DIR/\n'
        txt += '    cp  crab_fjr_$NJob.xml $_CONDOR_SCRATCH_DIR/\n'

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


# presa di brutto da SchedulerGrid.py
"""

    def wsSetupEnvironment(self):

        #Returns part of a job script which does scheduler-specific work.

        taskId =common._db.queryTask('name')
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        if not self.environment_unique_identifier:
            try :
                self.environment_unique_identifier = self.envUniqueID()
            except :
                raise CrabException('environment_unique_identifier not set')

        # start with wrapper timing
        txt  = 'export TIME_WRAP_INI=`date +%s` \n'
        txt += 'export TIME_STAGEOUT=-2 \n\n'
        txt += '# '+self.name()+' specific stuff\n'
        txt += '# strip arguments\n'
        txt += 'echo "strip arguments"\n'
        txt += 'args=("$@")\n'
        txt += 'nargs=$#\n'
        txt += 'shift $nargs\n'
        txt += "# job number (first parameter for job wrapper)\n"
        txt += "NJob=${args[0]}; export NJob\n"
        txt += "NResub=${args[1]}; export NResub\n"
        txt += "NRand=`getRandSeed`; export NRand\n"
        # append random code
        txt += 'OutUniqueID=_$NRand\n'
        txt += 'OutUniqueID=_$NResub$OutUniqueID\n'
        txt += 'OutUniqueID=$NJob$OutUniqueID; export OutUniqueID\n'
        txt += 'CRAB_UNIQUE_JOB_ID=%s_${OutUniqueID}; export CRAB_UNIQUE_JOB_ID\n' % taskId
        txt += 'echo env var CRAB_UNIQUE_JOB_ID set to: ${CRAB_UNIQUE_JOB_ID}\n'
        # if we want to prepend
        #txt += 'OutUniqueID=_$NResub\n'
        #txt += 'OutUniqueID=_$NJob$OutUniqueID\n'
        #txt += 'OutUniqueID=$NRand$OutUniqueID; export OutUniqueID\n'

        txt += "out_files=out_files_${NJob}; export out_files\n"
        txt += "echo $out_files\n"
        txt += jbt.outList()
      #  txt += 'if [ $JobRunCount ] && [ `expr $JobRunCount - 1` -gt 0 ] && [ $Glidein_MonitorID ]; then \n'
        txt += 'if [ $Glidein_MonitorID ]; then \n'
#        txt += '   attempt=`expr $JobRunCount - 1` \n'
#        txt += '   MonitorJobID=${NJob}_${Glidein_MonitorID}__${attempt}\n'
#        txt += '   SyncGridJobId=${Glidein_MonitorID}__${attempt}\n'
        txt += '   MonitorJobID=${NJob}_${Glidein_MonitorID}\n'
        txt += '   SyncGridJobId=${Glidein_MonitorID}\n'
        txt += 'else \n'
        txt += '   MonitorJobID=${NJob}_'+self.environment_unique_identifier+'\n'
        txt += '   SyncGridJobId='+self.environment_unique_identifier+'\n'
        txt += 'fi\n'
        txt += 'MonitorID='+taskId+'\n'
        txt += 'echo "MonitorJobID=$MonitorJobID" > $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=$SyncGridJobId" >> $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=$MonitorID" >> $RUNTIME_AREA/$repo\n'

        txt += 'echo ">>> GridFlavour discovery: " \n'
        txt += 'if [ $OSG_GRID ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        SyncCE="$OSG_JOB_CONTACT"; \n'
        txt += '        echo "SyncCE=$SyncCE" >> $RUNTIME_AREA/$repo ;\n'
        txt += '    else\n'
        txt += '        echo "not reporting SyncCE";\n'
        txt += '    fi\n';
        txt += '    echo "GridFlavour=$middleware" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "source OSG GRID setup script" \n'
        txt += '    source $OSG_GRID/setup.sh \n'
        txt += 'elif [ $NORDUGRID_CE ]; then \n' # We look for $NORDUGRID_CE before $VO_CMS_SW_DIR,
        txt += '    middleware=ARC \n'           # because the latter is defined for ARC too
        txt += '    echo "SyncCE=${NORDUGRID_CE}:2811/nordugrid-GE-${QUEUE:-queue}" >> $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=$middleware" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'elif [ $VO_CMS_SW_DIR ]; then \n'
        txt += '    middleware=LCG \n'
        txt += '    if  [ $GLIDEIN_Gatekeeper ]; then \n'
        txt += '        echo "SyncCE=`echo $GLIDEIN_Gatekeeper | sed -e s/:2119//`" >> $RUNTIME_AREA/$repo \n'
        txt += '    else \n'
        txt += '        echo "SyncCE=`glite-brokerinfo getCE`" >> $RUNTIME_AREA/$repo \n'
        txt += '    fi \n'
        txt += '    echo "GridFlavour=$middleware" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'else \n'
        txt += '    echo "ERROR ==> GridFlavour not identified" \n'
        txt += '    job_exit_code=10030 \n'
        txt += '    func_exit \n'
        txt += 'fi \n'

        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '\n\n'


        txt += 'export VO='+self.VO+'\n'
        txt += 'if [ $middleware == LCG ]; then\n'
        txt += '   if  [ $GLIDEIN_Gatekeeper ]; then\n'
        txt += '       CloseCEs=$GLIDEIN_Gatekeeper \n'
        txt += '   else\n'
        txt += '       CloseCEs=`glite-brokerinfo getCE`\n'
        txt += '   fi\n'
        txt += '   echo "CloseCEs = $CloseCEs"\n'
        txt += '   CE=`echo $CloseCEs | sed -e "s/:.*//"`\n'
        txt += '   echo "CE = $CE"\n'
        txt += 'elif [ $middleware == OSG ]; then \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        CE=`echo $OSG_JOB_CONTACT | /usr/bin/awk -F\/ \'{print $1}\'` \n'
        txt += '    else \n'
        txt += '        echo "ERROR ==> OSG mode in setting CE name from OSG_JOB_CONTACT" \n'
        txt += '        job_exit_code=10099\n'
        txt += '        func_exit\n'
        txt += '    fi \n'
        txt += 'elif [ $middleware == ARC ]; then \n'
        txt += '    echo "CE = $NORDUGRID_CE"\n'
        txt += 'fi \n'

        return txt
"""
