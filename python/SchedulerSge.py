from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
import common

import os,string

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#
# Author: Hartmut Stadie <stadie@mail.desy.de> Inst. f. Experimentalphysik; Universitaet Hamburg
#

class SchedulerSge(SchedulerLocal) :

    def __init__(self):
        Scheduler.__init__(self,"SGE")
        self.datasetPath   = None
        self.selectNoInput = None
        return

    def configure(self, cfg_params):
        SchedulerLocal.configure(self, cfg_params)

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

        self.return_data = cfg_params.get('USER.return_data', 0)
        self.copy_data   = cfg_params.get("USER.copy_data", 0)

        if ( int(self.return_data) == 0 and int(self.copy_data) == 0 ):
            msg = 'Error: return_data and copy_data cannot be set both to 0\n'
            msg = msg + 'Please modify your crab.cfg file\n'
            raise CrabException(msg)

        if ( int(self.return_data) == 1 and int(self.copy_data) == 1 ):
            msg = 'Error: return_data and copy_data cannot be set both to 1\n'
            msg = msg + 'Please modify your crab.cfg file\n'
            raise CrabException(msg)

        if ( int(self.copy_data) == 0 and int(self.publish_data) == 1 ):
            msg = 'Warning: publish_data = 1 must be used with copy_data = 1\n'
            msg = msg + 'Please modify copy_data value in your crab.cfg file\n'
            common.logger.info(msg)
            raise CrabException(msg)

        if int(self.copy_data) == 1:
            self.SE = cfg_params.get('USER.storage_element', None)
            if not self.SE:
                msg = "Error. The [USER] section has no 'storage_element'"
                common.logger.info(msg)
                raise CrabException(msg)

            self.proxyValid = 0
            self.dontCheckProxy = int(cfg_params.get("GRID.dont_check_proxy",0))
            self.proxyServer = cfg_params.get("GRID.proxy_server",'myproxy.cern.ch')
            common.logger.debug('Setting myproxy server to ' + self.proxyServer)

            self.group = cfg_params.get("GRID.group", None)
            self.role  = cfg_params.get("GRID.role", None)
            self.VO    = cfg_params.get('GRID.virtual_organization', 'cms')

            self.checkProxy()

        self.role = None

        self.pool = cfg_params.get('USER.storage_pool',None)
#        self.cpu = cfg_params.get('USER.cpu',172800)
#        self.vmem = cfg_params.get('USER.vmem',2)
        return

    def envUniqueID(self):
        id = "https://"+common.scheduler.name()+":/${JOB_ID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")
        return id

    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use
        with real scheduler
        """
        params = {}
        return  params

    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        index = int(common._db.nJobs()) - 1
        sched_param= ''

        for i in range(index): # Add loop DS
            sched_param= ''
            if (self.queue):
                sched_param += '-q '+self.queue +' '
                if (self.res): sched_param += ' -l '+self.res +' '
            pass

        #default is request 2G memory and 48 hours CPU time
        #sched_param += ' -V -l h_vmem=2G -l h_cpu=172800 '
#        sched_param += ' -V -l h_vmem='
#        sched_param += self.vmem.__str__()
#        sched_param += 'G -l h_cpu='
#        sched_param += self.cpu.__str__()
#        sched_param += ' '

        return sched_param

    def loggingInfo(self, id):
        """ return logging info about job nj """
        print "Warning: SchedulerSge::loggingInfo not implemented!"
        return ""

    def wsExitFunc(self):
        """
        """
        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()

        txt += '    cp ${SGE_STDOUT_PATH} CMSSW_${NJob}.stdout \n'
        txt += '    cp ${SGE_STDERR_PATH} CMSSW_${NJob}.stderr \n'
        txt += '    tar zcvf ${out_files}.tgz  ${filesToCheck}\n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'

        return txt

    def listMatch(self, dest, full):
        """
        """
        #if len(dest)!=0:
        sites = [self.blackWhiteListParser.cleanForBlackWhiteList(dest,'list')]
        #else:
        #    sites = [str(getLocalDomain(self))]
        return sites

    def wsCopyOutput(self):
        txt=self.wsCopyOutput_comm(self.pool)
        return txt

    def userName(self):
        """ return the user name """

        ## hack for german naf
        import pwd,getpass
        tmp=pwd.getpwnam(getpass.getuser())[4]
        tmp=tmp.rstrip(',')
        tmp=tmp.rstrip(',')
        tmp=tmp.rstrip(',')


        return "/CN="+tmp.strip()
