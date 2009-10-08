from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
import common

import os,string

# PBS/torque interface for CRAB (dave.newbold@cern.ch, June 09)
#
#
# In the [CRAB] section of crab.cfg, use:
#
# scheduler = pbs
#
# In the [PBS] section of crab.cfg, use the following optional parameters
#
# queue= pbs_queue_to_use [default, use the default queue in your local PBS config]
# resources = resource_1=value,resource_2=value, etc [like qsub -l syntax]
#
# NB: - the scheduler uses a wrapper script to create a local dir (see BossLite scheduler module)
# Both wrapper stdout/stderr and job script output files are placed in your crab_*/res directory by default
#

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerPbs(SchedulerLocal) :

    def __init__(self):
        Scheduler.__init__(self,"PBS")

    def configure(self, cfg_params):
        SchedulerLocal.configure(self, cfg_params)
        if "PBS.queue" in cfg_params:
            if len(cfg_params["PBS.queue"]) == 0 or cfg_params["PBS.queue"] == "default":
                common.logger.info(" The default queue of local PBS configuration will be used")
        else:
            common.logger.info(" The default queue of local PBS configuration will be used")

        #self.return_data = cfg_params.get('USER.return_data', 0)
        #self.copy_data   = cfg_params.get("USER.copy_data", 0)

        #if ( int(self.return_data) == 0 and int(self.copy_data) == 0 ):
        #    msg = 'Error: return_data and copy_data cannot be set both to 0\n'
        #    msg = msg + 'Please modify your crab.cfg file\n'
        #    raise CrabException(msg)

        #if ( int(self.return_data) == 1 and int(self.copy_data) == 1 ):
        #    msg = 'Error: return_data and copy_data cannot be set both to 1\n'
        #    msg = msg + 'Please modify your crab.cfg file\n'
        #    raise CrabException(msg)

        #if ( int(self.copy_data) == 0 and int(self.publish_data) == 1 ):
        #    msg = 'Warning: publish_data = 1 must be used with copy_data = 1\n'
        #    msg = msg + 'Please modify copy_data value in your crab.cfg file\n'
        #    common.logger.info(msg)
        #    raise CrabException(msg)

        #if int(self.copy_data) == 1:
        #    self.SE = cfg_params.get('USER.storage_element', None)
        #    if not self.SE:
        #        msg = "Error. The [USER] section has no 'storage_element'"
        #        common.logger.info(msg)
        #        raise CrabException(msg)

        #    self.proxyValid = 0
        #    self.dontCheckProxy = int(cfg_params.get("GRID.dont_check_proxy",0))
        #    self.proxyServer = cfg_params.get("GRID.proxy_server",'myproxy.cern.ch')
        #    common.logger.debug('Setting myproxy server to ' + self.proxyServer)

        #    self.group = cfg_params.get("GRID.group", None)
        #    self.role  = cfg_params.get("GRID.role", None)
        #    self.VO    = cfg_params.get('GRID.virtual_organization', 'cms')

        #    self.checkProxy()

        return


    def envUniqueID(self):
        id = "https://"+common.scheduler.name()+":/${PBS_JOBID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")
        return id

    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use
        with real scheduler
        """

        params={'jobScriptDir':common.work_space.jobDir(),
                'jobResDir':common.work_space.resDir()
               } 
#                'use_proxy': 0}

        for s in ('resources', 'queue'):
            params.update({s:cfg_params.get(self.name().upper()+'.'+s,'')})

        #if 'PBS.use_proxy' in cfg_params:
        #    if cfg_params['PBS.use_proxy'] == "1" or cfg_params['PBS.use_proxy'] == "0":
        #        params['use_proxy'] = int(cfg_params['PBS.use_proxy'])
        #        if params['use_proxy'] == 1:
        #            import os
        #            params['user_proxy'] = os.path.join(params['jobScriptDir'],'pbs_proxy')

        return params

    def listMatch(self, dest, full):
        return [str(getLocalDomain(self))]

    def wsCopyOutput(self):
        return self.wsCopyOutput_comm()

    def wsExitFunc(self):
        """
        """
        s=[]
        s.append('func_exit(){')
        s.append(self.wsExitFunc_common())
        s.append('tar zcvf '+common.work_space.resDir()+'${out_files}.tgz ${filesToCheck}')
        s.append('exit $job_exit_code')
        s.append('}')
        return '\n'.join(s)

    def envUniqueID(self):
        id = "https://"+common.scheduler.name()+":/${PBS_JOBID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")
        return id

