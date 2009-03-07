from Scheduler import Scheduler
from SchedulerLocal import SchedulerLocal
from crab_exceptions import *
from crab_util import *
from crab_logger import Logger
import common
from LFNBaseName import *

import os,string

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerLsf(SchedulerLocal) :

    def __init__(self):
        Scheduler.__init__(self,"LSF")
        
        return

    def configure(self, cfg_params):
        SchedulerLocal.configure(self, cfg_params)
        self.outputDir = cfg_params.get('USER.outputdir' ,common.work_space.resDir())

        self.pool = cfg_params.get('USER.storage_pool',None)
        return
    
    def Env_uniqueId(self):
        id = "https://"+common.scheduler.name()+":/${LSB_BATCH_JID}-"+ \
            string.replace(common._db.queryTask('name'),"_","-")
        return id

    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use 
        with real scheduler  
        """
        ### use by the BossLite script 
        self.cpCmd  =  cfg_params.get(self.name().upper()+'.cp_command','cp')
        self.rfioName =  cfg_params.get(self.name().upper()+'.rfio_server','')

        params = { 'cpCmd'  : self.cpCmd, \
                   'rfioName' : self.rfioName
                 }
        return  params

    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        sched_param= ''

        if (self.queue):
            sched_param += '-q '+self.queue +' '
            if (self.res): sched_param += ' -R '+self.res +' '

        # sched_param+='-cwd '+ str(self.outputDir)  + ' '
        return sched_param
   
    def listMatch(self, dest, full):
        """
        """ 
        if len(dest)!=0: 
            sites = [self.blackWhiteListParser.cleanForBlackWhiteList(dest,'list')]
        else:     
            sites = [str(getLocalDomain(self))]  
        return sites    

    def loggingInfo(self, id, fname):
        """ return logging info about job nj """
        lsfid = common._db.queryRunJob('schedulerId',id)
        cmd = 'bjobs -l ' + str(lsfid[0])
        cmd_out = runCommand(cmd)
        f = open(fname,'w')
        f.write(cmd_out)
        f.close()
        return fname

    def wsCopyOutput(self):
        txt=self.wsCopyOutput_comm(self.pool)
        return txt

    def wsExitFunc(self):
        """
        """
        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()

        txt += '    cp *.${LSB_BATCH_JID}.out CMSSW_${NJob}.stdout \n'
        txt += '    cp *.${LSB_BATCH_JID}.err CMSSW_${NJob}.stderr \n'
        txt += '    tar zcvf ${out_files}.tgz  ${filesToCheck}\n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'

        return txt
