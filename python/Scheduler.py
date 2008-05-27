from crab_exceptions import *
from Boss import Boss
import common
import string, time, os
from crab_util import *

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class Scheduler :

    _instance = None

    def getInstance():
        if not Scheduler._instance :
            raise CrabException('Scheduler has no instance.')
        return Scheduler._instance
    
    getInstance = staticmethod(getInstance)

    def __init__(self, name):
        Scheduler._instance = self
        self._name = string.lower(name)
        self._boss = Boss()
        return

    def name(self):
        return self._name

    def realSchedParams(self,cfg_params):
        """
        """
        return {} 

    def configure(self, cfg_params):
        self._boss.configure(cfg_params)
        from BlackWhiteListParser import BlackWhiteListParser
        self.blackWhiteListParser = BlackWhiteListParser(cfg_params)
        return

    def boss(self):
        return self._boss

    def rb_configure(self, RB):
        """
        Return a requirement to be add to Jdl to select a specific RB/WMS:
        return None if RB=None
        To be re-implemented in concrete scheduler
        """
        return None

    def ce_list(self):
        return '',None,None

    def se_list(self, id, dest):
        return '',None,None

    def sched_fix_parameter(self):
        return     

    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        return ''

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return ''

    def  wsExitFunc(self):
        """
        Returns part of a job script which does scheduler-specific
        output checks and management.
        """
        return ''

    def checkProxy(self, deep=0):
        """ check proxy """
        return

    def userName(self):
        """ return the user name """
        return

    def loggingInfo(self,list_id,outfile ):
        """ return logging info about job nj """
        return self.boss().LoggingInfo(list_id,outfile)

    def tags(self):
        return ''

    def listMatch(self, dest):
        """ Return the number of differente sites matching the actual requirements """
        start = time.time()
        tags=self.tags()

        if len(dest)!=0: dest = self.blackWhiteListParser.cleanForBlackWhiteList(dest,'list') 

        whiteL=self.ce_list()[1]
        blackL=self.ce_list()[2]

        sites= self.boss().listMatch(tags, dest , whiteL, blackL) 
        stop = time.time()

        return sites 
    
    def submit(self,list,task):
        """ submit to scheduler a list of jobs """
        if (not len(list)): common.logger.message("No sites where to submit jobs")
        req=str(self.sched_parameter(list[0],task))

        ### reduce collection size...if needed 
        new_list = bulkControl(self,list)
  
        for sub_list in new_list: 
            self.boss().submit(task['id'],sub_list,req)
        return 

    def queryEverything(self,taskid):
        """
        Query needed info of all jobs with specified boss taskid
        """
        return self.boss().queryEverything(taskid)

    def getOutput(self, taskId, jobRange, outdir):
        """
        Get output for a finished job with id.
        """
        self.boss().getOutput(taskId, jobRange, outdir)
        return

    def cancel(self,ids):
        """
        Cancel the job(s) with ids (a list of id's)
        """
        self._boss.cancel(ids)
        return

    def decodeLogInfo(self, file):
        """
        Parse logging info file and return main info
        """
        return

    def writeJDL(self, list, task):
        """ 
        Materialize JDL for a list of jobs
        """
        req=str(self.sched_parameter(list[0],task))
        new_list = bulkControl(self,list)
        jdl=[]
        for sub_list in new_list: 
            tmp_jdl =  self.boss().writeJDL(task['id'], sub_list, req)
            jdl.append(tmp_jdl)
        return jdl

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return ''

    def wsCopyInput(self):
        """
        Copy input data from SE to WN
        """
        return ""

    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        return ""

    def declare(self,jobs):
        """
        Declaration of jobs
        """
        self._boss.declare(jobs)

