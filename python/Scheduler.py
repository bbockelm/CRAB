from crab_exceptions import *
from Boss import Boss
import common
import string, time, os

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

    def configure(self, cfg_params):
        self._boss.configure(cfg_params)
        return

    def boss(self):
        return self._boss

    def sched_fix_parameter(self):
        return     

    def sched_parameter(self):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        return ''

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return ''

    def checkProxy(self):
        """ check proxy """
        return

    def userName(self):
        """ return the user name """
        return

    def loggingInfo(self, nj):
        """ return logging info about job nj """
        return

    def listMatch(self,tags, dest , whiteL, blackL): ##  whiteL, blackL added by MATTY as patch
        """ Return the number of differente sites matching the actual requirements """
        start = time.time()
        nsites= self.boss().listMatch(tags, dest , whiteL, blackL) 
        stop = time.time()

        return nsites 
    
    def submit(self,list,req):
        """ submit to scheduler a list of jobs """
        if (not len(list)): common.logger.message("No sites where to submit jobs")
        self.boss().submit(list,req) 
        return

    def queryEverything(self,taskid):

        """
        Query needed info of all jobs with specified boss taskid
        """
        self.checkProxy()
        return self.boss().queryEverything(taskid)

    def getOutput(self, int_id):
        """
        Get output for a finished job with id.
        """
        self.checkProxy()
        self.boss().getOutput(int_id)
        return

    def cancel(self,int_id):
        """
        Cancel the job job with id: if id == -1, means all jobs.
        """
        subm_id = []

        nTot = common.jobDB.nJobs()
        for id in int_id:
            if nTot >= id: ## TODO check the number of jobs..else: 'IndexError: list index out of range'
                if ( common.jobDB.status(id-1) in ['S','R','A']) and (id not in subm_id):
                    subm_id.append(id)
                else:
                    common.logger.message("Not possible to kill Job #"+str(id))
            else:
                common.logger.message("Warning: job # "+str(id)+" doesn't exists! Not possible to kill it.")
        self._boss.cancel(subm_id)
        return

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

    def tOut(self, list):
        return 120

    def moveOutput(self, nj):
        self.boss().moveOutput(nj)

