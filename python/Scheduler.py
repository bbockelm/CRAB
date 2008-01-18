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
        #print 'Created Scheduler ',self.name(),' with BOSS',self.boss()
        return

    def clean(self):
        """ destroy instance """
        del self._boss
        return

    def name(self):
        return self._name

    def configure(self, cfg_params):
        self._boss.configure(cfg_params)
        self._boss.checkSchedRegistration_(self.name())
        #print 'Configured Scheduler ',self.name(),' with BOSS',self.boss()
        return

    def boss(self):
        return self._boss

    def sched_parameter(self):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        index = int(common.jobDB.nJobs()) - 1
        job = common.job_list[index]
        jbt = job.type()

        lastBlock=-1
        first = []
        for n in range(common.jobDB.nJobs()):
            currBlock=common.jobDB.block(n)
            if (currBlock!=lastBlock):
                lastBlock = currBlock
                first.append(n)

        req = ''
        req = req + jbt.getRequirements()

        for i in range(len(first)): # Add loop DS
            groupReq = req
            self.param='sched_param_'+str(i)+'.clad'
            param_file = open(common.work_space.shareDir()+'/'+self.param, 'w')

            param_file.write('foo = bar;\n') ## Boss complain for empty clad
            if (self.queue):
                param_file.write('queue = '+self.queue +';\n')
                if (self.res): param_file.write('requirement = '+self.res +';\n')
            pass

            param_file.close()
        pass
        return 

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

    def listMatch(self, nj, Block, whiteL, blackL): ##  whiteL, blackL added by MATTY as patch
        """ Return the number of differente sites matching the actual requirements """
        start = time.time()
        schcladstring = ''
        self.schclassad = common.work_space.shareDir()+'/'+'sched_param_'+str(Block)+'.clad'
        if os.path.isfile(self.schclassad):  
            schcladstring=self.schclassad

        CEs=self.boss().listMatch( self.name(), schcladstring)
        stop = time.time()
        common.logger.debug(1,"listMatch time :"+str(stop-start))
        common.logger.write("listMatch time :"+str(stop-start))

        ### SL: I'm positive that we should use BlackWhiteListParser here
        #### MATTY's patch 4 CE white-black lists ####
        sites = []
        for it in CEs :
            it = it.split(':')[0]
            if not sites.count(it) :
                sites.append(it)
        ### white-black list on CE ###
        CE_whited = []
        if len(whiteL) > 0:
            common.logger.message("Using ce white list functionality...")
            common.logger.debug(1,str(whiteL))
            for ce2check in sites:
                for ceW in whiteL:
                    if ce2check.find(ceW.strip()) != -1:
                        CE_whited.append(ce2check)
                        common.logger.debug(5,"CEWhiteList: adding from matched site: " + str(ce2check))
            sites = CE_whited

        CE_blacked = []
        if len(blackL) > 0:
            for ce2check in sites:
                for ceB in blackL:
                    if ce2check.find(ceB.strip()) != -1:
                        CE_blacked.append(ce2check)

        toRemove = []
        if len(CE_blacked) > 0:
            common.logger.message("Using ce black list functionality...")
            common.logger.debug(1,str(blackL))
            for ce2check in sites:
                for ceB in CE_blacked:
                    if ce2check.find(ceB.strip()) != -1:
                        toRemove.append(ce2check)

            for rem in toRemove:
                if rem in sites:
                    sites.remove(rem)
                    common.logger.debug(5,"CEBlackList: removing from matched site " + str(rem))
        ##############################

        if (len(sites)!=0): ## it was CEs
            common.logger.debug(5,"All Sites :"+str(CEs))
            common.logger.message("Matched Sites :"+str(sites))
        else: self.listMatchFailure(sites)

        return len(sites)

    def listMatchFailure(self, sites):
        """ Do whatever appropriate to notify the user about possible reason why no sites were matched """
        return
    
    def submit(self,list):
        """ submit to scheduler a list of jobs """
        if (not len(list)): common.logger.message("No sites where to submit jobs")
        Tout = int(self.tOut(list))
        jobsList = list[1]
        schcladstring = ''
        self.schclassad = common.work_space.shareDir()+'/'+'sched_param_'+str(list[0])+'.clad'# TODO add a check is file exist
        if os.path.isfile(self.schclassad):  
            schcladstring=self.schclassad
        jid, bjid = self.boss().submit(jobsList, schcladstring, Tout)
        
        return jid, bjid

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """
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

    def createXMLSchScript(self, nj, argsList):

        """
        Create a XML-file for BOSS4.
        """
        return

    def declare(self):
        """
        BOSS declaration of jobs
        """
        self._boss.declareJob_()

    def taskDeclared(self, taskName ):
        taskDict = self.boss().taskDeclared( taskName )
        if len(taskDict) > 0:
            return True
        return False

    def tOut(self, list):
        return 120

    def list(self):
        return self.boss().list()
