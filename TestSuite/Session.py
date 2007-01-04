# -*- coding: iso-8859-1 -*-

class CrabException(Exception):
    pass;

class TestException(Exception):
    pass;


from Parser import *
import logging, datetime, os
from time import sleep

from Runner import runner
from History import *


CREATED = ['created']
WAITING = ['waiting', 'scheduled','ready','hold','checkpointed','idle']
SUBMITTED = ['submitted (boss)', 'submitted']
RUNNING = ['running']
DONE = ['done', 'done (success)']
CLEARED = ['cleared']
BAD = ['cleared (corrupt)', 'done (aborted)', 'undefined', 'aborted']
KILLED = ['killed', 'cancelled']


status = (CREATED, WAITING, SUBMITTED, RUNNING, DONE, CLEARED, BAD, KILLED)

class Session:
    """ Manager of a Crab session. """
    def __init__(self, logger, configFile, root):
        """ Construt a session with the current thread logger and the configFile name. """
        self.cwd = None # Working directory (discovered after crab -create)
        self.root = os.path.abspath(root)
        self.jobsHistory = History() 
        self.configFile = os.path.abspath(configFile)
        self.outNames = parseOutNames(open(self.configFile).read()) # Output file names found in the UI/res after getouput
        self.totJobs = None # Jobs managed (discovered after crab -crete)
        self.submitted = 0 # jobs submitted
        self.logger = logger

    def jobIds2str(self, jobIds):
        """ Translates a list of jobs into a string. "all" means a complete list of jobs built using self.totJobs.

        >>> jobIds2str([3,1,2])
        '3,1,2'
        """
        if jobIds == "all":
            return str(self.totJobs)
        elif jobIds:
            jobIds = list(jobIds)
            jobs = str(jobIds[0])
            for jobId in jobIds[1:]:
                jobs += ","+str(jobId)
            return jobs
        else:
            return ""

    def crabRunner(self, cmd):
        """ Executes crab with the list of parameters cmd.

        Executes crab with the list of parameters cmd. It returns a tuple containing the returncode and the stdout and the stderr
        in two string.
        """
        cmd = ["crab"] + cmd
        self.logger.debug("Testing: "+" ".join(cmd))
        returncode, outdata, errdata = runner(cmd)
        if returncode:
            raise CrabException, (cmd, returncode, outdata, errdata)
        return returncode, outdata, errdata


    def crabCreate(self):
        """ Run and test crab -create, returning the set of jobsIds created. """

        cmd = ['-create', '-cfg', self.configFile, '-USER.ui_working_dir', self.root]
        returncode, outdata, errdata = self.crabRunner(cmd)

        self.totJobs = parseCreate(outdata)

        if int(self.totJobs < 1):
            raise TestException, "No jobs created!"
        
        self.logger.info("creating->"+self.jobIds2str(range(1,self.totJobs+1)))

        self.jobsHistory.setJobsNumber(self.totJobs)
        self.cwd = findCrabWD(outdata)
        if not str(self.cwd):
            raise TestException, "Can't get the WD!"
        
        for i in range(1, self.totJobs+1):
            self.jobsHistory.setLocalJobStatus(i, 'created')
        
        self.crabStatus()
        
        for i in range(1, self.totJobs+1):
            local, remote = self.jobsHistory.getJobStatus(i)
            if not remote in CREATED:
                raise TestException, "Job "+str(i)+" not correctly created!"
        
        return set(range(1, self.totJobs+1))
            
    def crabStatus(self):
        """ Run and test crab -status, updating the jobsHistory. """

        assert(self.cwd)

        cmd = ['-status', '-c', self.cwd]
        returncode, outdata, errdata = self.crabRunner(cmd)

        jobList = parseEntireStatus(outdata)
        self.logger.debug("parseEntireStatus->"+str(jobList))
        
        for jobId, status, host, exitcode, statuscode in jobList:
            self.jobsHistory.setRemoteJobStatus(jobId, status)
            self.jobsHistory.setJobAttrs(jobId, host, exitcode, statuscode)

        self.logger.debug(str(self.jobsHistory))

    def crabSubmit(self, nSubmit=-1):
        """ Run and test crab -submit, returning the set of jobsIds submitted. If nSubmit < 0 submit all the jobs it can submit. """

        assert(self.cwd)

        if int(nSubmit) < 0:
            cmd = ['-submit', '-c', self.cwd]
        else:
            cmd = ['-submit', str(nSubmit), '-c', self.cwd]

        returncode, outdata, errdata = self.crabRunner(cmd)

        submitted = parseSubmit(outdata)

        for i in range(self.submitted+1, self.submitted+submitted+1):
            self.jobsHistory.setLocalJobStatus(i, 'submitted')
        
        self.logger.info("submitting->"+self.jobIds2str(range(self.submitted+1, self.submitted+submitted+1)))
        
        self.crabStatus()

        if self.submitted+submitted > self.totJobs:
            raise TestException, "More jobs submitted than created!!!"

        for i in range(self.submitted+1, self.submitted+submitted+1):
            local, remote = self.jobsHistory.getJobStatus(i)
            if not (remote in SUBMITTED or remote in WAITING or remote in RUNNING or remote in DONE):
                raise TestException, "Job "+str(i+1)+" not submitted correctly!"

        self.submitted += submitted
        
        return set(range(self.submitted-submitted+1,self.submitted+1))
        
    def crabGetOutput(self, jobIds = "all", expectedIds = None):
        """ Run and test crab -getouput, returning the set of jobsIds retrieved.
         
        Run and test crab -getouput, returning the set of jobsIds retrieved. if expectedIds is a set of jobIds, the jobs
        retrieved is compared to the expectedIds set, in order to check if some jobs aren't correctly retrieved.
        """
        assert(self.cwd)

        if jobIds == "all":
            cmd = ['-getoutput', '-c', self.cwd]
            self.logger.info("crab -getouput")
        else:
            cmd = ['-getoutput', self.jobIds2str(jobIds), '-c', self.cwd]

        if jobIds == "all":
            jobIds = set(range(1, self.totJobs+1))

        self.logger.info("retrieving->"+self.jobIds2str(jobIds))

        returncode, outdata, errdata = self.crabRunner(cmd)
        


        if not expectedIds:
            expectedIds = jobIds

        try:
            getoutput = parseGetOutput(outdata)
        except TestException, txt:
            if expectedIds:
                raise TestException, txt

        if expectedIds > getoutput: # If something was retrievable but wasn't retrieved
            raise TestException, "Crab didn't succeed in getting every output requested!"

        for i in getoutput:
            self.jobsHistory.setLocalJobStatus(i, 'retrieved')

        self.crabStatus()
        
        for i in getoutput:
            local, remote = self.jobsHistory.getJobStatus(i)
            host, exitcode, exitstatus = self.jobsHistory.getJobAttrs(i)

            if not remote in CLEARED:
                raise TestException, "Status of job "+str(i)+" after getoutput isn't correctly cleared!"

            # Checking the existance of output files in the UI
            try:
                for name in self.outNames:
                    self.logger.debug("Verifing "+name+" was retrieved...")
                    name = name.rsplit(".",1)
                    assert(len(name) == 2)
                    name = os.path.join(self.cwd, "res", name[0]+"_"+str(i)+"."+name[1])
                    try:
                        open(name, "rb").close()
                    except IOError:
                        raise TestException, "Output "+name+ " for job "+str(i)+" doesn't exist!"
            except TestException, txt:
                if not exitcode or not exitstatus: # The job hasn't correctly ended
                    self.logger.error(txt) # no need to say crab failed
                else:
                    raise TestException, txt # well, maybe here crab failed!
    
            if not exitcode or not exitstatus:
                self.logger.warning("Job "+str(i)+" not ended cleanly: EXITCODE="+str(exitcode)+" EXITSTATUS="+str(exitstatus))

    
        return getoutput

    def crabKill(self, jobIds = "all", expectedIds = None):
        """ Run and test crab -kill, returning the set of jobsIds killed.
         
         Run and test crab -getouput, returning the set of jobsIds killed. If expectedIds is a set of jobIds, the jobs
        killed are compared to the expectedIds set, in order to check if some jobs aren't correctly killed.
        """
        assert(self.cwd)

        cmd = ['-kill', self.jobIds2str(jobIds), '-c', self.cwd]
        
        if jobIds == "all":
            jobIds = set(range(1, self.totJobs+1))

        self.logger.info("killing->"+self.jobIds2str(jobIds))
        
        returncode, outdata, errdata = self.crabRunner(cmd)


        if not expectedIds:
            expectedIds = jobIds

        for i in expectedIds:
            self.jobsHistory.setLocalJobStatus(i, 'killed')
        time.sleep(10) # Wait for jobs to die!
        self.crabStatus()
        for i in expectedIds:
            local, remote = self.jobsHistory.getJobStatus(i)
            if not remote in KILLED:
                raise TestException, "Status of job "+str(i)+" after killing isn't aborted or killed!"

        return set(expectedIds)

    def crabResubmit(self, jobIds = "all", expectedIds=None):
        """ Run and test crab -resubmit, returning the set of jobsIds resubmitted.

        Run and test crab -resubmit, returning the set of jobsIds resubmitted. If expectedIds is a set of jobIds, the jobs
        resubmitted are compared to the expectedIds set, in order to check if some jobs aren't correctly resubmitted.
        """
        assert(self.cwd)
        
        cmd = ['-resubmit', self.jobIds2str(jobIds), '-c', self.cwd]
        
        if jobIds == "all":
            jobIds = set(range(1, self.totJobs+1))

        self.logger.info("resubmitting->"+self.jobIds2str(jobIds))

        returncode, outdata, errdata = self.crabRunner(cmd)

        if not expectedIds:
            expectedIds = jobIds

        try:
            resubmitted = parseSubmit(outdata)
        except TestException, txt:
            if expectedIds:
                raise TestException
        
        if not (set(expectedIds) == set(submitted)):
            raise TestException, "crab didn't succeed in resubmitting every output"

        for i in expectedIds:
            self.jobsHistory.setLocalJobStatus(i, 'resubmitted')
        
        self.crabStatus()

        for i in expectedIds:
            local, remote = self.jobsHistory.getJobStatus(i)
            if not (remote in WAITING or remote in RUNNING or remote in DONE):
                raise TestException, "Job "+str(i)+" not resubmitted correctly!"
        return set(expectedIds)

    def crabPostMortem(self):
        cmd = ['-postMortem', '-c', str(self.cwd)]
        self.crabRunner(cmd)
        #cmd = ['-status', '-debug', '10', '-c', str(self.cwd)]
        #returncode, outdata, errdata = self.crabRunner(cmd)
        #return returncode, outdata, errdata
        