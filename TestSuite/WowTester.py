# -*- coding: iso-8859-1 -*-
from Session import *
from Tester import Tester
import random

class WowTester(Tester):
    """ This class implements a complex behavioural test for CRAB stressing it in all its main action. """
    def __init__(self, configFile, name, timeout, semaphore, debug = False):
        """ WowTester constructor.

        WowTester constructor: configFile is the path to crab.cfg, name is a identification name for the test and timeout is the
        time after which the test is stopped.
        """
        Tester.__init__(self, configFile, name+"-wow", timeout, semaphore, debug)

    def testAction(self, name, jobList, debugName):
        """ Handy method to test a particular crab action.

        name is the actual name of the action among getoutput, kill and resubmit.
        jobList is the list of jobs number to pass to the action.
        debugName is a name to print on the debug screen.
        This method call Crab with the requested action, passing it a job list built from
        the parameter jobList, and check its result, updating the tests statistics.
        """
        self.logger.debug(debugName)
        jobList = set(jobList)
        actualJobList = self.randomList(jobList, self.session.totJobs) 
        try:
            if name == "getoutput":
                self.session.crabGetOutput(actualJobList, jobList & actualJobList)
            if name == "kill":
                self.session.crabKill(actualJobList, jobList & actualJobList)
            if name == "resubmit":
                self.session.crabResubmit(actualJobList, jobList & actualJobList)
        except TestException, txt:
            self.dumpError(txt)
            self.tests[name] = False
        if self.tests[name] == None:
            self.tests[name] = True
        
    
    def test(self):
        """ The Wow Test :-). """

        # Creation
        self.tests["create"] = False 
        toSubmit = self.session.crabCreate() # toSubmit is a set of job created
        self.tests["create"] = True

        # Main cicle
        while self.checkTimeout():
            # Status update
            self.session.crabStatus()
            
            badJobs = self.session.jobsHistory.getJobsInRemoteStatus(BAD)
            if badJobs and not self.toBeChecked:
                self.logger.warning("Some jobs in a bad status!")
                self.toBeChecked = True

            if self.session.jobsHistory.isChanged():
                self.session.logger.info(str(self.session.jobsHistory))
            # Submit with a certain probability and if it is possible
            if (len(toSubmit) > 0 and random.random() > .75) or len(toSubmit) == self.session.totJobs:
                self.logger.debug("submitting...")
                weSubmit = random.randint(1, len(toSubmit))
                self.tests["submit"] = False
                submitted = self.session.crabSubmit(weSubmit)
                self.tests["submit"] = True
                toSubmit.difference_update(submitted) # Subtracting the job submitted from those to submit
                clearedJobs = set() # hack
            else:
                # other actions
                self.logger.debug("playing...")
                runningJobs = self.session.jobsHistory.getJobsInRemoteStatus(RUNNING)
                doneJobs = self.session.jobsHistory.getJobsInRemoteStatus(DONE)
                clearedJobs = self.session.jobsHistory.getJobsInRemoteStatus(CLEARED)
                killedJobs = self.session.jobsHistory.getJobsInRemoteStatus(KILLED)
                abortedJobs = self.session.jobsHistory.getJobsInRemoteStatus(BAD)

                killable = runningJobs 
                retrievable = doneJobs
                resubmittable = abortedJobs | killedJobs

                self.logger.debug("killable->"+str(killable)+" retrievable->"+str(retrievable)+" resubmittable->"+str(resubmittable))

                # In what follows, we take an action with a certain probability and only if there's at least a job in a usesful state
                if retrievable and random.random() > 0.25:
                    self.testAction("getoutput", retrievable, "retrieving")
                elif resubmittable and random.random() > 0.50:
                    self.testAction("resubmit", resubmittable, "resubmitting")
                elif killable and random.random() > 0.90:
                    self.testAction("kill", killable, "killing")
            
            if len(clearedJobs) == self.session.totJobs:
                break;
