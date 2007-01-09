# -*- coding: iso-8859-1 -*-
from Session import *
from Tester import Tester
import random

class WowTester(Tester):
    def __init__(self, configFile, name, timeout, debug = False):
        Tester.__init__(self, configFile, name+"-wow", timeout, debug)

    def testAction(self, name, jobList, debugName):
        self.logger.debug(debugName)
        actualJobList = self.randomList(jobList, self.session.totJobs)
        try:
            if name == "getoutput":
                self.session.crabGetOutput(actualJobList, jobList & actualJobList)
        except TestException, txt:
            self.dumpError(txt)
            self.tests[name] = False
        if self.tests[name] == None:
            self.tests[name] = True
        
    
    def test(self):
        self.tests["create"] = False
        toSubmit = self.session.crabCreate()
        self.tests["create"] = True

        while self.checkTimeout():
            self.session.crabStatus()
            if self.session.jobsHistory.isChanged():
                self.session.logger.info(str(self.session.jobsHistory))
            clearedJobs = set()
            if len(toSubmit) > 0 and random.random() > .75:
                self.logger.debug("submitting...")
                weSubmit = random.randint(1, len(toSubmit))
                self.tests["submit"] = False
                submitted = self.session.crabSubmit(weSubmit)
                self.tests["submit"] = True
                toSubmit.difference_update(submitted) # rimuovo dai job da sottomettere quelli sottomessi
            else:
                self.logger.debug("playing...")
                runningJobs = self.session.jobsHistory.getJobsInRemoteStatus(RUNNING)
                doneJobs = self.session.jobsHistory.getJobsInRemoteStatus(DONE)
                clearedJobs = self.session.jobsHistory.getJobsInRemoteStatus(CLEARED)
                killedJobs = self.session.jobsHistory.getJobsInRemoteStatus(KILLED)
                abortedJobs = self.session.jobsHistory.getJobsInRemoteStatus(BAD)
                waitingJobs = self.session.jobsHistory.getJobsInRemoteStatus(WAITING)
                submittedJobs = self.session.jobsHistory.getJobsInRemoteStatus(SUBMITTED)

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


        
