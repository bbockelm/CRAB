# -*- coding: iso-8859-1 -*-
from Session import *
from Tester import Tester

class LinearTester(Tester):
    def __init__(self, configFile, name, timeout, debug = False):
        Tester.__init__(self, configFile, name+"-linear", timeout, debug)
        
    def test(self):
        self.tests["create"] = False
        toSubmit = self.session.crabCreate()
        self.tests["create"] = True

        self.session.crabStatus()

        self.tests["submit"] = False
        self.session.crabSubmit()
        self.tests["submit"] = True

        while self.checkTimeout():
            self.session.crabStatus()
            if self.session.jobsHistory.isChanged():
                self.session.logger.info(str(self.session.jobsHistory))
                            
            jobsDone = self.session.jobsHistory.getJobsInRemoteStatus(DONE)
            if jobsDone:
                try:
                    self.session.crabGetOutput(jobsDone)
                except TestException, txt:
                    self.logger.warning(txt)
                    self.tests["getoutput"] = False
                if self.tests["getoutput"] == None:
                    self.tests["getoutput"] = True
            
            jobsCleared = self.session.jobsHistory.getJobsInRemoteStatus(CLEARED)
            if len(jobsCleared) == self.session.totJobs:
                self.logger.info("All the jobs retrieved!")
                break
