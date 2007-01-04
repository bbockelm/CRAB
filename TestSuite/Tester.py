# -*- coding: iso-8859-1 -*-
from Session import *
import threading, os, time
import random

class Tester(threading.Thread):
    def __init__(self, configFile, name, timeout):
        """ Tester constructor.

        Tester constructor: configFile is the path to crab.cfg, name is a identification name for the test and timeout is the
        time after which the test is stopped.
        """
        
        threading.Thread.__init__(self)

        self.configFile = configFile
        self.name = name
        self.root = os.path.abspath(name+"-"+time.strftime("%Y%m%d%H%M%S")) # basename for the test and the log file.
        self.timeout = timeout
        
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        self.filelog = logging.FileHandler(filename=self.name+"-"+time.strftime("%Y%m%d%H%M%S")+".log")
        self.filelog.setLevel(logging.INFO)
        #self.stdout = logging.StreamHandler()
        #self.stdout.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
        #formatter2 = logging.Formatter("%(name)s\t%(asctime)s\t%(levelname)s\t%(message)s")
        self.filelog.setFormatter(formatter)
        #self.stdout.setFormatter(formatter2)
        self.logger.addHandler(self.filelog)
        #self.logger.addHandler(self.stdout)

    def run(self):
        try:
            self.logger.info("Running Linear Test")
            s = Session(self.logger, self.configFile, self.root+"-linear")
            self.linearTester(s)
            self.logger.info("Linear Test ended successfully!")

            self.logger.info("Running Wow Test")
            s = Session(self.logger, self.configFile, self.root+"-wow")
            self.wowTester(s)
            self.logger.info("Wow Test ended successfully!")

        except TestException, txt:
            self.logger.error(txt)
        except CrabException, (cmd, returncode, outdata, errdata):
            self.logger.error("Error executing: "+str(cmd))
            self.logger.error("Return log: "+returncode)
            self.logger.error("\n--- Last command STDOUT ---\n"+outdata+"\n----------------------- ---\n")
            self.logger.error("\n--- Last command STDERR ---\n"+errdata+"\n----------------------- ---\n")
        self.logger.error("\nJobs history\n"+s.jobsHistory.__repr__())
        #returncode, outdata, errdata = s.crabPostMortem()
        s.crabPostMortem()
        #self.logger.error("\n--- Crab -status STDOUT ---\n"+outdata+"\n----------------------- ---\n")
        #self.logger.error("\n--- Crab -status STDERR ---\n"+errdata+"\n----------------------- ---\n")
        self.filelog.close()

    def linearTester(self, session):
        session.crabCreate()
        session.logger.info("Create test: OK!")
        session.crabSubmit()
        session.logger.info("Submit test: OK!")
        getoutputTest = False
        ok = True
        begin = time.time()
        while time.time() - begin <= self.timeout or not getoutputTest:
            session.logger.info("Time remaining: "+str(int(self.timeout-(time.time()-begin)))+"s")
            sleep(10)
            session.crabStatus()
            if session.jobsHistory.isChanged():
                session.logger.info(str(session.jobsHistory))
                            
            jobsDone = session.jobsHistory.getJobsInRemoteStatus(DONE)
            if jobsDone:
                session.crabGetOutput(jobsDone)
                getoutputTest = True
            jobsCleared = session.jobsHistory.getJobsInRemoteStatus(CLEARED)
            if len(jobsCleared) == session.totJobs:
                break
        if getoutputTest:
            session.logger.info("GetOutput test: OK!")
        if len(jobsCleared) < session.totJobs:
            session.logger.warning("Not every output was retrieved!")
        session.logger.info("Linear test completed!")
        session.logger.info("Please clean "+session.cwd+" by hand.")

    def wowTester(self, session):
        toSubmit = session.crabCreate()
        session.logger.info("Create test: OK!")
        begin = time.time()
        killTest = False
        resubmitTest = False
        getoutputTest = False
        while time.time() - begin <= self.timeout:
            session.logger.info("Time remaining: "+str(int(self.timeout-(time.time()-begin)))+"s")
            sleep(10)
            session.crabStatus()
            if session.jobsHistory.isChanged():
                session.logger.info(str(session.jobsHistory))
            clearedJobs = set()
            if len(toSubmit) > 0 and random.random() > .75:
                self.logger.debug("submitting...")
                weSubmit = random.randint(1, len(toSubmit))
                submitted = session.crabSubmit(weSubmit)
                toSubmit.difference_update(submitted) # rimuovo dai job da sottomettere quelli sottomessi
            else:
                self.logger.debug("playing...")
                runningJobs = session.jobsHistory.getJobsInRemoteStatus(RUNNING)
                doneJobs = session.jobsHistory.getJobsInRemoteStatus(DONE)
                clearedJobs = session.jobsHistory.getJobsInRemoteStatus(CLEARED)
                killedJobs = session.jobsHistory.getJobsInRemoteStatus(KILLED)
                abortedJobs = session.jobsHistory.getJobsInRemoteStatus(BAD)
                waitingJobs = session.jobsHistory.getJobsInRemoteStatus(WAITING)
                submittedJobs = session.jobsHistory.getJobsInRemoteStatus(SUBMITTED)

                killable = runningJobs | waitingJobs | submittedJobs
                retrievable = doneJobs
                resubmittable = abortedJobs | killedJobs

                self.logger.debug("killable->"+str(killable)+" retrievable->"+str(retrievable)+" resubmittable->"+str(resubmittable))

                if retrievable and random.random() > 0.25:
                    self.logger.debug("retrieving")
                    toRetrieve = self.randomList(retrievable, session.totJobs)
                    session.crabGetOutput(toRetrieve, retrievable & toRetrieve)
                    getoutputTest = True
                    session.logger.info("Getoutput tests: OK!")
                elif resubmittable and random.random() > 0.5:
                    self.logger.debug("resubmitting")
                    toResubmit = self.randomList(resubmittable, session.totJobs)
                    session.crabResubmit(toResubmit, resubmittable & toResubmit)
                    resubmitTest = True
                    session.logger.info("Resubmit tests: OK!")
                elif killable and random.random() > 0.75:
                    self.logger.debug("killing")
                    toKill = self.randomList(killable, session.totJobs)
                    session.crabKill(toKill, killable & toKill)
                    killTest = True
                    session.logger.info("Kill tests: OK!")
            if len(clearedJobs) == session.totJobs:
                break;
        if not killTest:
            session.logger.warning("Not tested crab -kill")
        if not resubmitTest:
            session.logger.warning("Not tested crab -resubmit")
        if not getoutputTest:
            session.logger.warning("Not tested crab -getoutput")
                        
        session.logger.info("Linear test completed!")
        session.logger.info("Please clean "+session.cwd+" by hand.")

    def randomList(self, jobsList, tot):
        r = random.random()
        if r < 0.25: # Tutti i job
            ret = range(1, tot+1)
        elif r < 0.75: # Esattamente quelli richiesti
            ret = jobsList
        elif r < 0.90: # Un sottoinsieme di quelli richiesti
            ret = [x for x in jobsList if random.random() > .25]
        else: # Un sottoinsieme di tutti i job
            ret = [x for x in range(1, tot+1) if random.random() > .25]
        if ret:
            return set(ret)
        else:
            return set(jobsList)

