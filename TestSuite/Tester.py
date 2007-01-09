# -*- coding: iso-8859-1 -*-
from Session import *
import threading, os, time
import random

# Tester class: it's an abstract class implementing a thread running a test on CRAB

class Tester(threading.Thread):
    def __init__(self, configFile, name, timeout, debug = False):
        """ Tester constructor.

        Tester constructor: configFile is the path to crab.cfg, name is a identification name for the test and timeout is the
        time after which the test is stopped.
        """
        
        threading.Thread.__init__(self) # Mandatory

        self.configFile = configFile # crab.cfg path
        self.name = name # identification name
        self.root = os.path.abspath(name+"-"+time.strftime("%Y%m%d%H%M%S")) # basename for the test and the log file.
        self.timeout = timeout # timeout after which the test is ended
        self.debug = debug # Be verbouse?

        # Test logger builder
        self.logger = logging.getLogger(self.name)
        logFilename = self.name+"-"+time.strftime("%Y%m%d%H%M%S")+".log"
        self.filelog = logging.FileHandler(filename=logFilename)
        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        self.filelog.setLevel(logging.INFO)
        formatter = logging.Formatter("%(levelname)s\t%(message)s")
        self.filelog.setFormatter(formatter)
        self.logger.addHandler(self.filelog)

        # Tests to run and their results
        self.tests = {"create" : None, "submit" : None, "kill" : None, "getoutput" : None, "resubmit" : None}

        self.logger.info("Running "+self.name+" test (log: "+logFilename+")...")
        self.session = Session(self.logger, self.configFile, self.root) # Creating the CRAB session

        self.begin = time.time() # Starting the timer (for timeout handling)

    def test(self):
        """ The actual implementation of the test. """
        raise Exception, "This class must be derived, and the method test must be overloaded, in order to use this class"

    def run(self):
        """ Thread main method. """
        # Test
        try:
            self.test()
            self.logger.info(self.name+" ended successfully!")
        except TestException, txt:
            self.dumpError(txt)
            
        except CrabException, (cmd, returncode, outdata, errdata):
            error = "\nError executing: " + str(cmd) + "\n"
            error += "Return code: " + str(returncode) + "\n"
            error += "\n--- Last command STDOUT ---\n" + outdata + "\n---------------------------\n"
            error += "\n--- Last command STDERR ---\n" + errdata + "\n---------------------------\n"
            self.logger.error(error)

        # Test finished. Dumping results
        try:
            self.session.crabPostMortem()
        except CrabException, (cmd, returncode, outdata, errdata):
            error = "Yes: it happens, even crab -postmortem could fail!\n"
            error += "Return code: " + str(returncode) + "\n"
            error += "\n--- Last command STDOUT ---\n" + outdata + "\n---------------------------\n"
            error += "\n--- Last command STDERR ---\n" + errdata + "\n---------------------------\n"
            self.logger.error(error)

        # Dumping the jobs history
        self.logger.info("\n------ Jobs history -------\n" + self.session.jobsHistory.__repr__() + "\n---------------------------\n")

        # Dumping crab actions tests performed
        for testname,result in self.tests.iteritems():
            if result == None:
                self.logger.warning(testname + ": test not performed...")
            elif result:
                self.logger.info(testname + ": test OK!")
            else:
                self.logger.error(testname + ": some or all tests failed!!")
                        
        self.logger.info(self.name+" test completed!")
        self.logger.info("Please clean "+str(self.session.cwd)+" by hand.")

    def dumpError(self, txt):
        """ Dump an error, printing txt and last command executed output. """
        error = str(txt)+"\n"
        error += "Last command: "+self.session.cmd + "\n"
        error += "\n--- Last command STDOUT ---\n"+self.session.outdata+"\n---------------------------\n"
        error += "\n--- Last command STDERR ---\n"+self.session.errdata+"\n---------------------------\n"
        self.logger.error(error)
        

    def checkTimeout(self):
        """ Check the timeout. """
        still = self.timeout - (time.time() - self.begin)
        self.logger.info("Time remaining: "+str(int(still))+"s") 
        sleep(10)
        return still >= 0.

    def randomList(self, jobsList, tot):
        """ Handy method to create a random job number set with a random behaviour.

        There are 4 predefined behaviour:
        * producing a list of all the jobs from 1 to tot
        * producing the input jobsList
        * producing a subset of jobsList
        * producing a subset of all the jobs
        if the set produced is empty it returns the jobsList itself
        """
        r = random.random()
        if r < 0.25: # All the jobs
            ret = range(1, tot+1)
        elif r < 0.75: # Exactly the requested jobs
            ret = jobsList
        elif r < 0.90: # A subset of the requested jobs
            ret = [x for x in jobsList if random.random() > .25]
        else: # A subset of all the jobs
            ret = [x for x in range(1, tot+1) if random.random() > .25]
        ret = set(ret)
        jobsList = set(jobsList)
        if ret & jobsList: #
            return ret
        else:
            return jobsList

