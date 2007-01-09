# -*- coding: iso-8859-1 -*-
from Session import *
import threading, os, time
import random

class Tester(threading.Thread):
    def __init__(self, configFile, name, timeout, debug = False):
        """ Tester constructor.

        Tester constructor: configFile is the path to crab.cfg, name is a identification name for the test and timeout is the
        time after which the test is stopped.
        """
        
        threading.Thread.__init__(self)

        self.configFile = configFile
        self.name = name
        self.root = os.path.abspath(name+"-"+time.strftime("%Y%m%d%H%M%S")) # basename for the test and the log file.
        self.timeout = timeout
        self.debug = debug
        
        self.logger = logging.getLogger(self.name)
        self.filelog = logging.FileHandler(filename=self.name+"-"+time.strftime("%Y%m%d%H%M%S")+".log")
        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        self.filelog.setLevel(logging.INFO)
        formatter = logging.Formatter("%(levelname)s\t%(message)s")
        self.filelog.setFormatter(formatter)
        
        self.logger.addHandler(self.filelog)
        self.tests = {"create" : None, "submit" : None, "kill" : None, "getoutput" : None, "resubmit" : None}
        self.logger.info("Running "+self.name+" test...")
        self.session = Session(self.logger, self.configFile, self.root)
        self.begin = time.time() # Time when the test begin

        self.timerLogger = logging.getLogger(self.name+"-timer")
        timerLog = logging.StreamHandler()
        timerFormatter = logging.Formatter("%(name)s\t%(message)s")
        timerLog.setFormatter(timerFormatter)
        timerLog.setLevel(logging.INFO)
        self.timerLogger.addHandler(timerLog)


    def run(self):
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
        try:
            self.session.crabPostMortem()
        except CrabException, (cmd, returncode, outdata, errdata):
            error = "Yes: it happens, even crab -postmortem could fail!\n"
            error += "Return code: " + str(returncode) + "\n"
            error += "\n--- Last command STDOUT ---\n" + outdata + "\n---------------------------\n"
            error += "\n--- Last command STDERR ---\n" + errdata + "\n---------------------------\n"
            self.logger.error(error)
            
        self.logger.info("\n------ Jobs history -------\n" + self.session.jobsHistory.__repr__() + "\n---------------------------\n")
            
        for testname,result in self.tests.iteritems():
            if result == None:
                self.logger.warning(testname + ": test not performed...")
            elif result:
                self.logger.info(testname + ": test OK!")
            else:
                self.logger.error(testname + ": some or all tests failed!!")
                        
        self.logger.info(self.name+" test completed!")
        self.logger.info("Please clean "+self.session.cwd+" by hand.")

    def dumpError(self, txt):
        error = str(txt)+"\n"
        error += "Last command: "+self.session.cmd + "\n"
        error += "\n--- Last command STDOUT ---\n"+self.session.outdata+"\n---------------------------\n"
        error += "\n--- Last command STDERR ---\n"+self.session.errdata+"\n---------------------------\n"
        self.logger.error(error)
        

    def checkTimeout(self):
        still = self.timeout - (time.time() - self.begin)
        self.logger.info("Time remaining: "+str(int(still))+"s") 
        #self.logger.info("Time remaining: "+str(int(still))+"s")
        sleep(10)
        return still >= 0.

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
        ret = set(ret)
        jobsList = set(jobsList)
        if ret & jobsList: # If there's at least one job in the required situation
            return set(ret)
        else:
            return set(jobsList)

