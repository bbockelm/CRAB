# -*- coding: iso-8859-15 -*-

import os, subprocess, logging
from os import path
from InteractCrab import *
from random import randint
from time import sleep

class ProcessTest:
    """
    This class handles a threaded job
    """
    def __init__ (self, cfg, nC, nS, wd, debug):
        """
        The constructor of ThreadedJob takes a working dir, a number of grid jobs to create(nC), a number of grid jobs to submit, a plugin to run for managing the grid jobs and a semaphore to handles the multithreading
        """
        self.cfg = str(cfg)
        self.nC = nC
        self.nS = nS
        self.wd = str(wd)
        self.name = 'Process '+self.cfg
        self.outLog = None
        self.debug

    def getOutLog(self):
        return self.outLog

    def getName(self):
        return self.name

    def run(self):
        """
        This launch the TestSuite embedded code, in orderd to grasp useful information about the tests results
        """
        logging.info('Preparing the test '+self.cfg+' in '+self.wd)
        logging.debug(str(['TestSuite', str(self.nC), str(self.nS), '-cfg', self.cfg]))
        return subprocess.Popen (['TestSuite', str(self.nC), str(self.nS), '-cfg', self.cfg, '-dbg', self.debug], shell=False, cwd=self.wd, stdout=PIPE, stderr=PIPE)

