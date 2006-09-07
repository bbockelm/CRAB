# -*- coding: iso-8859-15 -*-

import os, threading, logging
from os import path
from InteractCrab import *
from random import randint
from time import sleep

class ThreadedTest(threading.Thread):
    """
    This class handles a threaded job
    """
    def __init__ (self, cfg, nC, nS, wd):
        """
        The constructor of ThreadedJob takes a working dir, a number of grid jobs to create(nC), a number of grid jobs to submit, a plugin to run for managing the grid jobs and a semaphore to handles the multithreading
        """
        threading.Thread.__init__(self)
        self.cfg = str(cfg)
        self.nC = int(nC)
        self.nS = int(nS)
        self.wd = str(wd)
        self.setName('Thread '+self.cfg)
        self.badLog = None
        self.juiceLog = None

    def getBadLog(self):
        return self.badLog

    def getJuiceLog(self):
        return self.juiceLog

    def run(self):
        """
        This launch the TestSuite embedded code, in orderd to grasp useful information about the tests results
        """
        logging.info('Preparing the test '+self.cfg+' in '+self.wd)
        r = InteractCrab(self.nC, self.nS, True, 60, self.cfg, self.wd)
        logging.info('Running the test '+self.cfg+' in '+r.getRoboLogDir())
        self.badLog = path.abspath(r.getRoboLogDir()+'/Robolog/bad.table.out')
        self.juiceLog = path.abspath(r.getRoboLogDir()+'/Robolog/juice.table.out')
        r.crabRunner()
        logging.info('The test '+self.cfg+' in '+r.getRoboLogDir()+' is finished')
        logging.info('You can find a clean log for '+self.cfg+' in '+self.juiceLog+'.')

