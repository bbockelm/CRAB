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
    def __init__ (self, nicename, cfg, nC, nS, wd, debug):
        """
        The constructor of ThreadedJob takes a working dir, a number of grid jobs to create(nC), a number of grid jobs to submit, a plugin to run for managing the grid jobs and a semaphore to handles the multithreading
        """
        threading.Thread.__init__(self)
        self.nicename = str(nicename)
        self.cfg = str(cfg)
        self.nC = nC
        self.nS = nS
        self.wd = str(wd)
        self.setName(self.nicename)
        self.badLog = None
        self.juiceLog = None
        self.debug = debug

    def getBadLog(self):
        return self.badLog

    def getJuiceLog(self):
        return self.juiceLog

    def run(self):
        """
        This launch the TestSuite embedded code, in orderd to grasp useful information about the tests results
        """
        logging.info('Preparing the test '+self.nicename+' in '+self.wd+'\n')
        r = InteractCrab(self.nC, self.nS, self.debug, 60, self.cfg, self.wd)
        logging.info('Running the test '+self.nicename+' in '+r.getRoboLogDir()+'\n')
        self.badLog = path.abspath(r.getRoboLogDir()+'/Robolog/bad.table.out')
        self.juiceLog = path.abspath(r.getRoboLogDir()+'/Robolog/juice.table.out')
        r.crabRunner()
        logging.info('The test '+self.nicename+' in '+r.getRoboLogDir()+' is finished'+'\n')
        logging.info('You can find a clean log for '+self.nicename+' in '+self.juiceLog+'.\n')

