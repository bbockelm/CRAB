# -*- coding: iso-8859-1 -*-

import os, sys, logging, subprocess, time
from optparse import OptionParser
from Tester import *
from WowTester import WowTester
from LinearTester import LinearTester
from os import path
from subprocess import Popen
from ProxyInit import *
from sys import stderr
from threading import BoundedSemaphore

class TestSuite:
    def __init__(self):
        self.t = []
        self.options = None
        self.getOptions()
        self.getConfig()
        briefLog = logging.getLogger("brief")
        filelog = logging.FileHandler("TestSuite-brief.log")
        briefLog.setLevel(logging.INFO)
        filelog.setLevel(logging.INFO)
        formatter = logging.Formatter("%(message)s")
        filelog.setFormatter(formatter)
        briefLog.addHandler(filelog)


    def printHelp(self):
        print >> stderr
        print >> stderr, "**********************************************************"
        print >> stderr, "* TestSuite needs a simple config file to get happy.     *"
        print >> stderr, "* Each row represents a particular test to run.          *"
        print >> stderr, "* A row must contain a comma-separated of 3 data:        *"
        print >> stderr, "*         testname, crab.cfg filename, timeout, timeout2 *"
        print >> stderr, "* where testname is a nice name to display               *"
        print >> stderr, "* crab.cfg name is the name of the crab.cfg to run       *"
        print >> stderr, "* timeout after which the linear test ends (in seconds)  *"
        print >> stderr, "* timeout2 after which the wow test ends (in seconds)    *"
        print >> stderr, "**********************************************************"


    def getOptions (self):
        logging.debug('Parsing the command line') 
        parser = OptionParser(version='1.0.1')
        parser.add_option('-c', '--config', action='store', type='string', dest='config', default='TestSuite.cfg', help='set the config file of the testsuite (default %default)')
        parser.add_option('-l', '--logname', action='store', type='string', dest='log', default='TestSuite.log', help='set the log file name of the TestSuite (default %default)')
        parser.add_option('-d', '--debug', action='store', type='int', dest='debug', default=0, help='Activate debug output (the greater the value the more verbouse output) (default: %default)')
        parser.add_option('-t', '--threads', action='store', type='int', dest='threads', default=3, help='Max number of threads (default: %default)')
        (self.options, args) = parser.parse_args()

        self.options.config=path.abspath(self.options.config.strip())
        self.options.log=path.abspath(self.options.log.strip())

        if self.options.debug:
            logging.basicConfig(level=logging.DEBUG, format='%(name)s\t%(levelname)s\t%(message)s')
        else:
            logging.basicConfig(level=logging.INFO, format='%(name)s\t%(levelname)s\t%(message)s')
            

        try:
            open(self.options.config)
        except IOError, msg:
            self.printHelp()
            parser.error('Error in opening the config file '+self.options.config+': '+str(msg))

        try:
            open(self.options.log, 'a')
        except IOError, msg:
            parser.error('Error while creating the log file '+self.options.log+': '+str(msg))

    def getConfig (self):
        logging.debug('Parsing the config file') # Sk.
        i = 1
        for line in open(self.options.config):
            line = line.strip()
            if line != '' and line[0] != '#':
                try:
                    nicename, cfg, timeout, timeout2 = line.split(',', 3)
                except ValueError:
                    self.printHelp()
                    logging.error('Error while reading in '+self.options.config+' rows: '+str(i))

                logging.debug('Read: nicename='+nicename+', cfg='+cfg)
                cfg = path.abspath(cfg.strip())
                try: # Check of cfg
                    open (cfg, 'r')
                except IOError, msg:
                    self.printHelp()
                    logging.error(cfg+' can\'t be opened for reading: '+str(msg))
                try:
                    timeout=float(timeout)
                except ValueError:
                    self.printHelp()
                    logging.error('Linear Timeout must be a number in seconds')
                
                try:
                    timeout2=float(timeout2)
                except ValueError:
                    self.printHelp()
                    logging.error('Wow Timeout must be a number in seconds')
                
                self.t.append((nicename, cfg, timeout, timeout2))
            i += 1
        if (len(self.t) == 0):
            logging.error('Empty config file!? Nothing to do!')
            self.printHelp();
            sys.exit(1)

    def mainThreads(self):
        logging.debug('Starting tests...')
        semaphore = BoundedSemaphore(self.options.threads)
        tests = []
        for (nicename, cfg, timeout, timeout2) in self.t:
            semaphore.acquire()
            test = LinearTester (cfg, nicename, timeout, semaphore, self.options.debug)
            logging.debug('Thread '+test.getName()+' initialized')
            tests.append(test)
            test.start()
            logging.debug('Thread '+test.getName()+' started')
            
            semaphore.acquire()
            test = WowTester (cfg, nicename, timeout2, semaphore, self.options.debug)
            logging.debug('Thread '+test.getName()+' initialized')
            tests.append(test)
            test.start()
            logging.debug('Thread '+test.getName()+' started')

            
        for test in tests:
            logging.debug('Waiting for '+test.getName())
            test.join()
            logging.debug('Joined with '+test.getName())


if __name__=='__main__':
    checkProxies()
    t = TestSuite()
    t.mainThreads()
