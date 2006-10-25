#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys, logging, subprocess
from optparse import OptionParser
from Scanner import *
from ThreadedTest import *
from ProcessTest import *
from os import path
from subprocess import Popen
from ProxyInit import *
from sys import stderr

class TestSuite:
    def __init__(self):
        self.t = []
        self.options = None
        self.getOptions()
        self.getConfig()

    def printHelp(self):
        print >> stderr
        print >> stderr, "**********************************************************"
        print >> stderr, "* TestSuite needs a simple config file to get happy.     *"
        print >> stderr, "* Each row represents a particular test to run.          *"
        print >> stderr, "* A row must contain a comma-separated of 4 data:        *"
        print >> stderr, "*         testname, crab.cfg filename, nC, nS            *"
        print >> stderr, "* where testname is a nice name to display               *"
        print >> stderr, "* crab.cfg name is the name of the crab.cfg to run       *"
        print >> stderr, "* nC is the number of jobs to create (<1 for all)        *"
        print >> stderr, "* nS is the number of jobs to submit (<1 for all)        *"
        print >> stderr, "**********************************************************"


    def getOptions (self):
        #logging.debug('Parsing the command line')      ## Matt
        logging.debug('Parsing the command line') ## Sk.
        parser = OptionParser(version='0.1')
        parser.add_option('-c', '--config', action='store', type='string', dest='config', default='TestSuite.cfg', help='set the config file of the testsuite (default %default)')
        parser.add_option('-l', '--logname', action='store', type='string', dest='log', default='TestSuite.log', help='set the log file name of the TestSuite (default %default)')
        parser.add_option('-t', '--usethreads', action='store_true', dest='threads', default=True, help='Use threads (default)' )
        parser.add_option('-p', '--useprocesses', action='store_false', dest='threads', default=True, help='Use processes (\'only for testing purpose\')')
        parser.add_option('-d', '--debug', action='store', type='int', dest='debug', default=0, help='Activate debug output (the greater the value the more verbouse output) (default: %default)')
        (self.options, args) = parser.parse_args()

        self.options.config=path.abspath(self.options.config.strip())
        self.options.log=path.abspath(self.options.log.strip())

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
        ##logging.debug('Parsing the config file')      ## Matt
        logging.debug('Parsing the config file') # Sk.
        i = 1
        for line in open(self.options.config):
            line = line.strip()
            if line != '' and line[0] != '#':
                try:
                    (nicename, cfg, nC, nS) = line.split(',', 4)
                except ValueError:
                    self.printHelp();
                    logging.error('Error while reading in '+self.options.config+' rows: '+str(i))

                logging.debug('Read: nicename='+nicename+', cfg='+cfg+', nC='+str(nC)+', nS='+str(nS))
                cfg = path.abspath(cfg.strip())
                try: # Check of nC
                    nC = int(nC)
                    #if nC < 1:
                    #    self.printHelp()
                    #    logging.error('nC must be a value greater than 0 in '+self.options.config+' row: '+str(i))
                except ValueError:
                    self.printHelp()
                    logging.error('nC must be an integer value greater than 0 in '+self.options.config+' row: '+str(i))
                try: # Check of nS
                    nS = int(nS)
                    #if nS < 1:
                    #    self.printHelp()
                    #    logging.error('nS must be a value greater than 0 in '+self.options.config+' row: '+str(i))
                except ValueError:
                    self.printHelp()
                    logging.error('nS must be an integer value greater than 0 in '+self.options.config+' row: '+str(i))
                try: # Check of cfg
                    open (cfg, 'r')
                except IOError, msg:
                    self.printHelp()
                    logging.error(cfg+' can\'t be opened for reading: '+str(msg))
                self.t.append((nicename, cfg, nC, nS))
            i += 1
        if (len(self.t) == 0):
            logging.error('Empty config file!? Nothing to do!')
            self.printHelp();
            sys.exit(1)

    def printDebugThrd(self, strr):           ## Matt
        if self.options.debug > 1:
            logging.info( strr )
        else:
            logging.debug(strr)


    def mainThreads(self):
        ##logging.debug('Starting tests...')
        self.printDebugThrd('Starting tests...')       ## Matt
        tests = []
        for (nicename, cfg, nC, nS) in self.t:
            test = ThreadedTest (nicename, cfg, nC, nS, os.getcwd(), self.options.debug)
            #logging.debug('Thread '+test.getName()+' initialized')
            self.printDebugThrd('Thread '+test.getName()+' initialized')   ## Matt
            tests.append(test)
            test.start()
            #logging.debug('Thread '+test.getName()+' started')
            self.printDebugThrd('Thread '+test.getName()+' started')       ## Matt
            time.sleep (15) # To not fall into the Boss 3.6 "concurrent bug"
        testerLog = open(self.options.log, 'a')
        for test in tests:
            #logging.debug('Waiting for '+test.getName())
            self.printDebugThrd('Waiting for '+test.getName())             ## Matt
            test.join()
            #logging.debug('Joined with '+test.getName())
            self.printDebugThrd('Joined with '+test.getName())             ## Matt
            try:
                for line in open(test.getBadLog()):
                    if 'Failed' in line or 'missing' in line:
                        testerLog.write(test.getName()+'\t'+line)
            except OSError, msg:
                logging.warning('Unable to write ouputlog concerning the test '+test.getName()+': '+str(msg))

    def mainProcesses(self):
        logging.debug('Starting tests...')
        tests = []
        for (cfg, nC, nS) in self.t:
            test = ProcessTest (cfg, nC, nS, os.getcwd(), self.options.debug)
            logging.debug('Process '+test.getName()+' initialized')
            tests.append((test, test.run()))
            logging.debug('Process'+test.getName()+' started')
            #time.sleep (15) # To not fall into the Boss 3.6 "concurrent bug"
        testerLog = open(self.options.log, 'a')
        for (test, p) in tests:
            logging.debug('Waiting for '+test.getName())
            (outdata, errdata) = p.communicate()
            logging.debug('Joined with '+test.getName())
            testerLog.write("Stdout of " + test.getName())
            for line in outdata:
                testerLog.write(line)
            testerLog.write("Stderr of " + test.getName())
            for line in errdata:
                testerLog.write(line)


if __name__=='__main__':
    #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
    logging.basicConfig(level=logging.INFO, format='TestSuite. %(levelname)s: %(message)s')
    t = TestSuite()
    p = ProxyInit(t.t[0][1]) # Necessary!
    p.checkProxy() # Necessary!
    if t.options.threads:
        t.mainThreads()
    else:
        t.mainProcesses()
