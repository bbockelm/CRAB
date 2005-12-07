import string
from crab_exceptions import *
from WorkSpace import WorkSpace
from threading import RLock
import common

class Logger :

    #_instance = None

    #def getInstance():
    #    if not Logger._instance :
    #        Logger._instance = Logger()
    #    return Logger._instance
    
    #getInstance = staticmethod(getInstance)

    #def hasInstance():
    #    return Logger._instance

    #hasInstance = staticmethod(hasInstance)

    def __init__(self):
        #if Logger._instance:
        #    raise CrabException, 'Logger already exists.'

        self.lock = RLock()
        self.debug_level = 0
        self.flag_quiet = 0
        self.prog_name = common.prog_name
        log_dir = common.work_space.logDir()
        self.log_fname = log_dir+self.prog_name+'.log'
        self.log_file = open(self.log_fname, 'a')
        self.log_file.write('\n-------------------------------------------\n')

        #Logger._instance = self
        return

    def __del__(self):
        if not self.flag_quiet:
            print self.prog_name+'. Log-file is '+self.log_fname
        self.log_file.close()
        return
    
    def close(self):
        self.log_file.close()
        return

    def get(self):
        """
        Returns list of lines in the log-file.
        """
        logf = open(self.log_fname, 'r')
        lines = logf.readlines()
        logf.close()
        return lines

    def quiet(self, flag):
        self.flag_quiet = flag
        return

    def flush(self):
        self.log_file.flush()
        return

    def write(self, msg):
        """
        Stores the given message into log-file.
        """
        self.lock.acquire()
        self.log_file.write(msg)
        self.lock.release()
        return

    def message(self, msg):
        """
        Prints the given message on a screen and stores it into log-file.
        """
        if len(msg) == 0: return
        self.lock.acquire()

        # print whitespace first
        for i in range(len(msg)):
            if msg[i] in string.whitespace:
                if not self.flag_quiet: print msg[i],
                self.log_file.write(msg[i])
            else: break
            pass

        # print the rest of the message prefixing with the program name
        msg0 = msg[i:]
        if not self.flag_quiet: print self.prog_name+'. '+msg0
        self.log_file.write(msg0+'\n')
        self.log_file.flush()
        self.lock.release()
        return

    def debugLevel(self):
        return self.debug_level

    def setDebugLevel(self, level):
        self.debug_level = level
        return

    def debug(self, level, msg):
        self.lock.acquire()
        if level <= self.debug_level: self.message(msg)
        self.lock.release()
        return
    
