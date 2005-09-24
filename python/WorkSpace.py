from crab_exceptions import *
from threading import RLock
import common

import os, shutil, string, time

class WorkSpace:

    #_instance = None

    #def getInstance():
    #    if not WorkSpace._instance :
    #        raise CrabException('There is no instance of WorkSpace.')
    #    return WorkSpace._instance
    
    #getInstance = staticmethod(getInstance)

    def __init__(self, top_dir):
        #if WorkSpace._instance:
        #    raise CrabException('WorkSpace already exists.')

        self.lock = RLock()
        self._cwd_dir = os.getcwd()+'/'
        self._top_dir = top_dir                    # top working directory
        self._log_dir = self._top_dir + '/log'     # log-directory
        self._job_dir = self._top_dir + '/job'     # job pars, scripts, jdl's
        self._res_dir = self._top_dir + '/res'     # dir to store job results
        self._share_dir = self._top_dir + '/share' # directory for common stuff

        #WorkSpace._instance = self
        return

    def create(self):
        if not os.path.exists(self._top_dir):
            os.mkdir(self._top_dir)
            os.mkdir(self._log_dir)
            os.mkdir(self._job_dir)
            os.mkdir(self._res_dir)
            os.mkdir(self._share_dir)

            fileCODE1 = open(self._log_dir+"/.code","w")
            fileCODE1.write(str(time.time()))
            fileCODE1.close()
            pass
        return

    def delete(self):
        """
        delete the whole workspace without doing any test!!!
        """
        if os.path.exists(self._top_dir):
            shutil.rmtree(self._top_dir)
            pass
        return

    def cwdDir(self):
        return self._cwd_dir + '/'

    def topDir(self):
        return self._top_dir + '/'

    def logDir(self):
        return self._log_dir + '/'

    def jobDir(self):
        return self._job_dir + '/'

    def resDir(self):
        return self._res_dir + '/'

    def shareDir(self):
        return self._share_dir + '/'

    def setResDir(self, dir):
        self._res_dir = dir
        return

    def saveFileName(self):
        return self.shareDir() + common.prog_name + '.sav'

    def cfgFileName(self):
        return self.shareDir() + common.prog_name + '.cfg'

    def saveConfiguration(self, opts, cfg_fname):

        # Save options
        
        save_file = open(self.saveFileName(), 'w')

        for k in opts.keys():
            if opts[k] : save_file.write(k+'='+opts[k]+'\n')
            else       : save_file.write(k+'\n')
            pass
        
        save_file.close()

        # Save cfg-file

        shutil.copyfile(cfg_fname, self.cfgFileName())

        return

    def loadSavedOptions(self):
        
        # Open save-file

        try:
            save_file = open(self.saveFileName(), 'r')
        except IOError, e:
            msg = 'Misconfigured continuation directory:\n'
            msg += str(e)
            raise CrabException(msg)

        # Read saved options

        save_opts = {}
        for line in save_file:
            line = line[:-1]  # drop '\n'
            try:
                (k, v) = string.split(line, '=')
            except:
                k=line
                v=''
                pass
            save_opts[k] = v
            pass
        
        save_file.close()
        return save_opts
    
