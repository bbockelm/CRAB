from crab_exceptions import *
#from threading import RLock
import common

import os, shutil, string, time

class WorkSpace:
    def __init__(self, top_dir, cfg_params):
        self._cwd_dir = os.getcwd()+'/'
        self._top_dir = top_dir                    # top working directory
        self._log_dir = self._top_dir + '/log'     # log-directory
        self._job_dir = self._top_dir + '/job'     # job pars, scripts, jdl's
        self._res_dir = self._top_dir + '/res'     # dir to store job results
        self._share_dir = self._top_dir + '/share' # directory for common stuff

        try:    
            self.outDir = cfg_params["USER.outputdir"]
        except:
            self.outDir = self._res_dir
        try:
            self.log_outDir = cfg_params["USER.logdir"]
        except:
            self.log_outDir = self._res_dir 
        return

    def create(self):
        if not os.path.exists(self._top_dir):
            os.mkdir(self._top_dir)
            pass
        if not os.listdir(self._top_dir):
            os.mkdir(self._log_dir)
            os.mkdir(self._job_dir)
            os.mkdir(self._res_dir)
            os.mkdir(self._share_dir)

            common.taskDB.setDict("CODE",(str(time.time())))
            pass

        # fede
        if not os.path.exists(self.outDir):
            try: 
                os.mkdir(self.outDir)
            except:
                msg = 'Cannot mkdir ' + self.outDir + ' Check permission'
                raise CrabException(msg)
        if os.listdir(self.outDir):
            msg = self.outDir + ' already exists and is not empty. Please remove it before create new task'
            raise CrabException(msg)
        if not os.path.exists(self.log_outDir):
            try:
                os.mkdir(self.log_outDir)
            except:
                msg = 'Cannot mkdir ' + self.log_outDir + ' Check permission'
                raise CrabException(msg)
            pass 
        if os.listdir(self.log_outDir):
            msg = self.log_outDir + ' already exists and is not empty. Please remove it before create new task'
            raise CrabException(msg)
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
    
