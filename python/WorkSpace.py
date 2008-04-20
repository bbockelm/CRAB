from crab_exceptions import *
#from threading import RLock
import common
import os, shutil, string, time, commands 
#from crab_logger import Logger

class WorkSpace:
    def __init__(self, top_dir, cfg_params):

        self._cwd_dir = os.getcwd()+'/'
        self._top_dir = top_dir    # top working directory

        #Matteo: Necessary to manage user ui_working_dir
        if 'USER.ui_working_dir' in cfg_params.keys():    
            self._top_dir = os.path.abspath(cfg_params['USER.ui_working_dir'])
            self._pathForTgz = self._top_dir

        self._log_dir = cfg_params.get("USER.logdir",self._top_dir + '/log')    # log-directory
        self._job_dir = self._top_dir + '/job'     # job pars, scripts, jdl's
        self._res_dir = cfg_params.get("USER.outputdir", self._top_dir + '/res')     # dir to store job results
        self._share_dir = self._top_dir + '/share' # directory for common stuff

        #Matteo: Necessary to manage user ui_working_dir
        if 'USER.ui_working_dir' not in cfg_params.keys():    
            self._pathForTgz = string.split(top_dir, '/')[-1]

        self.uuid = commands.getoutput('uuidgen')

    def create(self):
        # Matteo change in order to ban only "data" in "CMSSW" dir and 
        # not crash when short path is given    
        subpath = self._top_dir.split('CMSSW')
        if len(subpath)!=1 and len(subpath[-1].split("data"))!=1:  
            msg = 'Cannot run CRAB from "data" directory.\n'
            msg += 'please change direcotry\n'
            raise CrabException(msg)

        if not os.path.exists(self._top_dir):
            try:
                os.mkdir(self._top_dir)
            except OSError:
                msg = 'Cannot create '+str(self._top_dir) +' directory.\n'
                raise CrabException(msg)
            pass
        if not os.listdir(self._top_dir):
            os.mkdir(self._job_dir)
            os.mkdir(self._share_dir)
            pass

        # Some more check for _res_dir, since it can be user defined
        if not os.path.exists(self._res_dir):
            try: 
                os.mkdir(self._res_dir)
            except:
                msg = 'Cannot mkdir ' + self._res_dir + ' Check permission'
                raise CrabException(msg)
        if os.listdir(self._res_dir):
            msg = self._res_dir + ' already exists and is not empty. Please remove it before create new task'
            raise CrabException(msg)
        # ditto for _log_dir
        if not os.path.exists(self._log_dir):
            try:
                os.mkdir(self._log_dir)
            except:
                msg = 'Cannot mkdir ' + self._log_dir + ' Check permission'
                raise CrabException(msg)
            pass 
        if os.listdir(self._log_dir):
            msg = self._log_dir + ' already exists and is not empty. Please remove it before create new task'
            raise CrabException(msg)
        return

    def delete(self):
        """
        delete the whole workspace without doing any test!!!
        """
        common.logger.quiet(1)
        common.logger.close()
        if os.path.exists(self._top_dir):
#            shutil.rmtree(self._top_dir)
            # os.system("/usr/sbin/lsof %s/crab.log" % self._log_dir ) 
            os.system("rm -rf %s" % self._top_dir ) 
            # SL For some obscure reason the lgo dir is not removed at the first try
            os.system("rm -rf %s" % self._top_dir )
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
        
    def pathForTgz(self):
        return self._pathForTgz + '/'
        
    def taskName(self):

        self.taskName_=os.environ['USER'] + '_' + string.split(common.work_space.topDir(),'/')[-2]+'_'+self.uuid
        return self.taskName_

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
    
