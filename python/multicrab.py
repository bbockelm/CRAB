#!/usr/bin/env python
import sys, os, time, string, shutil
from crab_util import *
from crab import *
import common

###########################################################################
class MultiCrab:
    def __init__(self, opts):
        self.prog_name='multicrab'
        # Configuration file
        self.cfg_fname = None
        # Continuation flag
        self.flag_continue = 0
        self.continue_dir = None
        self.processContinueOption_(opts)

        self.processIniFile_(opts)
        # configuration
        self.opts=opts

        if not self.flag_continue:
            self.createWorkSpace()

        print self.prog_name + ' running on ' +  time.ctime(time.time())
        print '  working directory   ' + self.continue_dir

        crabs=[]

    def processContinueOption_(self,opts):

        # Look for the '-continue' option.

        for opt in opts.keys():
            if ( opt in ('-continue','-c') ):
                self.flag_continue = 1
                val = opts[opt]
                if val:
                    if val[0] == '/': self.continue_dir = val     # abs path
                    else: self.continue_dir = os.getcwd() + '/' + val      # rel path
                    pass
                break
            pass

        # Look for actions which has sense only with '-continue'

        if "-create" not in opts.keys() :
            self.flag_continue = 1

        if not self.flag_continue: return

        if not self.continue_dir:
            prefix = self.prog_name + '_'
            self.continue_dir = findLastWorkDir(prefix)
            pass

        if not self.continue_dir:
            raise CrabException('Cannot find last working directory.')

        if not os.path.exists(self.continue_dir):
            msg = 'Cannot continue because the working directory <'
            msg += self.continue_dir
            msg += '> does not exist.'
            raise CrabException(msg)

        return

    def createWorkSpace(self):
        # create WorkingDir for Multicrab
        if 'MULTICRAB.working_dir' in self.opts.keys():    
            self.continue_dir = os.path.abspath(self.opts['MULTICRAB.working_dir'])
        else:
            current_time = time.strftime('%y%m%d_%H%M%S', time.localtime(time.time()))
            self.continue_dir = os.getcwd() + '/' + self.prog_name + '_' + current_time

        if self.continue_dir and not os.path.exists(self.continue_dir):
            try:
                os.mkdir(self.continue_dir)
            except OSError:
                msg = 'Cannot create '+str(self.continue_dir) +' directory.\n'
                raise CrabException(msg)
            pass
        else:
            msg = 'Directory '+str(self.continue_dir) +' already exist.\n'
            raise CrabException(msg)

        shutil.copyfile('multicrab.cfg',self.continue_dir+'/multicrab.cfg')
        
        return
        
    def processIniFile_(self, opts):
        """
        Processes a configuration INI-file.
        """

        # Extract cfg-file name from the cmd-line options.

        for opt in opts.keys():
            if ( opt == '-cfg' ):
                if self.flag_continue:
                    raise CrabException('-continue and -cfg cannot coexist.')
                if opts[opt] : self.cfg_fname = opts[opt]
                else : processHelpOptions()
                pass
            pass

        # Set default cfg-fname

        if self.cfg_fname == None:
            if self.flag_continue:
                self.cfg_fname = self.continue_dir + '/multicrab.cfg'
            else:
                self.cfg_fname = 'multicrab.cfg'
                pass
            pass

        # Load cfg-file

        if self.cfg_fname != None:
            if os.path.exists(self.cfg_fname):
                self.cfg_params = self.loadMultiConfig(self.cfg_fname)
                pass
            else:
                msg = 'cfg-file '+self.cfg_fname+' not found.'
                raise CrabException(msg)
                pass
            pass

        # process the [CRAB] section

        lhp = len('MULTICRAB.')
        for k in self.cfg_params.keys():
            if len(k) >= lhp and k[:lhp] == 'MULTICRAB.':
                opt = '-'+k[lhp:]
                if len(opt) >= 3 and opt[:3] == '-__': continue
                if opt not in opts.keys():
                    opts[opt] = self.cfg_params[k]
                    pass
                pass
            pass

        self.cfg_params_dataset = {}
        common_opts = []
        # first get common sections
        for sec in self.cfg_params:
            if sec in ['MULTICRAB']:
                cfg_common=self.cfg_params[sec]
                continue
            if sec in ['COMMON']:
                common_opts.append(self.cfg_params[sec])
                continue
            pass

        # then Dataset's specific
        for sec in self.cfg_params:
            if sec in ['MULTICRAB', 'COMMON']: continue
            self.cfg_params_dataset[sec]=self.cfg_params[sec]
            # add common to all dataset
            for opt in common_opts:
                self.cfg_params_dataset[sec]=opt
            pass

        self.cfg=cfg_common['cfg']

        return

    def loadMultiConfig(self, file):
        """
        returns a dictionary with keys of the form
        <section>.<option> and the corresponding values
        """
        config={}
        cp = ConfigParser.ConfigParser()
        cp.read(file)
        for sec in cp.sections():
            # print 'Section',sec
            config[sec]={}
            for opt in cp.options(sec):
                #print 'config['+sec+'.'+opt+'] = '+string.strip(cp.get(sec,opt))
                config[sec][opt] = string.strip(cp.get(sec,opt))
        return config

    def run(self):
        #run crabs
        for sec in self.cfg_params_dataset:
            options={}
            if self.flag_continue:
                options['-c']=sec
            # DatasetName to be used
            options['-USER.ui_working_dir']=sec
            # options from multicrab.cfg
            for opt in self.cfg_params_dataset[sec]:
                tmp="-"+string.upper(opt.split(".")[0])+"."+opt.split(".")[1]
                options[tmp]=self.cfg_params_dataset[sec][opt]
            # Input options (command)
            for opt in self.opts:
                options[opt]=self.opts[opt]
            try:
                # print options
                crab = Crab(options)
                crab.run()
                common.apmon.free()
            except CrabException, e:
                print '\n' + common.prog_name + ': ' + str(e) + '\n'
                if common.logger:
                    common.logger.write('ERROR: '+str(e)+'\n')
                    pass
                pass
            pass
        pass
        
        #common.apmon.free()

###########################################################################
if __name__ == '__main__':
    ## Get rid of some useless warning
    try:
        import warnings
        warnings.simplefilter("ignore", RuntimeWarning)
    except ImportError:
        pass # too bad, you'll get the warning

    # Parse command-line options and create a dictionary with
    # key-value pairs.
    options = parseOptions(sys.argv[1:])

    # Process "help" options, such as '-help', '-version'
    if processHelpOptions(options) : sys.exit(0)

    # Create, initialize, and run a Crab object
    try:
        multicrab = MultiCrab(options)
        multicrab.run()
    except CrabException, e:
        print '\n' + common.prog_name + ': ' + str(e) + '\n'

    pass
