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
                if opts[opt] :
                    self.cfg_fname = opts[opt]
                    del opts[opt] # do not pass cfg further on
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

        cfg_params = {}
        if self.cfg_fname != None:
            if os.path.exists(self.cfg_fname):
                cfg_params = self.loadMultiConfig(self.cfg_fname)
                pass
            else:
                msg = 'cfg-file '+self.cfg_fname+' not found.'
                raise CrabException(msg)
                pass
            pass

        # process the [CRAB] section

        lhp = len('MULTICRAB.')
        for k in cfg_params.keys():
            if len(k) >= lhp and k[:lhp] == 'MULTICRAB.':
                opt = '-'+k[lhp:]
                if len(opt) >= 3 and opt[:3] == '-__': continue
                if opt not in opts.keys():
                    opts[opt] = cfg_params[k]
                    pass
                pass
            pass

        self.cfg_params_dataset = {}
        common_opts = {}
        # first get common sections
        for sec in cfg_params:
            if sec in ['MULTICRAB']:
                if 'cfg' in cfg_params[sec]:
                    common_opts['cfg']=cfg_params[sec]['cfg']
                continue
            if sec in ['COMMON']:
                common_opts.update(cfg_params[sec])
                continue
            pass

        crab_cfg='crab.cfg'
        if common_opts.has_key('cfg') : crab_cfg=common_opts['cfg']

        # then Dataset's specific
        for sec in cfg_params:
            if sec in ['MULTICRAB', 'COMMON']: continue
            self.cfg_params_dataset[sec]=cfg_params[sec]
            # add common to all dataset
            for key in common_opts:
                self.cfg_params_dataset[sec][key]=common_opts[key]
            pass

        # read crab.cfg file and search for storage_path
        crab_cfg_params = loadConfig(crab_cfg,{})
        # also USER.ui_working_dir USER.outputdir and USER.logdir need special treatment
        if cfg_params.has_key("COMMON"):
            self.user_remote_dir = cfg_params["COMMON"].get("user.user_remote_dir", crab_cfg_params.get("USER.user_remote_dir",None))
            self.outputdir = cfg_params["COMMON"].get("user.outputdir", crab_cfg_params.get("USER.outputdir",None))
            self.logdir = cfg_params["COMMON"].get("user.logdir", crab_cfg_params.get("USER.logdir",None))
            self.ui_working_dir = cfg_params["COMMON"].get("user.ui_working_dir", crab_cfg_params.get("USER.ui_working_dir",None))
        else:
            self.user_remote_dir = crab_cfg_params.get("USER.user_remote_dir",None)
            self.outputdir = crab_cfg_params.get("USER.outputdir",None)
            self.logdir = crab_cfg_params.get("USER.logdir",None)
            self.ui_working_dir = crab_cfg_params.get("USER.ui_working_dir",None)

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
        runFileName = 'multicrab.exe'
        runFile = open(runFileName,"w")
        for sec in self.cfg_params_dataset:
            options={}
            if self.flag_continue:
                options['-c']=sec
            # DatasetName to be used
            options['-USER.ui_working_dir']=sec
            # options from multicrab.cfg
            for opt in self.cfg_params_dataset[sec]:
                tmp = "-"+str(opt)
                if len(opt.split("."))==2:
                    tmp="-"+string.upper(opt.split(".")[0])+"."+opt.split(".")[1]
                
                options[tmp]=self.cfg_params_dataset[sec][opt]

            # add section to storage_path if exist in crab.cfg
            if not self.cfg_params_dataset.has_key("USER.user_remote_dir") and self.user_remote_dir:
                options["-USER.user_remote_dir"]=self.user_remote_dir+"/"+sec
            # also for ui_working_dir
            if not self.cfg_params_dataset.has_key("USER.ui_working_dir") and self.ui_working_dir:
                options["-USER.ui_working_dir"]=self.ui_working_dir+"/"+sec
            # also for logDir
            if not self.cfg_params_dataset.has_key("USER.logdir") and self.logdir:
                options["-USER.logdir"]=self.logdir+"/"+sec
            # also for outputdir
            if not self.cfg_params_dataset.has_key("USER.outputdir") and self.outputdir:
                options["-USER.outputdir"]=self.outputdir+"/"+sec

            # Input options (command)
            for opt in self.opts:
                options[opt]=self.opts[opt]
                if self.flag_continue and options.has_key("-cfg"):
                    del options['-cfg']
                pass

            # write crab command to be executed later...
            cmd='crab '
            for o in options:
                if options[o]==None: options[o]=""
                cmd+=str(o)+" "+str(options[o])+" "
            cmd+="\n"
            # print cmd

            runFile.write(cmd)

            # SL this does not work for complex, multi include pset.py 

            # crab = Crab()
            # try:
            #     crab.initialize_(options)
            #     crab.run()
            #     del crab
            #     print 'Log file is %s%s.log'%(common.work_space.logDir(),common.prog_name)  
            #     print '\n##############################  E N D  ####################################\n'
            # except CrabException, e:
            #     del crab
            #     print '\n' + common.prog_name + ': ' + str(e) + '\n'
            #     pass
            # pass
            # if (common.logger): common.logger.delete()
        pass
        

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
