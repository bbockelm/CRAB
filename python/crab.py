#!/usr/bin/env python
from crab_help import *
from crab_util import *
from crab_exceptions import *
from crab_logger import Logger
from WorkSpace import WorkSpace
from JobDB import JobDB
from Creator import Creator
from Submitter import Submitter
import common

import sys, os, time, string

###########################################################################
class Crab:
    def __init__(self):

        # The order of main_actions is important !
        self.main_actions = [ '-create', '-submit', '-resubmit',
                              '-monitor', '-retrieve' ]
        self.aux_actions = [ '-kill', '-status' ]

        # Dictionary of actions, e.g. '-create' -> object of class Creator
        self.actions = {}

        # Configuration file
        self.cfg_fname = None
        # Dictionary with configuration parameters
        self.cfg_params = {}

        # Current working directory
        self.cwd = os.getcwd()+'/'
        # Current time in format 'yymmdd_hhmmss'
        self.current_time = time.strftime('%y%m%d_%H%M%S',
                                          time.localtime(time.time()))

        # Session name
        self.name = '0'

        # Job type
        self.job_type_name = None

        # Continuation flag
        self.flag_continue = 0

        # quiet mode, i.e. no output on screen
        self.flag_quiet = 0
        # produce more output
        self.debug_level = 0

        # Scheduler, e.g. 'edg', 'lsf'
        self.scheduler_name = 'edg'

        #TODO: for future
        # Flag: if true then run postprocess script to make summary
        self.flag_mksmry = 0
        # postprocess script path given with '-make_summary'
        self.postprocess ='postprocess'

        # Flag: notify users or not when the job finishes
        self.flag_notify = 0    
        # Comma separated e-mail addresses for notification
        self.email = ''
        #end of TODO

        return

    def version(self):
        return common.prog_version_str

    def initialize(self, opts):

        # Process the '-continue' option first because
        # in the case of continuation the CRAB configuration
        # parameters are loaded from already existing Working Space.
        self.processContinueOption(opts)

        # Process ini-file first, then command line options
        # because they override ini-file settings.

        self.processIniFile(opts)

        if self.flag_continue: opts = self.loadConfiguration(opts)
        
        self.processOptions(opts)

        if not self.flag_continue:
            self.createWorkingSpace()
            common.work_space.saveConfiguration(opts, self.cfg_fname)
            pass

        # At this point all configuration options have been read.
        
        args = string.join(sys.argv,' ')
        self.updateHistory(args)
        self.createLogger(args)
        common.jobDB = JobDB()
        self.createScheduler()
        self.initializeActions(opts)
        return

    def processContinueOption(self,opts):

        continue_dir = None
        
        for opt in opts.keys():
            if ( opt in ('-continue','-c') ):
                self.flag_continue = 1
                val = opts[opt]
                if val:
                    if val[0] == '/': continue_dir = val     # abs path
                    else: continue_dir = self.cwd + val      # rel path
                    pass
                pass
            pass

        if not self.flag_continue: return

        
        if not continue_dir:
            prefix = common.prog_name + '_' + self.name + '_'
            continue_dir = findLastWorkDir(prefix)
            pass

        if not continue_dir:
            raise CrabException('Cannot find last working directory.')

        if not os.path.exists(continue_dir):
            msg = 'Cannot continue because the working directory <'
            msg += continue_dir
            msg += '> does not exist.'
            raise CrabException(msg)

        # Instantiate WorkSpace
        common.work_space = WorkSpace(continue_dir)

        return

    def processIniFile(self, opts):
        """
        Processes a configuration INI-file.
        """

        # Extract cfg-file name from the cmd-line options.
    
        for opt in opts.keys():
            if ( opt == '-cfg' ):
                if self.flag_continue:
                    raise CrabException('-continue and -cfg cannot coexist.')
                if opts[opt] : self.cfg_fname = opts[opt]
                else : usage()
                pass

            elif ( opt == '-name' ):
                self.name = opts[opt]
                pass

            pass

        # Set default cfg-fname
        
        if self.cfg_fname == None:
            if self.flag_continue:
                self.cfg_fname = common.work_space.cfgFileName()
            else:
                self.cfg_fname = common.prog_name+'.cfg'
                pass
            pass

        # Load cfg-file
        
        if string.lower(self.cfg_fname) != 'none':
            if os.path.exists(self.cfg_fname):
                self.cfg_params = loadConfig(self.cfg_fname)
                pass
            else:
                msg = 'cfg-file '+self.cfg_fname+' not found.'
                raise CrabException(msg)
                pass
            pass

        # process the [CRAB] section

        lhp = len('CRAB.')
        for k in self.cfg_params.keys():
            if len(k) >= lhp and k[:lhp] == 'CRAB.':
                opt = '-'+k[lhp:]
                if len(opt) >= 3 and opt[:3] == '-__': continue
                if opt not in opts.keys():
                    opts[opt] = self.cfg_params[k]
                    pass
                pass
            pass

        return

    def processOptions(self, opts):
        """
        Processes the command-line options.
        """

        for opt in opts.keys():
            val = opts[opt]

            if opt in self.main_actions: continue
            if opt in self.aux_actions: continue
            

            elif ( opt == '-cfg' ):
                pass

            elif ( opt in ('-continue', '-c') ):
                pass

            elif ( opt == '-jobtype' ):
                if val : self.job_type_name = string.upper(val)
                else : usage()
                pass

            elif ( opt == '-make_summary' ):
                if val :
                    if val == 'none' or val == '0':
                        self.flag_mksmry = 0
                        pass
                    else:
                        self.flag_mksmry = 1
                        self.postprocess = val
                        pass
                    pass
                else:
                    self.flag_mksmry = 1
                    pass
                pass

            elif ( opt == '-notify' ):
                if val:
                    self.flag_notify = 1
                    self.email = val
                    pass
                else:
                    usage()
                    pass
                pass

            elif ( opt == '-Q' ):
                self.flag_quiet = 1
                pass

            elif ( opt == '-debug' ):
                self.debug_level = val
                pass

            elif ( opt == '-scheduler' ):
                if val: self.scheduler_name = val
                else:
                    print common.prog_name+". No value for '-scheduler'."
                    usage()
                    pass
                pass

            # Obsolete old code which is kept for reference only.
            # TODO: Must be redone
            elif ( opt in ('-use_monitor=boss', '-useboss') ):
                if val:
                    if ( val == '0' or val == '1' ):
                        common.use_boss = int(val)
                    else:
                        print common.prog_name+'. Bad flag for -use_boss option:',\
                              val,'Possible values are 0 or 1'
                        usage()
                        pass
                    pass
                else: usage()

            elif ( opt in ('-use_jam', '-usejam') ):
                if val:
                    if ( val == '0' or val == '1' ):
                        common.use_jam = int(val)
                    else:
                        print common.prog_name+'. Bad flag for -use_jam option:',\
                              val,'Possible values are 0(=No)  or 1=(Yes)'
                        usage()
                        pass
                    pass
                if ( common.use_jam == 1 ):
                    common.run_jam = common.INI_params['USER.run_jam'] 
                    print "using JAM monitoring"
                    common.output_jam = common.INI_params['USER.output_jam']
                else: usage()
                pass
            # End of the obsolete code

            elif string.find(opt,'.') != -1 and val:
                # Command line parameters in the form -SECTION.ENTRY=VALUE
                self.cfg_params[opt[1:]] = val
                pass
      
            else:
                print common.prog_name+'. Unrecognized option '+opt
                usage()
                pass
      
            pass
        return

    def initializeActions(self, opts):
        """
        For each user action instantiate a corresponding
        object and put it in the action dictionary.
        """
        
        for opt in opts.keys():
            val = opts[opt]

            if ( opt == '-create' ):
                if val:
                    if ( isInt(val) ):
                        ncjobs = int(val)
                    elif ( val == 'all'):
                        ncjobs = val
                    else:
                        print common.prog_name+'. Bad creation bunch size',val
                        print '         Must be an integer or "all"'
                        pass
                    pass
                else: ncjobs = 'all'

                if ncjobs != 0:
                    creator = Creator(self.job_type_name,
                                      self.cfg_params,
                                      ncjobs)
                    self.actions[opt] = creator
                    if not self.flag_continue:
                        common.jobDB.create(creator.nJobs())
                        pass
                    pass

            elif ( opt == '-submit' ):
                if val:
                    if ( isInt(val) ):
                        nsjobs = int(val)
                    elif ( val == 'all'):
                        nsjobs = val
                    else:
                        print common.prog_name+'. Bad submission bunch size',val
                        print '         Must be an integer or "all"'
                        pass
                    pass
                else: nsjobs = 'all'

                if nsjobs != 0:
                    self.actions[opt] = Submitter(self.cfg_params, nsjobs)
                    pass

            elif ( opt == '-resubmit' ):
                # TODO
                common.flag_resubmit = 1
                if val:
                    (common.resubmit_from,common.resubmit_to) =parseJobidRange(val)
                    if ( common.resubmit_to == None or
                         common.resubmit_from > common.resubmit_to ):
                        print common.prog_name+'. Bad BOSS JobId range ['+val+']'
                        usage()
                        pass
                    pass
                else: usage()

            elif ( opt == '-status' ):
                # TODO
                pass
            
            elif ( opt == '-monitor' ):
                # TODO
                if val and ( isInt(val) ):
                    common.delay = val
                else:
                    common.delay = 60
                    pass
                common.autoretrieve = 1
                pass

            elif ( opt == '-retrieve' ):
                # TODO
                if val and ( isInt(val) ):
                    common.delay = val
                else:
                    common.delay = 60
                    pass
                common.autoretrieve = 1
                pass
            
            
            elif ( opt == '-kill' ):
                # TODO
                jobMon = retrieve.Monitor()
                jobMon.killJobs()
                sys.exit()

            pass
        return

    def createWorkingSpace(self):
        new_dir = common.prog_name + '_' + self.name + '_' + self.current_time
        new_dir = self.cwd + new_dir
        common.work_space = WorkSpace(new_dir)
        common.work_space.create()
        return

    def loadConfiguration(self, opts):

        save_opts = common.work_space.loadSavedOptions()

        # Override saved options with new command-line options

        for k in opts.keys():
            save_opts[k] = opts[k]
            pass

        # Return updated options
        return save_opts

    def createLogger(self, args):

        log = Logger()
        log.quiet(self.flag_quiet)
        log.setDebugLevel(self.debug_level)
        log.write(args+'\n')
        log.message(self.headerString())
        log.flush()
        common.logger = log
        return

    def updateHistory(self, args):
        history_fname = common.prog_name+'.history'
        history_file = open(history_fname, 'a')
        history_file.write(self.current_time+': '+args+'\n')
        history_file.close()
        return

    def headerString(self):
        """
        Creates a string describing program options either given in
        the command line or their default values.
        """
        header = common.prog_name + ' (version ' + common.prog_version_str + \
             ') running on ' + \
             time.ctime(time.time())+'\n\n' + \
             common.prog_name+'. Working options:\n'
        header = header +\
                 '  scheduler           ' + self.scheduler_name + '\n'+\
                 '  job type            ' + self.job_type_name + '\n'+\
                 '  working directory   ' + common.work_space.topDir()\
                 + '\n'
        return header
    
    def createScheduler(self):
        """
        Creates a scheduler object instantiated by its name.
        """
        klass_name = 'Scheduler' + string.capitalize(self.scheduler_name)
        file_name = klass_name
        try:
            klass = importName(file_name, klass_name)
        except KeyError:
            msg = 'No `class '+klass_name+'` found in file `'+file_name+'.py`'
            raise CrabException(msg)
        except ImportError, e:
            msg = 'Cannot create scheduler '+self.scheduler_name
            msg += ' (file: '+file_name+', class '+klass_name+'):\n'
            msg += str(e)
            raise CrabException(msg)

        common.scheduler = klass()
        common.scheduler.configure(self.cfg_params)
        return

    def run(self):
        """
        For each 
        """

        for act in self.main_actions:
            if act in self.actions.keys(): self.actions[act].run()
            pass

        for act in self.aux_actions:
            if act in self.actions.keys(): self.actions[act].run()
            pass
        return

###########################################################################
def processHelpOptions(opts):

    for opt in opts.keys():
        if opt == '-v':
            print Crab().version()
            return 1
        if opt in ('-h','-help','--help') :
            if opts[opt] : help(opts[opt])
            else:          help()
            return 1

    return 0

###########################################################################
if __name__ == '__main__':

    # Parse command-line options and create a dictionary with
    # key-value pairs.

    options = parseOptions(sys.argv[1:])

    # Process "help" options, such as '-help', '-version'

    if processHelpOptions(options): sys.exit(0)

    # Create, initialize, and run a Crab object

    try:
        crab = Crab()
        crab.initialize(options)
        crab.run()
    except CrabException, e:
        print '\n' + common.prog_name + ': ' + str(e) + '\n'
        if common.logger:
            common.logger.write('ERROR: '+str(e)+'\n')
            pass
        pass

    pass
