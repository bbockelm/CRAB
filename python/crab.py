#!/usr/bin/env python
from crab_help import *
from crab_util import *
from crab_exceptions import *
from crab_logger import Logger
from WorkSpace import WorkSpace
from JobDB import JobDB
from JobList import JobList
from Creator import Creator
from Submitter import Submitter
import common

import sys, os, time, string

###########################################################################
class Crab:
    def __init__(self, opts):

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

        # Session name (?) Do we need this ?
        self.name = '0'

        # Job type
        self.job_type_name = None

        # Continuation flag
        self.flag_continue = 0

        # quiet mode, i.e. no output on screen
        self.flag_quiet = 0
        # produce more output
        self.debug_level = 0

        # Scheduler name, e.g. 'edg', 'lsf'
        self.scheduler_name = 'edg'

        self.initialize_(opts)

        return

    def version(self):
        return common.prog_version_str

    def initialize_(self, opts):

        # Process the '-continue' option first because
        # in the case of continuation the CRAB configuration
        # parameters are loaded from already existing Working Space.
        self.processContinueOption_(opts)

        # Process ini-file first, then command line options
        # because they override ini-file settings.

        self.processIniFile_(opts)

        if self.flag_continue: opts = self.loadConfiguration_(opts)
        
        self.processOptions_(opts)

        if not self.flag_continue:
            self.createWorkingSpace_()
            common.work_space.saveConfiguration(opts, self.cfg_fname)
            pass

        # At this point all configuration options have been read.
        
        args = string.join(sys.argv,' ')
        self.updateHistory_(args)
        self.createLogger_(args)
        common.jobDB = JobDB()
        if self.flag_continue:
            common.jobDB.load()
            common.logger.debug(6, str(common.jobDB))
            pass
        self.createScheduler_()
        if common.logger.debugLevel() >= 6:
            common.logger.debug(6, 'Used properties:')
            keys = self.cfg_params.keys()
            keys.sort()
            for k in keys:
                if self.cfg_params[k]:
                    common.logger.debug(6, '   '+k+' : '+self.cfg_params[k])
                    pass
                else:
                    common.logger.debug(6, '   '+k+' : ')
                    pass
                pass
            common.logger.debug(6, 'End of used properties.\n')
            pass
        self.initializeActions_(opts)
        return

    def processContinueOption_(self,opts):

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

    def processOptions_(self, opts):
        """
        Processes the command-line options.
        """

        for opt in opts.keys():
            val = opts[opt]

            # Skip actions, they are processed later in initializeActions_()
            if opt in self.main_actions:
                self.cfg_params['CRAB.'+opt[1:]] = val
                continue
            if opt in self.aux_actions:
                self.cfg_params['CRAB.'+opt[1:]] = val
                continue
            

            elif ( opt == '-cfg' ):
                pass

            elif ( opt in ('-continue', '-c') ):
                pass

            elif ( opt == '-jobtype' ):
                if val : self.job_type_name = string.upper(val)
                else : usage()
                pass

            elif ( opt == '-Q' ):
                self.flag_quiet = 1
                pass

            elif ( opt == '-debug' ):
                self.debug_level = int(val)
                pass

            elif ( opt == '-scheduler' ):
                if val: self.scheduler_name = val
                else:
                    print common.prog_name+". No value for '-scheduler'."
                    usage()
                    pass
                pass

            elif string.find(opt,'.') == -1:
                print common.prog_name+'. Unrecognized option '+opt
                usage()
                pass

            # Override config parameters from INI-file with cmd-line params
            if string.find(opt,'.') == -1 :
                self.cfg_params['CRAB.'+opt[1:]] = val
                pass
            else:
                # Command line parameters in the form -SECTION.ENTRY=VALUE
                self.cfg_params[opt[1:]] = val
                pass
            pass
        return

    def initializeActions_(self, opts):
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
                    # Instantiate Creator object
                    creator = Creator(self.job_type_name,
                                      self.cfg_params,
                                      ncjobs)
                    self.actions[opt] = creator

                    # Initialize the JobDB object if needed
                    if not self.flag_continue:
                        common.jobDB.create(creator.nJobs())
                        pass

                    # Create and initialize JobList

                    common.job_list = JobList(common.jobDB.nJobs(),
                                              creator.jobType())

                    common.job_list.setScriptNames(self.job_type_name+'.sh')
                    common.job_list.setJDLNames(self.job_type_name+'.jdl')
                    creator.jobType().setSteeringCardsNames()
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
                    # Instantiate Submitter object
                    self.actions[opt] = Submitter(self.cfg_params, nsjobs)

                    # Create and initialize JobList

                    if len(common.job_list) == 0 :
                        common.job_list = JobList(common.jobDB.nJobs(),
                                                  None)
                        common.job_list.setJDLNames(self.job_type_name+'.jdl')
                        pass
                    pass
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

    def createWorkingSpace_(self):
        new_dir = common.prog_name + '_' + self.name + '_' + self.current_time
        new_dir = self.cwd + new_dir
        common.work_space = WorkSpace(new_dir)
        common.work_space.create()
        return

    def loadConfiguration_(self, opts):

        save_opts = common.work_space.loadSavedOptions()

        # Override saved options with new command-line options

        for k in opts.keys():
            save_opts[k] = opts[k]
            pass

        # Return updated options
        return save_opts

    def createLogger_(self, args):

        log = Logger()
        log.quiet(self.flag_quiet)
        log.setDebugLevel(self.debug_level)
        log.write(args+'\n')
        log.message(self.headerString_())
        log.flush()
        common.logger = log
        return

    def updateHistory_(self, args):
        history_fname = common.prog_name+'.history'
        history_file = open(history_fname, 'a')
        history_file.write(self.current_time+': '+args+'\n')
        history_file.close()
        return

    def headerString_(self):
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
    
    def createScheduler_(self):
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
        crab = Crab(options)
        crab.run()
    except CrabException, e:
        print '\n' + common.prog_name + ': ' + str(e) + '\n'
        if common.logger:
            common.logger.write('ERROR: '+str(e)+'\n')
            pass
        pass

    pass
