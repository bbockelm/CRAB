#!/usr/bin/env python
import sys, os, time, string

vers = sys.version_info[0] + .1 * sys.version_info[1]
if vers < 2.4:
    print \
"""
You are currently running python version:
%s\n
from the executable:
%s\n
This is not the correct python version.  CRAB requires at least python 2.4.
Please see
    https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrabHowTo
for more information about setting up your environment.
""" % (sys.version, sys.executable)
    sys.exit()

## actual import session
from crab_util import *
from crab_exceptions import *
import logging
from WorkSpace import WorkSpace
from DBinterface import DBinterface ## added to interface with DB BL--DS
from JobList import JobList
from ApmonIf import ApmonIf
import common

###########################################################################
class Crab:
    def __init__(self, opts):
        ## test_tag
        # The order of main_actions is important !
        self.main_actions = [ '-create', '-submit' ]
        self.aux_actions = [ '-list', '-kill', '-status', '-getoutput','-get',
                             '-resubmit' , '-testJdl',
                             '-listMatch', '-match', '-postMortem', '-clean',
                             '-printId', '-createJdl','-printJdl', '-publish', '-checkPublication',
                             '-copyData', '-renewCredential', '-extend','-validateCfg',
                             '-report' ]

        # Dictionary of actions, e.g. '-create' -> object of class Creator
        self.actions = {}

        # Configuration file
        self.cfg_fname = None
        self.cfg_sys_fname = None
        self.cfg_home_fname = None
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
        common.debugLevel = 0

        self.initialize_(opts)

        return

    def version():
        return common.prog_version_str

    version = staticmethod(version)

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

        srvName = 'default'
        self.UseServer = False
        if opts.has_key('-use_server'):
            self.UseServer = opts['-use_server']
            if self.UseServer:
                opts['-server_name']='default'
        if opts.has_key('-server_name'):
            srvName = opts.get('-server_name', None)
            if srvName == 'None':
                srvName = None
            self.UseServer = int( srvName is not None ) # cast bool to int

        common._db = DBinterface(self.cfg_params)

        isCreating = False
        if not self.flag_continue:
            self.createWorkingSpace_()
            common._db.configureDB()
            optsToBeSaved={}
            optsToBeSavedDB={}
            isCreating = True
            for it in opts.keys():
                if (it in self.main_actions) or (it in self.aux_actions) or (it == '-debug'):
                    pass
                else:
                    optsToBeSavedDB[it[1:]]=opts[it]
                    optsToBeSaved[it]=opts[it]
                    if self.UseServer==0: optsToBeSavedDB['server_name']= srvName
            parsToBeSaved={}
            for it in self.cfg_params.keys():
                sec,act = string.split(it,".")
                if sec == string.upper(common.prog_name):
                    tmp = "-"+act
                    if (tmp in self.main_actions) or (tmp in self.aux_actions) or (it == '-debug'):
                        pass
                    else:
                        parsToBeSaved[it]=self.cfg_params[it]
                else:
                    parsToBeSaved[it]=self.cfg_params[it]
            common.work_space.saveConfiguration(optsToBeSaved, parsToBeSaved)
        else:
            common._db.loadDB()

        # At this point all configuration options have been read.
        args = string.join(sys.argv,' ')

        self.updateHistory_(args)
        self.createLogger_(args)

        common.apmon = ApmonIf()

        self.createScheduler_()
        if not self.flag_continue:
            common._db.createTask_(optsToBeSavedDB)
        nsg=''
        msg = ('Used properties:\n')
        if isCreating and common.debugLevel < 2 :
            msg = ('Used properties:\n')
            msg += self.UserCfgProperties(msg)
            msg += 'End of used properties.\n'
            common.logger.debug( msg)
        else:
            msg += self.UserCfgProperties(msg)
            msg += 'End of used properties.\n'
            common.logger.log(10-1, msg)

        self.initializeActions_(opts)
        return

    def UserCfgProperties(self,msg):
        """
        print user configuration parameters
        """
        keys = self.cfg_params.keys()
        keys.sort()
        for k in keys:
            if self.cfg_params[k]:
                msg += '   %s : %s\n'%(str(k),str(self.cfg_params[k]))
            else:
                msg += '   %s : \n'%str(k)
        return msg

    def processContinueOption_(self,opts):

        continue_dir = None

        # Look for the '-continue' option.

        for opt in opts.keys():
            if ( opt in ('-continue','-c') ):
                self.flag_continue = 1
                val = opts[opt]
                if val:
                    if val[0] == '/': continue_dir = val     # abs path
                    else: continue_dir = self.cwd + val      # rel path
                    pass
                break
            pass

        # Look for actions which has sense only with '-continue'

        if "-create" not in opts.keys() :
            self.flag_continue = 1

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
        common.work_space = WorkSpace(continue_dir, self.cfg_params)

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
                self.cfg_home_fname = None
                if os.environ.has_key('CRABDIR'):
                    self.cfg_sys_fname = os.environ['CRABDIR']+"/etc/"+common.prog_name+'.cfg'
                if os.environ.has_key('HOME'):
                    self.cfg_home_fname = os.environ['HOME']+"/."+common.prog_name+'.cfg'
                pass
            pass

        # Load cfg-file

        if string.lower(self.cfg_fname) != 'none':
            if not self.flag_continue:
                if self.cfg_sys_fname and os.path.exists(self.cfg_sys_fname):
                    self.cfg_params = loadConfig(self.cfg_sys_fname, self.cfg_params)
                if self.cfg_home_fname and os.path.exists(self.cfg_home_fname):
                    self.cfg_params = loadConfig(self.cfg_home_fname, self.cfg_params)
            if os.path.exists(self.cfg_fname):
                self.cfg_params = loadConfig(self.cfg_fname,self.cfg_params)
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

            elif ( opt == '-server_name' ):
                # TODO
                pass

            elif ( opt == '-use_server' ):
                # TODO
                pass

            elif ( opt == '-cfg' ):
                pass

            elif ( opt in ('-continue', '-c') ):
                # Already processed in processContinueOption_()
                pass

            elif ( opt == '-jobtype' ):
                if val : self.job_type_name = string.upper(val)
                else : processHelpOptions()
                pass

            elif ( opt == '-Q' ):
                self.flag_quiet = 1
                pass

            elif ( opt == '-debug' ):
                if val: common.debugLevel = int(val)
                else: common.debugLevel = 1
                pass

            elif ( opt == '-scheduler' ):
                pass

            elif string.find(opt,'.') == -1:
                print common.prog_name+'. Unrecognized option '+opt
                processHelpOptions()
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

    def parseRange_(self, aRange):
        """
        Takes as the input a string with a range defined in any of the following
        way, including combination, and return a tuple with the ints defined
        according to following table. A consistency check is done.
        NB: the first job is "1", not "0".
        'all'       -> [1,2,..., NJobs]
        ''          -> [1,2,..., NJobs]
        'n1'        -> [n1]
        'n1-n2'     -> [n1, n1+1, n1+2, ..., n2-1, n2]
        'n1,n2'     -> [n1, n2]
        'n1,n2-n3'  -> [n1, n2, n2+1, n2+2, ..., n3-1, n3]
        """
        result = []

        common.logger.debug("parseRange_ "+str(aRange))
        if aRange=='all' or aRange==None or aRange=='':
            result=range(1,common._db.nJobs()+1)
            return result
        elif aRange=='0':
            return result

        subRanges = str(aRange).split(',') # DEPRECATED # Fabio #string.split(aRange, ',')
        for subRange in subRanges:
            result = result+self.parseSimpleRange_(subRange)

        if self.checkUniqueness_(result):
            return result
        else:
            common.logger.info( "Error " +str(result) )
            return []

    def checkUniqueness_(self, list):
        """
        check if a list contains only unique elements
        """

        uniqueList = []

        [uniqueList.append(it) for it in list if not uniqueList.count(it)]

        return (len(list)==len(uniqueList))


    def parseSimpleRange_(self, aRange):
        """
        Takes as the input a string with two integers separated by
        the minus sign and returns the tuple with these numbers:
        'n1-n2' -> [n1, n1+1, n1+2, ..., n2-1, n2]
        'n1'    -> [n1]
        """
        (start, end) = (None, None)

        result = []
        minus = str(aRange).find('-') #DEPRECATED #Fabio #string.find(aRange, '-')
        if ( minus < 0 ):
            if isInt(aRange) and int(aRange)>0 :
                result.append(int(aRange))
            else:
                common.logger.info("parseSimpleRange_  ERROR "+aRange)
                processHelpOptions()
                raise CrabException("parseSimpleRange_ ERROR "+aRange)
                pass

            pass
        else:
            (start, end) = str(aRange).split('-')
            if isInt(start) and isInt(end) and int(start)>0 and int(start)<int(end):
                result=range(int(start), int(end)+1)
            else:
                processHelpOptions()
                raise CrabException("parseSimpleRange_ ERROR "+start+end)

        return result

    def initializeActions_(self, opts):
        """
        For each user action instantiate a corresponding
        object and put it in the action dictionary.
        """

        for opt in opts.keys():

            val = opts[opt]


            if (  opt == '-create' ):
                if self.flag_continue:
                    msg =  'Cannot create an existing project. If you want to extend it (to analyze new fileBloks) use: \n'
                    msg += ' crab -extend '
                    raise CrabException(msg)
                if val and val != 'all':
                    msg  = 'Per default, CRAB will create all jobs as specified in the crab.cfg file, not the command line!'
                    common.logger.info(msg)
                    msg  = 'Submission will still take into account the number of jobs specified on the command line!\n'
                    common.logger.info(msg)
                ncjobs = 'all'

                from Creator import Creator
                # Instantiate Creator object
                self.creator = Creator(self.job_type_name,
                                       self.cfg_params,
                                       ncjobs)
                self.actions[opt] = self.creator

                # create jobs in the DB
                common._db.createJobs_(self.creator.nJobsL())

                # Create and initialize JobList
                common.job_list = JobList(common._db.nJobs(),
                                          self.creator.jobType())

                taskinfo={}
                taskinfo['cfgName'] = common.work_space.jobDir()+"/"+self.creator.jobType().configFilename()
                taskinfo['dataset'] = self.cfg_params['CMSSW.datasetpath']

                common.job_list.setScriptNames(self.job_type_name+'.sh')
                common.job_list.setCfgNames(self.creator.jobType().configFilename())
                self.creator.writeJobsSpecsToDB()
                common._db.updateTask_(taskinfo)
                pass

            if (  opt == '-extend' ):

                if val and val != 'all':
                    self.parseRange_(val)
                    msg  = 'Per default, CRAB will extend the task with all jobs as specified in the crab.cfg file, not the command line!'
                    msg  += 'Submission will still take into account the command line\n'
                    common.logger.info(msg)

                skip_blocks = True
                ncjobs = 'all'
                isNew=False
                firstJob=common._db.nJobs()

                from Creator import Creator
                # Instantiate Creator object
                self.creator = Creator(self.job_type_name,
                                       self.cfg_params,
                                       ncjobs, skip_blocks, isNew, firstJob)
                self.actions[opt] = self.creator
                 
                # create jobs in the DB
                common._db.createJobs_(self.creator.nJobsL(),isNew)

                # Create and initialize JobList
                common.job_list = JobList(common._db.nJobs(),
                                          self.creator.jobType())

                self.creator.writeJobsSpecsToDB(firstJob)
                pass


            elif ( opt == '-submit' ):
                ## Dealt with val == int so that -submit N means submit N jobs and not job # N
                if (self.UseServer== 1):
                    from SubmitterServer import SubmitterServer
                    self.actions[opt] = SubmitterServer(self.cfg_params, self.parseRange_(val), val)
                else:
                    from Submitter import Submitter
                    # Instantiate Submitter object
                    self.actions[opt] = Submitter(self.cfg_params, self.parseRange_(val), val)
                    # Create and initialize JobList
                    if len(common.job_list) == 0 :
                        common.job_list = JobList(common._db.nJobs(),
                                                  None)
                        pass
                    pass

            elif ( opt == '-list' ):
                '''
                Print the relevant infos of a range-all jobs/task
                '''
                jobs = self.parseRange_(val)

                common._db.dump(jobs)
                pass

            elif ( opt == '-printId' ):
                '''
                Print the unique name of the task if crab is used as client
                Print the SID list of all the jobs
                '''
                jid=False
                if val == 'full': jid=True
                common._db.queryID(self.UseServer,jid)

            elif ( opt == '-status' ):
                from Status import Status
                if (self.UseServer== 1):
                    from StatusServer import StatusServer
                    self.actions[opt] = StatusServer(self.cfg_params)
                else:
                    jobs = self.parseRange_(val)

                    if len(jobs) != 0:
                        self.actions[opt] = Status(self.cfg_params)
                    pass

            elif ( opt == '-kill' ):

                if val:
                    if val =='all':
                        jobs = common._db.nJobs("list")
                    else:
                        jobs = self.parseRange_(val)
                    pass
                else:
                    raise CrabException("Warning: with '-kill' you _MUST_ specify a job range or 'all'")
                    pass

                if (self.UseServer== 1):
                    from KillerServer import KillerServer
                    self.actions[opt] = KillerServer(self.cfg_params,jobs)
                else:
                    from Killer import Killer
                    self.actions[opt] = Killer(self.cfg_params,jobs)


            elif ( opt == '-getoutput' or opt == '-get'):

                if val=='all' or val==None or val=='':
                    jobs = 'all'
                else:
                    jobs = self.parseRange_(val)

                if (self.UseServer== 1):
                    from GetOutputServer import GetOutputServer
                    self.actions[opt] = GetOutputServer(self.cfg_params,jobs)
                else:
                    from GetOutput import GetOutput
                    self.actions[opt] = GetOutput(self.cfg_params,jobs)

            elif ( opt == '-resubmit' ):
                if val:
                    if val=='all':
                        jobs = common._db.nJobs('list')
                    else:
                        jobs = self.parseRange_(val)

                    if (self.UseServer== 1):
                        from ResubmitterServer import ResubmitterServer
                        self.actions[opt] = ResubmitterServer(self.cfg_params, jobs)
                    else:
                        from Resubmitter import Resubmitter
                        self.actions[opt] = Resubmitter(self.cfg_params, jobs)
                else:
                    common.logger.info("Warning: with '-resubmit' you _MUST_ specify a job range or 'all'")
                    common.logger.info("WARNING: _all_ job specified in the range will be resubmitted!!!")
                    pass
                pass

            elif ( opt in ['-testJdl','-listMatch', '-match']):
                jobs = self.parseRange_(val)

                if len(jobs) != 0:
                    # Instantiate Checker object
                    from Checker import Checker
                    self.actions[opt] = Checker(self.cfg_params, jobs)

            elif ( opt == '-postMortem' ):

                if val:
                    if val =='all':
                        jobs = common._db.nJobs("list")
                    else:
                        jobs = self.parseRange_(val)
                    pass
                else:
                    raise CrabException("Warning: please specify a job range or 'all'")
                    pass

                if (self.UseServer== 1):
                    from PostMortemServer import PostMortemServer
                    self.actions[opt] = PostMortemServer(self.cfg_params,jobs)
                else:
                    from PostMortem import PostMortem
                    self.actions[opt] = PostMortem(self.cfg_params, jobs)

            elif ( opt == '-clean' ):
                if val != None:
                    raise CrabException("No range allowed for '-clean'")
                if (self.UseServer== 1):
                    from CleanerServer import CleanerServer
                    self.actions[opt] = CleanerServer(self.cfg_params)
                else:
                    from Cleaner import Cleaner
                    self.actions[opt] = Cleaner(self.cfg_params)

            elif ( opt in ['-printJdl','-createJdl']):
                """
                Materialize JDL
                """
                ## Temporary:
                if opt == '-printJdl':
                    common.logger.info("WARNING: -printJdl option is deprecated : please use -createJdl \n")
                if val =='all' or val == None or val == '':
                    jobs = common._db.nJobs("list")
                else:
                    jobs = self.parseRange_(val)
                pass
                from JdlWriter import JdlWriter
                self.actions[opt] = JdlWriter(self.cfg_params, jobs)

            elif ( opt == '-publish' ):
                from Publisher import Publisher
                self.actions[opt] = Publisher(self.cfg_params)
            
            elif ( opt == '-checkPublication' ):
                from InspectDBS import InspectDBS
                self.actions[opt] = InspectDBS(self.cfg_params)

            elif ( opt == '-copyData' ):
                if val =='all' or val == None or val == '':
                    jobs = common._db.nJobs("list")
                else:
                    jobs = self.parseRange_(val)
                
                if (self.UseServer== 1):
                    from StatusServer import StatusServer
                    status = StatusServer(self.cfg_params)
                else:
                 #   from Status import Status
                 #   status = Status(self.cfg_params)
                    status=None  
                from CopyData import CopyData
                self.actions[opt] = CopyData(self.cfg_params, jobs,status)
  
            elif ( opt == '-validateCfg' ):
                from ValidateCfg import ValidateCfg     
                config= {'pset' : self.cfg_params.get('CMSSW.pset','None')}
                if val :
                    config['pset']=val
                self.actions[opt] = ValidateCfg(config)

            elif ( opt == '-renewCredential' ):
                if (self.UseServer== 1):
                    from CredentialRenew import CredentialRenew
                    self.actions[opt] = CredentialRenew(self.cfg_params)
                else:
                    msg = "The option [-renewProxy] can be used only with the server modality!"
                    raise CrabException(msg)
            elif ( opt == '-report' ):
                if (self.UseServer== 1):
                    from StatusServer import StatusServer
                    StatusServer(self.cfg_params).query(display=False)
                else:
                    # cause a core dump....
                    #from Status import Status
                    #Status(self.cfg_params).query(display=False)
                    pass
                from Reporter import Reporter     
                self.actions[opt] = Reporter(self.cfg_params)

            pass
        return

    def createWorkingSpace_(self):
        new_dir = ''

        if self.cfg_params.has_key('USER.ui_working_dir') :
            new_dir =os.path.abspath(self.cfg_params['USER.ui_working_dir'])
        else:
            new_dir = self.cwd + common.prog_name + '_' + self.name + '_' + self.current_time
            pass
        if os.path.exists(new_dir):
            if os.listdir(new_dir):
                msg = new_dir + ' already exists and is not empty. Please remove it before create new task'
                raise CrabException(msg)

        common.work_space = WorkSpace(new_dir, self.cfg_params)
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

        logging.DEBUG_VERBOSE = logging.DEBUG - 1
        logging.addLevelName(logging.DEBUG_VERBOSE,'debug_verbose')
        logging.root.setLevel([logging.DEBUG_VERBOSE, logging.DEBUG, logging.INFO, \
                               logging.WARNING,logging.ERROR, logging.CRITICAL])
        logger = logging.getLogger()
        logger = logging.getLogger("crab:")
        logger.setLevel(logging.DEBUG_VERBOSE)
        log_fname =common.work_space.logDir()+common.prog_name+'.log'
        fh=logging.FileHandler(log_fname)
        fh_formatter = logging.Formatter("%(asctime)s [%(levelname)s] \t%(message)s")
        ch=logging.StreamHandler()
        ch_formatter = logging.Formatter("%(name)s  %(message)s")
        fh_level=logging.DEBUG
        ch_level=logging.INFO
        if common.debugLevel > 0:ch_level=logging.DEBUG
        if common.debugLevel > 2: 
            fh_level=logging.DEBUG_VERBOSE
            ch_level=logging.DEBUG_VERBOSE
        fh.setLevel(fh_level)
        ch.setLevel(ch_level)
        fh.setFormatter(fh_formatter)
        ch.setFormatter(ch_formatter)
        logger.addHandler(ch)
        logger.addHandler(fh)
        logger.debug('%s\n'%args)
        logger.info(self.headerString_())
        common.logger = logger
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
        server= 'OFF' 
        if self.UseServer==1: server = 'ON' 
        local = time.ctime(time.time())
        UTC = time.asctime(time.gmtime()).split(' ')[3]
        tzone = time.tzname[0]
        header = 'Version ' + common.prog_version_str + ' running on ' + \
             local+' '+ tzone+ ' ('+UTC+' UTC)'+'\n\n' + \
             common.prog_name+'. Working options:\n'
        header = header +\
                 '\tscheduler           ' + self.cfg_params['CRAB.scheduler'] + '\n'+\
                 '\tjob type            ' + self.job_type_name + '\n'+\
                 '\tserver              ' + server + '\n'+\
                 '\tworking directory   ' + common.work_space.topDir()\
                 + '\n'
        return header

    def createScheduler_(self):
        """
        Creates a scheduler object instantiated by its name.
        """
        if not self.cfg_params.has_key("CRAB.scheduler"):
            msg = 'No real scheduler selected: edg, lsf ...'
            msg = msg + 'Please specify a scheduler type in the crab cfg file'
            raise CrabException(msg)
        self.scheduler_name = self.cfg_params["CRAB.scheduler"]
        ### just temporary... will disappear
        if self.scheduler_name.lower()=='glitecoll': self.scheduler_name='glite'

        klass_name = 'Scheduler' + string.capitalize(self.scheduler_name)
        file_name = klass_name
        try:
            klass = importName(file_name, klass_name)
        except KeyError:
            msg = 'No `class '+klass_name+'` found in file `'+file_name+'.py`'
            raise CrabException(msg)
        except ImportError, e:
            msg = 'Cannot create scheduler Boss'
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
def processHelpOptions(opts={}):
    from crab_help import usage, help
    if len(opts):
        for opt in opts.keys():
            if opt in ('-v', '-version', '--version') :
                print 'CRAB version: ',Crab.version()
                return 1
            if opt in ('-h','-help','--help') :
                if opts[opt] : help(opts[opt])
                else:          help()
                return 1
            if opt == '-debug' :
                if opts[opt] == None: opts[opt] = str(1)
    else:
        usage()
    return 0

###########################################################################
if __name__ == '__main__':
    ## Get rid of some useless warning
    try:
        import warnings
        warnings.simplefilter("ignore", RuntimeWarning)
        # import socket
        # socket.setdefaulttimeout(15) # Default timeout in seconds
    except ImportError:
        pass # too bad, you'll get the warning

    # Parse command-line options and create a dictionary with
    # key-value pairs.
    options = parseOptions(sys.argv[1:])

    # Process "help" options, such as '-help', '-version'
    if processHelpOptions(options) : sys.exit(0)

    # Create, initialize, and run a Crab object
    try:
        crab = Crab(options)
        crab.run()
        common.apmon.free()
        print 'Log file is %s%s.log'%(common.work_space.logDir(),common.prog_name)  
    except CrabException, e:
        print '\n' + common.prog_name + ': ' + str(e) + '\n'
        if common.logger:
            common.logger.debug('ERROR: '+str(e)+'\n')
            print 'Log file is %s%s.log'%(common.work_space.logDir(),common.prog_name)  
            pass
        pass
        sys.exit(1)

    sys.exit(0)
    pass
