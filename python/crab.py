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
from Checker import Checker
from PostMortem import PostMortem
from Status import Status
from StatusBoss import StatusBoss 
from ApmonIf import ApmonIf
from Cleaner import Cleaner
import common
import Statistic

import sys, os, time, string

###########################################################################
class Crab:
    def __init__(self, opts):

        # The order of main_actions is important !
        self.main_actions = [ '-create', '-submit' ] 
        self.aux_actions = [ '-list', '-kill', '-status', '-getoutput',
                             '-resubmit' , '-cancelAndResubmit', '-testJdl', '-postMortem', '-clean']

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
        # self.scheduler_name = ''

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

        if not self.flag_continue:
            self.createWorkingSpace_()
            optsToBeSaved={}
            for it in opts.keys():
                if (it in self.main_actions) or (it in self.aux_actions) or (it == '-debug'):
                    pass
                else:
                    optsToBeSaved[it]=opts[it]
            common.work_space.saveConfiguration(optsToBeSaved, self.cfg_fname)
            pass

        # At this point all configuration options have been read.
        
        args = string.join(sys.argv,' ')

        self.updateHistory_(args)

        self.createLogger_(args)

        common.jobDB = JobDB()
        
        if int(self.cfg_params['USER.activate_monalisa']): self.cfg_params['apmon'] = ApmonIf()
        
        if self.flag_continue:
            try:
                common.jobDB.load()
                self.cfg_params['taskId'] = common.jobDB._jobs[0].taskId 
                common.logger.debug(6, str(common.jobDB))
            except DBException,e:
                pass
            pass

        self.createScheduler_()

        if common.logger.debugLevel() >= 6:
            common.logger.debug(6, 'Used properties:')
            keys = self.cfg_params.keys()
            keys.sort()
            for k in keys:
                if self.cfg_params[k]:
                    common.logger.debug(6, '   '+k+' : '+str(self.cfg_params[k]))
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

        if not self.flag_continue:
            for opt in opts.keys():
                if ( opt in (self.aux_actions) ):
                    self.flag_continue = 1
                    break
                pass
            pass
            submit_flag=0
            create_flag=0
            for opt in opts.keys():
                if opt == "-submit": submit_flag=1
                if opt == "-create": create_flag=1
                pass
            if (submit_flag and not create_flag):
                msg = "'-submit' must be used with either '-create' or '-continue'."
                raise CrabException(msg)
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
                self.cfg_params['user'] = os.environ['USER']
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
                # Already processed in processContinueOption_()
                pass

            elif ( opt == '-jobtype' ):
                if val : self.job_type_name = string.upper(val)
                else : usage()
                pass

            elif ( opt == '-Q' ):
                self.flag_quiet = 1
                pass

            elif ( opt == '-debug' ):
                if val: self.debug_level = int(val)
                else: self.debug_level = 1
                pass

            elif ( opt == '-scheduler' ):
                if val:
                    self.scheduler_name = 'boss'
                    self.flag_useboss = 1
                else:
                    print common.prog_name+". No value for '-scheduler'."
                    usage()
                    pass
                pass

#            elif ( opt in ('-use_boss', '-useboss') ):
#                if ( val == '1' ):
#                    self.scheduler_name = 'boss'
#                    pass
#                elif ( val == '0' ): 
#                    pass
#                else:
#                    print common.prog_name+'. Bad flag for -use_boss option:',\
#                          val,'Possible values are 0(=No) or 1(=Yes)'
#                    usage()
#                    pass
#                pass

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
 
        common.logger.debug(5,"parseRange_ "+str(aRange))
        if aRange=='all' or aRange==None or aRange=='':
            result=range(0,common.jobDB.nJobs())
            return result
        elif aRange=='0':
            return result

        subRanges = string.split(aRange, ',')
        for subRange in subRanges:
            result = result+self.parseSimpleRange_(subRange)

        if self.checkUniqueness_(result):
            return result
        else:
            common.logger.message("Error "+result)
            return []

    def checkUniqueness_(self, list):
        """
        check if a list contains only unique elements
        """

        uniqueList = []
        # use a list comprehension statement (takes a while to understand) 

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
        minus = string.find(aRange, '-')
        if ( minus < 0 ):
            if isInt(aRange) and int(aRange)>0:
                # FEDE
                #result.append(int(aRange)-1)
                ###
                result.append(int(aRange))
            else:
                common.logger.message("parseSimpleRange_  ERROR "+aRange)
                usage()
                pass
  
            pass
        else:
            (start, end) = string.split(aRange, '-')
            if isInt(start) and isInt(end) and int(start)>0 and int(start)<int(end):
                #result=range(int(start)-1, int(end))
                result=range(int(start), int(end)+1) #Daniele  
            else:
                common.logger.message("parseSimpleRange_ ERROR "+start+end)

        return result

    def initializeActions_(self, opts):
        """
        For each user action instantiate a corresponding
        object and put it in the action dictionary.
        """
#        for opt in opts.keys():
#            self.flag_useboss = 0
#            if ( opt == '-use_boss'):  
#                val = opts[opt]
#                if ( val == '1' ):
#                    self.flag_useboss = 1
#                    common.logger.message('Using BOSS')
#                    pass
#                else:
#                    self.flag_useboss = 0
#                    pass

        for opt in opts.keys():
          
            val = opts[opt]
                                                                                                               
 
            if (  opt == '-create' ):
                ncjobs = 0
                if val:
                    if ( isInt(val) ):
                        ncjobs = int(val)
                    elif ( val == 'all'):
                        ncjobs = val
                    else:
                        msg = 'Bad creation bunch size <'+str(val)+'>\n'
                        msg += '      Must be an integer or "all"'
                        msg += '      Generic range is not allowed"'
                        raise CrabException(msg)
                    pass
                else: ncjobs = 'all'

                if ncjobs != 0:
                    # Instantiate Creator object
                    self.creator = Creator(self.job_type_name,
                                           self.cfg_params,
                                           ncjobs)
                    self.actions[opt] = self.creator

                    # Initialize the JobDB object if needed
                    if not self.flag_continue:
                        common.jobDB.create(self.creator.nJobs())
                        pass

                    # Create and initialize JobList

                    common.job_list = JobList(common.jobDB.nJobs(),
                                              self.creator.jobType())

                    common.job_list.setScriptNames(self.job_type_name+'.sh')
                    common.job_list.setJDLNames(self.job_type_name+'.jdl')
                    common.job_list.setCfgNames(self.creator.jobType().configFilename())

                    self.creator.writeJobsSpecsToDB()
                    pass
                pass

            elif ( opt == '-submit' ):

                # total jobs
                # get the first not already submitted
                common.logger.debug(5,'Total jobs '+str(common.jobDB.nJobs()))
                lastSubmittedJob=0
                for nj in range(common.jobDB.nJobs()):
                    if (common.jobDB.status(nj) in ['S','K','RC','Y','A','D']):
                        lastSubmittedJob +=1
                    else: break
                # count job from 1
                totalJobsSubmittable = common.jobDB.nJobs()-lastSubmittedJob
                common.logger.debug(5,'lastSubmittedJob '+str(lastSubmittedJob))
                common.logger.debug(5,'totalJobsSubmittable '+str(totalJobsSubmittable))

                nsjobs = lastSubmittedJob+totalJobsSubmittable
                # get user request
                if val:
                    if ( isInt(val) ):
                        tmp = int(val)
                        if (tmp >= totalJobsSubmittable): 
                            common.logger.message('asking to submit '+str(tmp)+' jobs, but only '+str(totalJobsSubmittable)+' left: submitting those')
                            pass
                        else:
                            nsjobs=lastSubmittedJob+int(val)
                    elif (val=='all'):
                        pass
                    else:
                        msg = 'Bad submission option <'+str(val)+'>\n'
                        msg += '      Must be an integer or "all"'
                        msg += '      Generic range is not allowed"'
                        raise CrabException(msg)
                    pass
                common.logger.debug(5,'nsjobs '+str(nsjobs))
    
                # submit N from last submitted job
                nj_list = range(lastSubmittedJob, nsjobs)
                common.logger.debug(5,'nj_list '+str(nj_list))

                if len(nj_list) != 0:
                    # Instantiate Submitter object
#                    self.actions[opt] = Submitter(self.cfg_params, nj_list, self.job_type)
                    self.actions[opt] = Submitter(self.cfg_params, nj_list)
                    # Create and initialize JobList
                    if len(common.job_list) == 0 :
                        common.job_list = JobList(common.jobDB.nJobs(),
                                                  None)
                        common.job_list.setJDLNames(self.job_type_name+'.jdl')
                        pass
                    pass
                pass

            elif ( opt == '-list' ):
                jobs = self.parseRange_(val)

                common.jobDB.dump(jobs)
                pass

            elif ( opt == '-status' ):
                jobs = self.parseRange_(val)

                if len(jobs) != 0:
                    if ( self.flag_useboss == 1 ):     
                        self.actions[opt] = StatusBoss(self.cfg_params)
                    else:                         
                        self.actions[opt] = Status(self.cfg_params, jobs)
                        pass
                    pass
                pass

            elif ( opt == '-kill' ):

                if ( self.flag_useboss == 1 ):
                    if val: 
                        if val =='all':
                            allBoss_id = common.scheduler.listBoss()
                            jobs = allBoss_id.keys()
                        else:
                            jobs = self.parseRange_(val)
                        common.scheduler.cancel(jobs)
                    else:
                        common.logger.message("Warning: with '-kill' you _MUST_ specify a job range or 'all'")
                else:
                    if val:
                        jobs = self.parseRange_(val)

                        for nj in jobs:
                            st = common.jobDB.status(nj)
                            if st == 'S' or st == 'A':
                                jid = common.jobDB.jobId(nj)
                                common.logger.message("Killing job # "+`(nj+1)`)
                                common.scheduler.cancel(jid)
                                common.jobDB.setStatus(nj, 'K')
                                pass
                            pass

                        common.jobDB.save()
                        pass
                    else:
                         common.logger.message("Warning: with '-kill' you _MUST_ specify a job range or 'all'")

            elif ( opt == '-getoutput' ):

                if ( self.flag_useboss == 1 ):
                    if val=='all' or val==None or val=='':
                        allBoss_id = common.scheduler.listBoss()
                        jobs = allBoss_id.keys()
                    else:
                        jobs = self.parseRange_(val)
                    common.scheduler.getOutput(jobs) 
                else:
                    jobs = self.parseRange_(val) 

                    ## also this: create a ActorClass (GetOutput)
                    jobs_done = []
                    for nj in jobs:
                        st = common.jobDB.status(nj)
                        if st == 'D':
                            jobs_done.append(nj)
                            pass
                        elif st == 'S':
                            jid = common.jobDB.jobId(nj)
                            currStatus = common.scheduler.queryStatus(jid)
                            if currStatus=="Done":
                                jobs_done.append(nj)
                            else:
                                msg = 'Job # '+`(nj+1)`+' submitted but still status '+currStatus+' not possible to get output'
                                common.logger.message(msg)
                            pass
                        else:
                          #  common.logger.message('Jobs #'+`(nj+1)`+' has status '+st+' not possible to get output')
                            pass
                        pass

                    for nj in jobs_done:
                        jid = common.jobDB.jobId(nj)
                        dir = common.scheduler.getOutput(jid)
                        common.jobDB.setStatus(nj, 'Y')

                    # Rename the directory with results to smth readable
                    new_dir = common.work_space.resDir()
                    if ( dir != '' ) :
                        try:
                            files = os.listdir(dir)
                            for file in files:
                                os.rename(dir+'/'+file, new_dir+'/'+file)
                            os.rmdir(dir)
                        except OSError, e:
                            msg = 'rename files from '+dir+' to '+new_dir+' error: '
                            msg += str(e)
                            common.logger.message(msg)
                            # ignore error
                            pass
                        pass
                    ###

                    resFlag = 0
                    exCode = common.scheduler.getExitStatus(jid)
                    Statistic.Monitor('retrieved',resFlag,jid,exCode)

                    msg = 'Results of Job # '+`(nj+1)`+' are in '+new_dir
                    common.logger.message(msg)
                    pass

                common.jobDB.save()
                pass

            elif ( opt == '-resubmit' ):
                if ( self.flag_useboss == 1 ):
                    if val=='all' or val==None or val=='':
                        allBoss_id = common.scheduler.listBoss()
                        jobs = allBoss_id.keys()
                    else:
                        jobs = self.parseRange_(val)
                else:
                    if val:
                        jobs = self.parseRange_(val)

                if val:
                    # create a list of jobs to be resubmitted.

                    ### as before, create a Resubmittter Class
                    allBoss_id = common.scheduler.listBoss()
                    maxIndex = allBoss_id.keys()
                    nj_list = []
                    for nj in jobs:
                        if int(nj) <= int(len(maxIndex)) :
                            st = common.jobDB.status(int(nj)-1)
                            if st in ['K','A']:
                                nj_list.append(int(nj)-1)
                                common.jobDB.setStatus(int(nj)-1,'C')
                            elif st == 'Y':
                                common.scheduler.moveOutput(nj)
                                nj_list.append(int(nj)-1)
                                st = common.jobDB.setStatus(int(nj)-1,'RC')
                            elif st in ['C','X']:
                                common.logger.message('Job #'+`int(nj)`+' has status '+crabJobStatusToString(st)+' not yet submitted!!!')
                                pass
                            elif st == 'D':
                                common.logger.message('Job #'+`int(nj)`+' has status '+crabJobStatusToString(st)+' must be retrieved before resubmission')
                            else:
                                common.logger.message('Job #'+`nj`+' has status '+crabJobStatusToString(st)+' must be "killed" before resubmission')
                        else:
                            common.logger.message('Job #'+`int(nj)`+' no possible to resubmit!! out of range')
                    if len(common.job_list) == 0 :
                         common.job_list = JobList(common.jobDB.nJobs(),None)
                         common.job_list.setJDLNames(self.job_type_name+'.jdl')
                         pass

                    if len(nj_list) != 0:
                        common.scheduler.resubmit(nj_list)
                        # Instantiate Submitter object
                        self.actions[opt] = Submitter(self.cfg_params, nj_list)

                        pass
                    pass
                else:
                    common.logger.message("Warning: with '-resubmit' you _MUST_ specify a job range or 'all'")
                    common.logger.message("WARNING: _all_ job specified in the range will be resubmitted!!!")
                    pass
                common.jobDB.save()
                pass

            elif ( opt == '-cancelAndResubmit' ):

                if ( self.flag_useboss == 1 ):
                    if val:
                        if val =='all':
                            allBoss_id = common.scheduler.listBoss()
                            jobs = allBoss_id.keys()
                        else:
                            jobs = self.parseRange_(val)
                        # kill submitted jobs
                        common.scheduler.cancel(jobs)
                    else:
                        common.logger.message("Warning: with '-cancelAndResubmit' you _MUST_ specify a job range or 'all'")
                else:
                    if val:    
                        jobs = self.parseRange_(val)
                    else:
                        common.logger.message("Warning: with '-kill' you _MUST_ specify a job range or 'all'")
                        pass

                # resubmit cancelled jobs.
                if val:
                    nj_list = []
                    for nj in jobs:
                        if ( self.flag_useboss != 1 ):
                            st = common.jobDB.status(nj)
                            if st == 'S':
                                jid = common.jobDB.jobId(nj)
                                common.scheduler.cancel(jid)
                                st = 'K'
                                common.jobDB.setStatus(nj, st)
                                pass
                            common.jobDB.save()
                            pass  
                        st = common.jobDB.status(int(nj)-1)
                        if st in ['K','A']:
                            nj_list.append(int(nj)-1)
                            common.jobDB.setStatus(int(nj)-1,'C')
                        elif st == 'Y':
                            common.scheduler.moveOutput(nj)
                            nj_list.append(int(nj)-1)
                            st = common.jobDB.setStatus(int(nj)-1,'RC')
                        elif st in ['C','X']:
                            common.logger.message('Job #'+`int(nj)`+' has status '+crabJobStatusToString(st)+' not yet submitted!!!')
                            pass
                        elif st == 'D':
                            common.logger.message('Job #'+`int(nj)`+' has status '+crabJobStatusToString(st)+' must be retrieved before resubmission')
                        else:
                            common.logger.message('Job #'+`nj`+' has status '+crabJobStatusToString(st)+' must be "killed" before resubmission')
                            pass
                                                                                                                                                            
                    if len(common.job_list) == 0 :
                        common.job_list = JobList(common.jobDB.nJobs(),None)
                        common.job_list.setJDLNames(self.job_type_name+'.jdl')
                        pass
                                                                                                                                                             
                    if len(nj_list) != 0:
                        common.scheduler.resubmit(nj_list)
                        self.actions[opt] = Submitter(self.cfg_params, nj_list)
                        pass
                        pass
                else:
                    common.logger.message("WARNING: _all_ job specified in the rage will be cancelled and resubmitted!!!")
                    pass
                common.jobDB.save()
                pass

            elif ( opt == '-testJdl' ):
                jobs = self.parseRange_(val)
                nj_list = []
                for nj in jobs:
                    st = common.jobDB.status(nj-1)
                    if st == 'C': nj_list.append(nj-1)
                    pass

                if len(nj_list) != 0:
                    # Instantiate Submitter object
                    self.actions[opt] = Checker(self.cfg_params, nj_list)

                    # Create and initialize JobList

                    if len(common.job_list) == 0 :
                        common.job_list = JobList(common.jobDB.nJobs(), None)
                        common.job_list.setJDLNames(self.job_type_name+'.jdl')
                        pass
                    pass

            elif ( opt == '-postMortem' ):
                jobs = self.parseRange_(val)
                nj_list = []
                for nj in jobs:
                    # fede: nj scala di uno perche' e' l'internal id di boss
                    # ed il jobDB parte da zero ...
                    st = common.jobDB.status(int(nj)-1)
                    if st not in ['X', 'C']: nj_list.append(int(nj))
                    pass

                if len(nj_list) != 0:
                    # Instantiate Submitter object
                    self.actions[opt] = PostMortem(self.cfg_params, nj_list,self.flag_useboss)

                    # Create and initialize JobList

                    if len(common.job_list) == 0 :
                        common.job_list = JobList(common.jobDB.nJobs(), None)
                        common.job_list.setJDLNames(self.job_type_name+'.jdl')
                        pass
                    pass
                else:
                    common.logger.message("No jobs to analyze")

            elif ( opt == '-clean' ):
                if val != None:
                    raise CrabException("No range allowed for '-clean'")
                
                theCleaner = Cleaner(self.scheduler_name == 'boss', self.cfg_params)
                theCleaner.clean()

            pass
        return

    def createWorkingSpace_(self):
        new_dir = ''

        try:
            new_dir = self.cfg_params['USER.ui_working_dir']
            self.cfg_params['taskId'] = self.cfg_params['user'] + '_' + string.split(new_dir, '/')[len(string.split(new_dir, '/')) - 1] + '_' + self.current_time
            if os.path.exists(new_dir):
                if os.listdir(new_dir):
                    msg = new_dir + ' already exists and is not empty. Please remove it before create new task'
                    raise CrabException(msg)
        except KeyError:
            new_dir = common.prog_name + '_' + self.name + '_' + self.current_time
            self.cfg_params['taskId'] = self.cfg_params['user'] + '_' + new_dir 
            new_dir = self.cwd + new_dir
            pass
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
        #print self.job_type_name 
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

#    def createJobtype_(self):
#        """
#        Create the jobtype specified in the crab.cfg file
#        """
#        file_name = 'cms_'+ string.lower(self.job_type_name)
#        klass_name = string.capitalize(self.job_type_name)
#
#        try:
#            klass = importName(file_name, klass_name)
#        except KeyError:
#            msg = 'No `class '+klass_name+'` found in file `'+file_name+'.py`'
#            raise CrabException(msg)
#        except ImportError, e:
#            msg = 'Cannot create job type '+self.job_type_name
#            msg += ' (file: '+file_name+', class '+klass_name+'):\n'
#            msg += str(e)
#            raise CrabException(msg)
#        job_type = klass(self.cfg_params)
#        return job_type

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

    if len(opts):
        for opt in opts.keys():
            if opt in ('-v', '-version', '--version') :
                print Crab.version()
                return 1
            if opt in ('-h','-help','--help') :
                if opts[opt] : help(opts[opt])
                else:          help()
                return 1
    else:
        usage()

    return 0

###########################################################################
if __name__ == '__main__':


#    # Initial settings for Python modules. Avoid appending manually lib paths.
#    try:
#        path=os.environ['EDG_WL_LOCATION']
#    except:
#        print "Error: Please set the EDG_WL_LOCATION environment variable pointing to the userinterface installation path"
#        sys.exit(1)
#                                                                                                                                                             
#    libPath=os.path.join(path, "lib")
#    sys.path.append(libPath)
#    libPath=os.path.join(path, "lib", "python")
#    sys.path.append(libPath)


    # Parse command-line options and create a dictionary with
    # key-value pairs.

    options = parseOptions(sys.argv[1:])

    # Process "help" options, such as '-help', '-version'

    if processHelpOptions(options) : sys.exit(0)

    # Create, initialize, and run a Crab object

    try:
        crab = Crab(options)
        crab.run()
        crab.cfg_params['apmon'].free()
    except CrabException, e:
        print '\n' + common.prog_name + ': ' + str(e) + '\n'
        if common.logger:
            common.logger.write('ERROR: '+str(e)+'\n')
            pass
        pass

    pass
