from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import os, sys, tempfile, shutil, time
from Submitter import *

class SchedulerBoss(Scheduler):
    def __init__(self):
        Scheduler.__init__(self,"BOSS")
        self.checkBoss_()
        self.schedRegistered = {}
        self.jobtypeRegistered = {}
        return

    def checkBoss_(self): 
        """
        Verify BOSS installation.
        """
        try:
            
            self.bossenv = os.environ["BOSS_ROOT"]
        except:
            msg = "Error: the BOSS_ROOT is not set."
            msg = msg + " Did you source crab.sh/csh or your bossenv.sh/csh from your BOSS area?\n"
            raise CrabException(msg)
        try:
            self.boss_dir = os.environ["CRABSCRIPT"]
        except:
            msg = "Error: the CRABSCRIPT is not set."
            msg = msg + " Did you source crab.sh/csh?\n"
            raise CrabException(msg)


    ###################### ---- OK for Boss4 ds
    def configBossDB_(self):
        """
        Configure Boss DB
        """
        # first I have to check if the db already esist
        self.boss_db_dir = common.work_space.shareDir()
        self.boss_db_name = 'bossDB'
        if os.path.isfile(self.boss_db_dir+self.boss_db_name) :
            common.logger.debug(6,'BossDB already exist')
        else:
            common.logger.debug(6,'Creating BossDB in '+self.boss_db_dir+self.boss_db_name)

            # First I have to create a SQLiteConfig.clad file in the proper directory
            cwd = os.getcwd()
            if not os.path.exists(self.boss_db_dir):
                os.mkdir(self.boss_db_dir)
            os.chdir(common.work_space.shareDir())
            confSQLFileName = 'SQLiteConfig.clad'
            confFile = open(confSQLFileName, 'w')
            confFile.write('[\n')
            confFile.write('SQLITE_DB_PATH = "'+self.boss_db_dir+'";\n')
            confFile.write('DB_NAME = "'+self.boss_db_name+'";\n')
            confFile.write(']\n')
            confFile.close()

            # then I have to run "bossAdmin configureDB"
            out = runBossCommand('bossAdmin configureDB',0)
     
            os.chdir(cwd)
     
        return

    ###################### ---- OK for Boss4 ds
    def configRT_(self): 
        """
        Configure Boss RealTime monitor
        """

        # check if RT is already configured
        boss_rt_check = "boss showRTMon"
        boss_out = runBossCommand(boss_rt_check,0)
        if string.find(boss_out, 'Default rtmon is: mysql') == -1 :
            common.logger.debug(6,'registering RT monitor')
            # First I have to create a SQLiteConfig.clad file in the proper directory
            cwd = os.getcwd()
            os.chdir(common.work_space.shareDir())
            confSQLFileName = os.environ["HOME"]+'/.bossrc/MySQLRTConfig.clad'
            confFile = open(confSQLFileName, 'w')

            confFile.write('[\n')
            # BOSS MySQL database file
            confFile.write('DB_NAME = "boss_rt_v4_2";')
            # Host where the MySQL server is running
            confFile.write('DB_HOST = "boss.bo.infn.it";\n')
            confFile.write('DB_DOMAIN = "bo.infn.it";\n')
            # Default BOSS MySQL user and password
            confFile.write('DB_USER = "BOSSv4_2manager";')
            confFile.write('DB_USER_PW = "BossMySQL";\n')
            # Guest BOSS MySQL user and password
            confFile.write('DB_GUEST = "BOSSv4_2monitor";')
            confFile.write('DB_GUEST_PW = "BossMySQL";\n')
            # MySQL table type
            confFile.write('TABLE_TYPE = "";\n')
            # MySQL port
            confFile.write('DB_PORT = 0;\n')
            # MySQL socket
            confFile.write('DB_SOCKET = "";\n')
            # MySQL client flag
            confFile.write('DB_CLIENT_FLAG = 0;\n')
            confFile.write('DB_CONNECT_TIMEOUT = 30;\n')
            confFile.write(']\n')
            confFile.close()

            # Registration of RealTime monitor
            register_script = 'registerMySQLRTmon'
            register_path = self.boss_dir + '/'
            if os.path.exists(register_path+register_script):
                boss_out = runBossCommand(register_path+register_script,0)
                if (boss_out==None): raise CrabException('Cannot execute '+register_script+'\nExiting')
                if string.find(boss_out, 'Usage') != -1 :
                    # print "boss_out = ", boss_out 
                    msg = 'Error: Problem with RealTime monitor registration\n'
                    raise CrabException(msg)
            else:
                msg = 'Warning: file '+ register_script + ' does not exist!\n'
                raise CrabException(msg)
            
            os.chdir(cwd)
        else:
            common.logger.debug(6,'RT monitor already registered')
            pass # RT already registered

        return

    def configure(self, cfg_params):
        
        try:    
            self.groupName = cfg_params['taskId']
        except:
            self.groupName = ''
         
        try:    
            self.outDir = cfg_params["USER.outputdir"] 
        except:
            self.outDir = common.work_space.resDir() 
        try:
            self.logDir = cfg_params["USER.logdir"]
        except:
            self.logDir = common.work_space.resDir()

        try:
            if (int(cfg_params["USER.use_central_bossdb"])==1): pass
            else: self.configBossDB_()
        except KeyError:
            self.configBossDB_()

        try:
            if (int(cfg_params["USER.use_boss_rt"])==1): self.configRT_()
        except KeyError:
            pass

        try: 
            self.boss_scheduler_name = cfg_params["CRAB.scheduler"]
        except KeyError: 
            msg = 'No real scheduler selected: edg, lsf ...'
            msg = msg + 'Please specify a scheduler type in the crab cfg file'
            raise CrabException(msg)

        try: 
            self.boss_jobtype = cfg_params["CRAB.jobtype"]
        except KeyError: 
            msg = 'Error: jobtype not defined ...'
            msg = msg + 'Please specify a jobtype in the cfg file'
            raise CrabException(msg)
 
        # create real scheduler (boss_scheduler)
        klass_name = 'Scheduler' + string.capitalize(self.boss_scheduler_name)
        file_name = klass_name
        try:
            klass = importName(file_name, klass_name)
        except KeyError:
            msg = 'No `class '+klass_name+'` found in file `'+file_name+'.py`'
            raise CrabException(msg)
        except ImportError, e:
            msg = 'Cannot create scheduler '+self.boss_scheduler_name
            msg += ' (file: '+file_name+', class '+klass_name+'):\n'
            msg += str(e)
            raise CrabException(msg)
        self.boss_scheduler = klass()
        self.boss_scheduler.configure(cfg_params)

    #    # create additional classad file
        self.schclassad = ''
 #   #    if (self.boss_scheduler.sched_parameter()):
 #       try:   
 #           self.schclassad = common.work_space.shareDir()+'/'+self.boss_scheduler.param
 #       except:
 #           pass  

        # check scheduler and jobtype registration in BOSS
        self.checkSchedRegistration_(self.boss_scheduler_name)

        self.checkJobtypeRegistration_(self.boss_jobtype) 
        self.checkJobtypeRegistration_(self.boss_scheduler_name) 
        self.checkJobtypeRegistration_('crab') 
        
        try: self.schedulerName = cfg_params['CRAB.scheduler']
        except KeyError: self.scheduler = ''

        return

    ###################### ---- OK for Boss4 ds
    def checkSchedRegistration_(self, sched_name): 
        """
        Verify scheduler registration.
        """
        ## we don't need to test this at every call:
        if (self.schedRegistered.has_key(sched_name)): return

        ## in some circumstances, boss schowSchedulers can result in a timout condition returning 
        ## BossDatabase::show :
        ## Not connected
        ## not connected
        ##
        ## implement 10 times retry for showSchedulers command with a sleep of 5 seconds in between
        ## if not succeeded afterwards, throw exception
        ##

        counter = 0
        query_succeeded = 0
        max_retries = 10
        sleep_interval = 5

        while query_succeeded == 0 :
            # increase counter, if reached max_retries, throw exception
            counter += 1
            if counter >= max_retries :
                msg = 'Boss cmd: boss showScheduler failed with "not connected" error message for the' + str(max_retries) + ' time.\n'
                msg += 'Abort registration.\n'
                raise CrabException(msg)

            ## we should cache the result of the first test
            boss_scheduler_check = "boss showSchedulers"
            boss_out = runBossCommand(boss_scheduler_check,0)
            
            if boss_out.find('not connected') == -1 :
                query_succeeded = 1
                if string.find(boss_out, sched_name) == -1 :
                    msg = sched_name + ' scheduler not registered in BOSS\n'
                    msg = msg + 'Starting registration\n'
                    common.logger.debug(6,msg)
                    # On demand registration of job type
                    register_path = self.boss_dir + '/'
                    register_boss_scheduler = 'register'+ string.upper(sched_name) + 'Scheduler'
            
                    if os.path.exists(register_path+register_boss_scheduler):
                        boss_out = runBossCommand(register_path+register_boss_scheduler,0)
                        if (boss_out==None): raise CrabException('Cannot execute '+register_boss_scheduler+'\nExiting')
                        if string.find(boss_out, 'Usage') != -1 :
                            msg = 'Error: Problem with scheduler '+sched_name+' registration\n'
                            raise CrabException(msg)
                    else:
                        msg = 'Warning: file '+ register_boss_scheduler + ' does not exist!\n'
                        msg = msg + 'Please create your scheduler plugins\n'
                        raise CrabException(msg)
            else :
                # sleep for defined sleep interval
                msg = 'Boss cmd: boss showScheduler failed with "not connected" error message for the' + str(max_retries) + ' time.\n'
                msg += 'Retry after ' + str(sleep_interval) + ' seconds.\n'
                common.logger.debug(5,msg)
                time.sleep(sleep_interval)

        # sched registered
        self.schedRegistered[sched_name] = 1
        return


    ###################### ---- OK for Boss4 ds
    def checkJobtypeRegistration_(self, jobtype): 
        """
        Verify jobtype registration.
        """
        ## we don't need to test this at every call:
        if (self.jobtypeRegistered.has_key(jobtype)): return

        ## in some circumstances, boss showProgramTypes can result in a timout condition returning 
        ## BossDatabase::show :
        ## Not connected
        ## not connected
        ##
        ## implement 10 times retry for showProgramTypes command with a sleep of 5 seconds in between
        ## if not succeeded afterwards, throw exception
        ##

        counter = 0
        query_succeeded = 0
        max_retries = 10
        sleep_interval = 5

        while query_succeeded == 0 :
            # increase counter, if reached max_retries, throw exception
            counter += 1
            if counter >= max_retries :
                msg = 'Boss cmd: boss showProgramTypes failed with "not connected" error message for the' + str(max_retries) + ' time.\n'
                msg += 'Abort registration.\n'
                raise CrabException(msg)

            ## we should cache the result of the first test
            boss_jobtype_check = "boss showProgramTypes"
            boss_out = runBossCommand(boss_jobtype_check,0)
            
            if boss_out.find('not connected') == -1 :
                query_succeeded = 1
                if string.find(boss_out, jobtype) == -1 :
                    msg =  'Warning:' + jobtype + ' jobtype not registered in BOSS\n'
                    msg = msg + 'Starting registration \n'
                    common.logger.debug(6,msg)
                    register_path = self.boss_dir + '/'
                    register_boss_jobtype= 'register' + string.upper(jobtype) + 'job'
                    if os.path.exists(register_path+register_boss_jobtype):
                        register_boss_jobtype= 'register' + string.upper(jobtype) + 'job'
                        boss_out = runBossCommand(register_path+register_boss_jobtype,0)
                        if (boss_out==None): raise CrabException('Cannot execute '+register_boss_jobtype+'\nExiting')
                        if string.find(boss_out, 'Usage') != -1 :
                            msg = 'Error: Problem with job '+jobtype+' registration\n'
                            raise CrabException(msg)
                    else:
                        self.jobtypeRegistered[jobtype] = 2
                        msg = 'Warning: file '+ register_boss_jobtype + ' does not exist!\n'
                        msg = msg + 'Will be used only JOB as default jobtype\n'
                        common.logger.message(msg)
                        return
            else :
                # sleep for defined sleep interval
                msg = 'Boss cmd: boss showProgramTypes failed with "not connected" error message for the' + str(max_retries) + ' time.\n'
                msg += 'Retry after ' + str(sleep_interval) + ' seconds.\n'
                common.logger.debug(5,msg)
                time.sleep(sleep_interval)

        ## we should cache the result of the first test
        # jobtype registered
        self.jobtypeRegistered[jobtype] = 1
        return


    ###################### ---- OK for Boss4 ds
    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return self.boss_scheduler.wsSetupEnvironment() 

    ###################### ---- OK for Boss4 ds
#    def createXMLSchScript(self, nj):
    """
    INDY
    come vedi qui ho cambiato il prototipo
    createFakeJdl non dovrebbe aver bisogno di un job number:
    in effetti usa il jobType, non il job.
    In poche parole la cosa potrebbe essere piu' diretta
    """
    def createXMLSchScript(self, nj, argsList):
        """
        Create script_scheduler file (JDL for EDG)
        """
        # create additional classad file
        self.boss_scheduler.sched_parameter()

        self.boss_scheduler.createXMLSchScript(nj, argsList)
        return

    ###################### ---- OK for Boss4 ds
    def declareJob_(self):                       #Changed For BOSS4
        """
        BOSS declaration of jobs
        """
        start = time.time()
        cmd = 'boss declare -xmlfile '+common.work_space.shareDir()+self.boss_jobtype+'.xml'
        cmd_out = runBossCommand(cmd,0,3600)
        stop = time.time()
        # debug
        msg = 'BOSS declaration:' + cmd
        common.logger.debug(4,msg)
        msg = 'BOSS declaration output:' + cmd_out
        common.logger.debug(4,msg)
        ###

        #  -------------------------------------------------> From Here changed for Boss4
        if cmd_out.find('Task') < 0 :
            common.logger.message('ERROR: BOSS declare failed: no TASK ID for jobs')
            raise CrabException(msg)

        # job counter, jobs in JobDB run from 0 - n-1
        num_job = 0

        for line in cmd_out.splitlines(): # boss4
            if line.find('Task') >= 0 :
                self.Task_id = line.split(":")[-1].strip()
                common.taskDB.setDict('BossTaskId',self.Task_id)
                common.logger.debug(4,"TASK ID =  "+self.Task_id)
            if line.find('Declared Chain') >= 0 :
                self.chain_id = line.split(":")[-1].strip()
                common.jobDB.setBossId(num_job, self.chain_id)
                common.logger.debug(4,"CHAIN ID =  "+self.chain_id+" of job: "+str(num_job))
                num_job += 1
        return 

    def checkProxy(self):
        """
        Check the Globus proxy. 
        """
        return self.boss_scheduler.checkProxy()

    ###################### ---- OK for Boss4 ds
    def loggingInfo(self, nj):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        return self.boss_scheduler.loggingInfo(nj) 

    ##########################################   ---- OK for Boss4 ds
    def listMatch(self, nj, Block):
        """
        Check the compatibility of available resources
        """
        start = time.time()
        schcladstring = ''
        self.schclassad = common.work_space.shareDir()+'/'+'sched_param_'+str(Block)+'.clad'
        if os.path.isfile(self.schclassad):  
            schcladstring=' -schclassad '+self.schclassad
        cmd = 'boss listMatch -scheduler '+ str(self.schedulerName) + ' -taskid  '+common.taskDB.dict('BossTaskId')+' -jobid ' +common.jobDB.bossId(nj) + schcladstring
        cmd_out = runBossCommand(cmd)
        stop = time.time()
        common.logger.debug(1,"listMatch time :"+str(stop-start))
        common.logger.write("listMatch time :"+str(stop-start))

        #return self.boss_scheduler.listMatch(nj)
        jdl = ''
        #cmd_out = runCommand(cmd,0,10)
        if not cmd_out:
            raise CrabException("ERROR: "+cmd+" failed!")

        return self.parseListMatch_(cmd_out, jdl)

    def parseListMatch_(self, out, jdl):
        """
        Parse the f* output of edg-list-match and produce something sensible
        """
        reComment = re.compile( r'^\**$' )
        reEmptyLine = re.compile( r'^$' )
        reVO = re.compile( r'Selected Virtual Organisation name.*' )
        reLine = re.compile( r'.*')
        #reCE = re.compile( r'(.*:.*)')
        reCE = re.compile( r'(\S*):')
        reCEId = re.compile( r'CEId.*')
        reNO = re.compile( r'No Computing Element matching' )
        reRB = re.compile( r'Connecting to host' )
        next = 0
        CEs=[]
        Match=0

        #print out
        lines = reLine.findall(out)

        i=0
        CEs=[]
        for line in lines:
            string.strip(line)
            if reNO.match( line ):
                common.logger.debug(5,line)
                return 0
                pass
            if reVO.match( line ):
                VO =reVO.match( line ).group()
                common.logger.debug(5,"VO "+VO)
                pass

            if reRB.match( line ):
                RB = reRB.match(line).group()
                common.logger.debug(5,"RB "+RB)
                pass

            if reCEId.search( line ):
                for lineCE in lines[i:-1]:
                    if string.find(lineCE, "Log file created") != -1: 
                       break 
                    if reCE.search( lineCE ):
                        CE = string.strip(reCE.search(lineCE).group(1))
                        CEs.append(CE.split(':')[0])
                        pass 
                    pass
                pass
            i=i+1
            pass

        common.logger.debug(5,"All CE :"+str(CEs))

        sites = []
        [sites.append(it) for it in CEs if not sites.count(it)]

        common.logger.debug(5,"All Sites :"+str(sites))
        common.logger.message("Matched Sites :"+str(sites))
        return len(sites)
    ##########################################   ----  add as workaround for list match with Boss4 ds
    def createFakeJdl(self,nj):
        return self.boss_scheduler.createFakeJdl(nj)
    
    ###################### ---- OK for Boss4 ds
    #def submit(self, nj):
    def submit(self,list):
        """
        Submit BOSS function.
        Submit one job. nj -- job number.
        """

        boss_scheduler_name = string.lower(self.boss_scheduler.name())
        boss_scheduler_id = None
        i = list[0]
        jobsList = list[1]
        schcladstring = ''
        self.schclassad = common.work_space.shareDir()+'/'+'sched_param_'+str(i)+'.clad'# TODO add a check is file exist
        if os.path.isfile(self.schclassad):  
            schcladstring=' -schclassad '+self.schclassad
        #cmd = 'boss submit -taskid  '+common.taskDB.dict('BossTaskId')+' -jobid ' +common.jobDB.bossId(nj) + schcladstring
        #cmd = 'boss submit -taskid  '+common.taskDB.dict('BossTaskId')+' -jobid ' +common.jobDB.bossId(first)+':'+ common.jobDB.bossId(last)+ schcladstring
        cmd = 'boss submit -taskid  '+common.taskDB.dict('BossTaskId')+' -jobid '+string.join(jobsList,',')+ schcladstring
        cmd_out = runBossCommand(cmd)
        # debug
        msg = 'BOSS submission: ' + cmd
        common.logger.debug(4,msg)
        msg = 'BOSS submission output: ' + cmd_out
        common.logger.debug(4,msg)
        jid=[]
        #
        # Marco.
        #
        bjid = []
        #
        # Marco.
        #
        if not cmd_out :
            msg = 'ERROR: BOSS submission failed: ' + cmd
            common.logger.message(msg)
            return None
        else:
            for line in cmd_out.splitlines(): # boss4
                if  line.find('error') >= 0 :
                    msg = 'ERROR: BOSS submission failed: ' + cmd + ', stopping submission at id ' + jid 
                    common.logger.message(msg)
                    return jid, bjid
                if line.find('Scheduler ID for job') >= 0 :
                    #jid = line.split()[-1]
                    jid.append(line.split()[-1])
                    #
                    # Marco
                    #
                    bjid.append(line.split()[-3])
        return jid, bjid
        #
        # Marco.
        #

    ###################### ---- OK for Boss4 ds
    def moveOutput(self, int_id):
        """
        Move output of job already retrieved 
        """
        self.current_time = time.strftime('%y%m%d_%H%M%S',time.localtime(time.time()))
        resDir = common.work_space.resDir()
        resDirSave = resDir+'res_backup'
        if not os.path.exists(resDirSave):
            os.mkdir(resDirSave)

        boss_id = int(int_id) 
        cmd = 'bossAdmin SQL -fieldsLen -query "select CHAIN_OUTFILES from CHAIN where ID='+str(boss_id)+' and JOB.TASK_ID=\''+common.taskDB.dict('BossTaskId')+'\'"'
        cmd_out = runBossCommand(cmd)

        nline = 0
        for line in cmd_out.splitlines():
            if nline == 3:
                files = line.split(',')
                for i in files:
                    i=i.strip()
                    i=i.strip('{}')
                    i=i.strip()
                    i=i.strip('"')
                                    
                    if os.path.exists(self.outDir+'/'+i):
                        os.rename(self.outDir+'/'+i, resDirSave+'/'+i+'_'+self.current_time)
                        common.logger.message('Output file '+i+' moved to '+resDirSave)

                    if os.path.exists(self.logDir+'/'+i):
                        os.rename(self.logDir+'/'+i, resDirSave+'/'+i+'_'+self.current_time)
                        common.logger.message('Output file '+i+' moved to '+resDirSave)
            nline = nline + 1
        return


    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """

        return self.boss_scheduler.queryDetailedStatus(id)

    ###################### ---- OK for Boss4 ds
    def getOutput(self, int_id):
        """
        Get output for a finished job with id.
        Returns the name of directory with results.
        """
        if not os.path.isdir(self.logDir) or not os.path.isdir(self.outDir):
            msg =  ' Output or Log dir not found!! check '+self.logDir+' and '+self.outDir
            raise CrabException(msg)
        common.jobDB.load()
        self.boss_scheduler.checkProxy()
        allBoss_id = common.scheduler.listBoss()
        
        bossTaskId = common.taskDB.dict('BossTaskId')
        ## first get the status of all job in the list
        statusList = common.scheduler.queryStatusList(bossTaskId, int_id)

        ## then loop over jobs and retrieve it if it's the case


        for i_id in int_id :
            if int(i_id) not in allBoss_id: 
                msg = 'Job # '+`int(i_id)`+' out of range for task '+ self.groupName
                common.logger.message(msg) 
            else:
                dir = self.outDir 
                logDir = self.logDir
                boss_id = i_id 
                #bossTaskIdStatus = common.scheduler.queryStatus(bossTaskId, boss_id)
                bossTaskIdStatus = statusList[boss_id]
                if bossTaskIdStatus == 'Done (Success)' or bossTaskIdStatus == 'Done (Abort)':   
                    cmd = 'boss getOutput -taskid  '+bossTaskId+' -jobid ' +str(boss_id) +' -outdir ' +dir
                    cmd_out = runBossCommand(cmd)
                    if cmd_out.find('error retrieving output') >= 0 :
                        msg = 'Results of Job # '+`int(i_id)`+' have been corrupted and could not be retrieved.'
                        common.logger.message(msg)
                        common.jobDB.setStatus(int(i_id)-1, 'Z') 
                    else :
                        if logDir != dir:
                            try:
                                cmd = 'mv '+str(dir)+'/*'+`int(i_id)`+'.std* '+str(dir)+'/.BrokerInfo ' +str(logDir)
                                cmd_out = runCommand(cmd)
                                msg = 'Results of Job # '+`int(i_id)`+' are in '+dir+' (log files are in '+logDir+')' 
                                common.logger.message(msg)
                            except:
                                msg = 'Problem with copy of job results' 
                                common.logger.message(msg)
                                pass  
                        else:   
                            msg = 'Results of Job # '+`int(i_id)`+' are in '+dir
                            common.logger.message(msg)
                        resFlag = 0
                        jid = common.scheduler.boss_SID(int(i_id)) 
                        try:
                            exCode = common.scheduler.getExitStatus(jid)
                        except:
                            exCode = ' '
                        Statistic.Monitor('retrieved',resFlag,jid,exCode,'dest')
                        common.jobDB.setStatus(int(i_id)-1, 'Y') 
                elif bossTaskIdStatus == 'Running' :
                    msg = 'Job # '+`int(i_id)`+' has status '+bossTaskIdStatus+'. It is not possible yet to retrieve the output.'
                    common.logger.message(msg)
                elif bossTaskIdStatus == 'Cleared' :
                    msg = 'Job # '+`int(i_id)`+' has status '+bossTaskIdStatus+'. The output was already retrieved.'
                    common.logger.message(msg)
                elif bossTaskIdStatus == 'Aborted' :
                    msg = 'Job # '+`int(i_id)`+' has status '+bossTaskIdStatus+'. It is not possible to retrieve the output.'
                    common.logger.message(msg)
                else:
                    msg = 'Job # '+`int(i_id)`+' has status '+bossTaskIdStatus+'. It is currently not possible to retrieve the output.'
                    common.logger.message(msg)
                dir += os.environ['USER']
                dir += '_' + os.path.basename(str(boss_id))
            pass
        common.jobDB.save() 
        return

    ###################### ---- OK for Boss4 ds
    def cancel(self,int_id):
        """
        Cancel the EDG job with id: if id == -1, means all jobs.
        """
        #print "CANCEL -------------------------"
        #print "int_id ",int_id," nSubmitted ", common.jobDB.nSubmittedJobs()
        
        subm_id = []
        for id in int_id:
           if ( common.jobDB.status(id-1) in ['S','R']) and (id not in subm_id):
              subm_id.append(id)
        #print "--------------"
        #print subm_id
        bossTaskId = common.taskDB.dict('BossTaskId')
        ## first get the status of all job in the list
        statusList = common.scheduler.queryStatusList(bossTaskId, subm_id)
        
        if len(subm_id)==common.jobDB.nSubmittedJobs() and common.jobDB.nSubmittedJobs()>0:
            bossTaskId = common.taskDB.dict('BossTaskId')
            common.logger.message("Killing jobs # "+str(subm_id[0])+':'+str(subm_id[-1]))
            cmd = 'boss kill -taskid '+bossTaskId+' -jobid '+str(subm_id[0])+':'+str(subm_id[-1])
            cmd_out = runBossCommand(cmd)

            if not self.parseBossOutput(cmd_out): 
                #for job in range(common.jobDB.nJobs()):
                for job in subm_id:
                    #status =  common.scheduler.queryStatus(bossTaskId,job) 
                    status = statusList[job]
                    if status not in ['Done (Success)','Killed','Cleared','Done (Aborted)','Created']:
                        common.jobDB.setStatus(job -1, 'K')
            else:
                common.logger.message("Error killing jobs # "+str(subm_id[0])+':'+str(subm_id[-1])+" . See log for details")

        else:

            common.jobDB.load() 
            allBoss_id = common.scheduler.listBoss() 
            for i_id in int_id :
                if int(i_id) not in allBoss_id:
                    msg = 'Job # '+`(i_id)`+' out of range for task '+self.groupName
                    common.logger.message(msg)
                else:
                   # boss_id = allBoss_id[int(i_id)]
                    boss_id = i_id 
                    #bossTaskId = common.taskDB.dict('BossTaskId')
                    #status =  common.scheduler.queryStatus(bossTaskId,boss_id) 
                    status = statusList[boss_id]
                    if status == 'Done (Success)' or status == 'Killed' or status =='Cleared' or status ==  'Done (Aborted)' or status == 'Created':
                        msg = 'Job # '+`int(i_id)`+' has status '+status+' not possible to Kill it'
                        common.logger.message(msg) 
                    else:
                        cmd = 'boss kill -taskid '+bossTaskId+' -jobid '+str(boss_id) 
                        common.logger.message("Killing job # "+`int(i_id)`)
                        cmd_out = runBossCommand(cmd)
                        if not self.parseBossOutput(cmd_out): 
                            common.jobDB.setStatus(int(i_id)-1, 'K')
                        else:
                            common.logger.message("Error killing job # "+`int(i_id)`+" . See log for details")
                        pass
                    pass
                pass
            common.jobDB.save()
            pass
        return #cmd_out    

    ################################################################ To remove when Boss4 store this info  DS. (start)
    def getAttribute(self, id, attr):
        return self.boss_scheduler.getStatusAttribute_(id, attr)

    def getExitStatus(self, id):
        return self.boss_scheduler.getStatusAttribute_(id, 'exit_code')

    def queryDest(self, id):  
        return self.boss_scheduler.getStatusAttribute_(id, 'destination')
    ################################################################   (stop)

    def wsCopyInput(self):
        return self.boss_scheduler.wsCopyInput()

    def wsCopyOutput(self):
        return self.boss_scheduler.wsCopyOutput()

    def wsRegisterOutput(self):  
        return self.boss_scheduler.wsRegisterOutput()

    ##############################   OK for BOSS4 ds. 
    ############################# ----> we use the SID for the postMortem... probably this functionality come for free with BOSS4? 
    def boss_SID(self,int_ID):
        """ Return Sid of job """
        SID = ''

        if common.jobDB.nSubmittedJobs() == 0:
            common.jobDB.load()

        SID = common.jobDB.jobId(int_ID-1)
    
        return SID

    ################################################## To change "much" when Boss4 store also this infos  DS.
    def queryEveryStatus(self,taskid):
        """ Query a status of all jobs with specified boss taskid """

        self.boss_scheduler.checkProxy()
        status={
            'H':'Hold',
            'U':'Ready',
            'I':'Scheduled',
            'X':'Canceled',
            'W':'Created',
            'R':'Running',
            'SC':'Checkpointed',
            'SS':'Scheduled',
            'SR':'Ready',
            'RE':'Ready',
            'SW':'Waiting',
            'SU':'Submitted',
            'UN':'Undefined',
            'SK':'Cancelled',
            'SD':'Done (Success)',
            'SA':'Aborted',
            'DA':'Done (Aborted)',
            'SE':'Cleared',
            'OR':'Done (Success)',
            'A?':'Aborted',
            'K':'Killed',
            'E':'Cleared',
            'Z':'Cleared (Corrupt)',
            'NA':'Unknown',
            'I?':'Idle',
            'O?':'Done',
            'R?':'Running'             
            }
        cmd = 'boss q -taskid '+str(taskid)+' -all' 
        cmd_out = runBossCommand(cmd)

        # debug
        msg = 'BOSS status ' + cmd
        common.logger.debug(4,msg)
        msg = 'BOSS status output: ' + cmd_out
        common.logger.debug(4,msg)
        ###  

        # fill dictionary { 'bossid' : 'status' , ... }
        results = {}
        nline=0
        for line in cmd_out.splitlines():
            if nline >= 1:
                header = line.split()
                if len(header) >= 9 and len(header) <= 11 :
                    results[int(header[1])] = status[header[5]]
            nline=nline+1

        return results

    ##################################################
    def queryStatusList(self,taskid,list_id):
        """ Query a status of the job with id """

        self.boss_scheduler.checkProxy()
        status={
            'H':'Hold',
            'U':'Ready',
            'I':'Scheduled',
            'X':'Canceled',
            'W':'Created',
            'R':'Running',
            'SC':'Checkpointed',
            'SS':'Scheduled',
            'SR':'Ready',
            'RE':'Ready',
            'SW':'Waiting',
            'SU':'Submitted',
            'UN':'Undefined',
            'SK':'Cancelled',
            'SD':'Done (Success)',
            'SA':'Aborted',
            'DA':'Done (Aborted)',
            'SE':'Cleared',
            'OR':'Done (Success)',
            'A?':'Aborted',
            'K':'Killed',
            'E':'Cleared',
            'Z':'Cleared (Corrupt)',
            'NA':'Unknown',
            'I?':'Idle',
            'O?':'Done',
            'R?':'Running'             
            }
        allBoss_id = common.scheduler.listBoss()
        tmpQ = ''
        if not len(allBoss_id)==len(list_id): tmpQ = ' -jobid '+string.join(map(str,list_id),",")
        cmd = 'boss q -taskid '+str(taskid)+tmpQ+' -all'
        cmd_out = runBossCommand(cmd)
        
        # debug
        msg = 'BOSS status ' + cmd
        common.logger.debug(4,msg)
        msg = 'BOSS status output: ' + cmd_out
        common.logger.debug(4,msg)
        ###  
        nline=0
        result={}
        fielddesc=()
        for line in cmd_out.splitlines():
            if nline==0:
                fielddesc=line.split()
            else:
                if nline >= 1:
                    array = line.split()
                    if isInt(array[1]) :
                        result[int(array[1])] = status[array[5]]
            nline += 1
        return result

    ###################### ---- OK for Boss4 ds
    def listBoss(self):
        """
        Return a list of all boss_Id of a task
        """
        ListBoss_ID = []
        cmd = 'bossAdmin SQL -fieldsLen -query "select JOB.CHAIN_ID from JOB where JOB.TASK_ID=\''+common.taskDB.dict('BossTaskId')+'\'"'   
        cmd_out = runBossCommand(cmd,0)
        nline = 0
        for line in cmd_out.splitlines():
            if nline > 2 :
                boss_Id = string.strip(line)
                ListBoss_ID.append(int(boss_Id))
            nline = nline + 1
        cmd = 'bossAdmin SQL -fieldsLen -query "select ENDED_JOB.CHAIN_ID from ENDED_JOB where ENDED_JOB.TASK_ID=\''+common.taskDB.dict('BossTaskId')+'\'"'   
        cmd_out = runBossCommand(cmd,0)
        nline = 0
        for line in cmd_out.splitlines():
            if nline > 2 :
                boss_Id = string.strip(line)
                ListBoss_ID.append(int(boss_Id))
            nline = nline + 1
        ListBoss_ID.sort()
        listBoss_Uniq = []
        for i in ListBoss_ID:  # check if there double index
            if i not in listBoss_Uniq: listBoss_Uniq.append(i)  
        return listBoss_Uniq

    ###################### ---- OK for Boss4 ds
    def setOutLogDir(self,outDir,logDir):
        if not os.path.isdir(outDir):
            #Create the directory
            os.mkdir(outDir)
            if not os.path.isdir(outDir):
                common.logger('Cannot mkdir ' + outDir + ' Switching to default ' + common.work_space.resDir())
                outDir = common.work_space.resDir()
        if not os.path.isdir(logDir):
            #Create the directory
            os.mkdir(logDir)
            if not os.path.isdir(logDir):
                common.logger('Cannot mkdir ' + logDir + ' Switching to default ' + common.work_space.resDir())
                logDir = common.work_space.resDir()
        return outDir, logDir

    ##################
    def parseBossOutput(self, out):
        reError = re.compile( r'status error' )
        lines = reError.findall(out)
        return len(lines)
        
        
