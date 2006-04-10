from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import os, sys, tempfile, shutil
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
            
            self.bossenv = os.environ["BOSSDIR"]
        except:
            msg = "Error: the BOSSDIR is not set."
            msg = msg + " Did you source crab.sh/csh or your bossenv.sh/csh from your BOSS area?\n"
            raise CrabException(msg)
        try:
            self.boss_dir = os.environ["CRABSCRIPT"]
        except:
            msg = "Error: the CRABSCRIPT is not set."
            msg = msg + " Did you source crab.sh/csh?\n"
            raise CrabException(msg)


    def configBossDB_(self):
        """
        Configure Boss DB
        """
        # first I have to chack if the db already esist
        self.boss_db_dir = common.work_space.shareDir()
        self.boss_db_name = 'bossDB'
        if os.path.isfile(self.boss_db_dir+self.boss_db_name) :
            common.logger.debug(5,'BossDB already exist')
        else:
            common.logger.debug(5,'Creating BossDB in '+self.boss_db_dir+self.boss_db_name)

            # First I have to create a SQLiteConfig.clad file in the proper directory
            cwd = os.getcwd()
            if not os.path.exists(self.boss_db_dir):
                os.mkdir(self.boss_db_dir)
            os.chdir(common.work_space.shareDir())
            confSQLFileName = 'SQLiteConfig.clad'
            confFile = open(confSQLFileName, 'w')
            confFile.write('[\n')
            confFile.write('SQLITE_DB_PATH = "'+self.boss_db_dir+'"\n')
            confFile.write('DB_NAME = "'+self.boss_db_name+'"\n')
            confFile.write(']\n')
            confFile.close()

            # then I have to run "boss configureDB"
            out = runBossCommand('boss configureDB',0)
     
            os.chdir(cwd)
     
        # that's it!
        return

    def configRT_(self): 
        """
        Configure Boss RealTime monitor
        """

        # check if RT is already configured
        boss_rt_check = "boss showRTMon"
        boss_out = runBossCommand(boss_rt_check,0)
        if string.find(boss_out, 'Default rtmon is: mysql') == -1 :
            common.logger.debug(5,'registering RT monitor')
            # First I have to create a SQLiteConfig.clad file in the proper directory
            cwd = os.getcwd()
            os.chdir(common.work_space.shareDir())
            confSQLFileName = os.environ["HOME"]+'/.bossrc/MySQLRTConfig.clad'
            confFile = open(confSQLFileName, 'w')

            confFile.write('[\n')
            # BOSS MySQL database file
            confFile.write('DB_NAME = "boss_rt_v3_6";')
            # Host where the MySQL server is running
            confFile.write('DB_HOST = "boss.bo.infn.it";')
            confFile.write('DB_DOMAIN = "bo.infn.it";')
            # Default BOSS MySQL user and password
            confFile.write('DB_USER = "BOSSv3_6manager";')
            confFile.write('DB_USER_PW = "BossMySQL";')
            # Guest BOSS MySQL user and password
            confFile.write('DB_GUEST = "BOSSv3_6monitor";')
            confFile.write('DB_GUEST_PW = "BossMySQL";')
            # MySQL table type
            confFile.write('TABLE_TYPE = "";')
            # MySQL port
            confFile.write('DB_PORT = 0;')
            # MySQL socket
            confFile.write('DB_SOCKET = "";')
            # MySQL client flag
            confFile.write('DB_CLIENT_FLAG = 0;')
            confFile.write(']\n')
            confFile.close()

            # Registration of RealTime monitor
            register_script = 'registerMySQLRTmon'
            register_path = self.boss_dir + '/script/'
            if os.path.exists(register_path+register_script):
                boss_out = runBossCommand(register_path+register_script,0)
                if (boss_out==None): raise CrabException('Cannot execute '+register_script+'\nExiting')
                if string.find(boss_out, 'Usage') != -1 :
                    msg = 'Error: Problem with RealTime monitor registration\n'
                    raise CrabException(msg)
            else:
                msg = 'Warning: file '+ register_script + ' does not exist!\n'
                raise CrabException(msg)
            
            os.chdir(cwd)
        else:
            common.logger.debug(5,'RT monitor already registered')
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

        # create additional classad file
        self.schclassad = ''
        if (self.boss_scheduler.sched_parameter()):
           self.schclassad = common.work_space.shareDir()+'/'+self.boss_scheduler.param
 
        # check scheduler and jobtype registration in BOSS
        self.checkSchedRegistration_(self.boss_scheduler_name)

        self.checkJobtypeRegistration_(self.boss_jobtype) 
        self.checkJobtypeRegistration_(self.boss_scheduler_name) 
        self.checkJobtypeRegistration_('crab') 
        
        return

    def checkSchedRegistration_(self, sched_name): 
        """
        Verify scheduler registration.
        """
        ## we don't need to test this at every call:
        if (self.schedRegistered.has_key(sched_name)): return

        ## we should cache the result of the first test
        boss_scheduler_check = "boss showSchedulers"
        boss_out = runBossCommand(boss_scheduler_check,0)
        if string.find(boss_out, sched_name) == -1 :
            msg = sched_name + ' scheduler not registered in BOSS\n'
            msg = msg + 'Starting registration\n'
            common.logger.debug(5,msg)
            # On demand registration of job type
            register_path = self.boss_dir + '/script/'
            register_boss_scheduler = './register'+ string.upper(sched_name) + 'Scheduler'
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
        self.schedRegistered[sched_name] = 1
        return


    def checkJobtypeRegistration_(self, jobtype): 
        """
        Verify jobtype registration.
        """
        ## we don't need to test this at every call:
        if (self.jobtypeRegistered.has_key(jobtype)): return

        ## we should cache the result of the first test
        boss_jobtype_check = "boss showJobTypes"
        boss_out = runBossCommand(boss_jobtype_check,0)
        if string.find(boss_out, jobtype) == -1 :
            msg =  'Warning:' + jobtype + ' jobtype not registered in BOSS\n'
            msg = msg + 'Starting registration \n'
            common.logger.debug(5,msg)
            register_path = self.boss_dir + '/script/'
            register_boss_jobtype= './register' + string.upper(jobtype) + 'job'
            if os.path.exists(register_path+register_boss_jobtype):
                register_boss_jobtype= './register' + string.upper(jobtype) + 'job'
                boss_out = runBossCommand(register_path+register_boss_jobtype,0)
                if (boss_out==None): raise CrabException('Cannot execute '+register_boss_scheduler+'\nExiting')
                if string.find(boss_out, 'Usage') != -1 :
                    msg = 'Error: Problem with job '+jobtype+' registration\n'
                    raise CrabException(msg)
            else:
                self.jobtypeRegistered[jobtype] = 2
                msg = 'Warning: file '+ register_boss_jobtype + ' does not exist!\n'
                msg = msg + 'Will be used only JOB as default jobtype\n'
                common.logger.message(msg)
                return
        self.jobtypeRegistered[jobtype] = 1
        return


    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return self.boss_scheduler.wsSetupEnvironment() 


    def createSchScript(self, nj):
        """
        Create script_scheduler file (JDL for EDG)
        """

        self.boss_scheduler.createSchScript(nj)
        self.declareJob_(nj)
        return


    def declareJob_(self, nj):
        """
        BOSS declaration of jobs
        """
        
        # copy of original file
        sch_script_orig = common.job_list[nj].jdlFilename()
        sch_script = sch_script_orig+"_boss"
        shutil.copyfile(sch_script_orig, sch_script)
        
        # BOSS job declaration
        dir = string.split(common.work_space.topDir(), '/')
        sch = open(sch_script, 'a')
        if (self.jobtypeRegistered[self.boss_jobtype] == 2):
            types = 'jobtype=crabjob'
        else:
            types = 'jobtype=crabjob,'+self.boss_jobtype  
        boss_scheduler_name = string.lower(self.boss_scheduler.name())
        # da decidere se lasciare come if ...
        if (self.jobtypeRegistered[self.boss_scheduler_name] != 2):
            types += ','+self.boss_scheduler_name
        sch.write(types+';\n')            
        #print "types = ", types
        sch.write('group='+ self.groupName +';\n')
        sch.write('BossAttr=[')
        sch.write('crabjob.INTERNAL_ID=' + str(nj+1) + ';')
        sch.write('];\n')
        sch.close()
 
        dirlog = common.work_space.logDir()
        scriptName=os.path.basename(common.job_list[nj].scriptFilename())

        cmd = 'boss declare -group '+ self.groupName +' -classad '+ sch_script +' -log '+ dirlog + scriptName + '.log'       
        msg = 'BOSS declaration:' + cmd
        common.logger.debug(5,msg)
        cmd_out = runBossCommand(cmd)
        prefix = 'Job ID '
        index = string.find(cmd_out, prefix)
        if index < 0 :
            common.logger.message('ERROR: BOSS declare failed: no BOSS ID for job')
            raise CrabException(msg)
        else :
            index = index + len(prefix)
            boss_id = string.strip(cmd_out[index:])
            common.jobDB.setBossId(nj, boss_id)
            common.logger.debug(5,"BOSS ID =  "+boss_id)
        os.remove(sch_script)
        return 


    def checkProxy(self):
        """
        Check the Globus proxy. 
        """
        return self.boss_scheduler.checkProxy()


    def loggingInfo(self, nj):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        return self.boss_scheduler.loggingInfo(nj) 

    def listMatch(self, nj):
        """
        Check the compatibility of available resources
        """
        return self.boss_scheduler.listMatch(nj)

    def submit(self, nj):
        """
        Submit BOSS function.
        Submit one job. nj -- job number.
        """

        boss_scheduler_name = string.lower(self.boss_scheduler.name())
        boss_scheduler_id = None

        schcladstring = ''
        
        if self.schclassad != '':
            schcladstring=' -schclassad '+self.schclassad
        cmd = 'boss submit -scheduler '+boss_scheduler_name+schcladstring+' -jobid '+common.jobDB.bossId(nj)
        msg = 'BOSS submission: ' + cmd
        common.logger.debug(4,msg)
        cmd_out = runBossCommand(cmd)

        if not cmd_out :
            msg = 'ERROR: BOSS submission failed: ' + cmd
            common.logger.message(msg)
            return None
        else:
            if cmd_out.find('https') != -1:
                reSid = re.compile( r'https.+' )
                jid = reSid.search(cmd_out).group()
            else :
                jid = cmd_out.split()[2]
            return jid


    # fede nuova funzione
    def resubmit(self, nj_list):
        """
        Prepare jobs to be submit
        """
        for nj in nj_list:
            self.declareJob_(int(nj)) 
        return


    def moveOutput(self, int_id):
        """
        Move output of job already retrieved 
        """
        self.current_time = time.strftime('%y%m%d_%H%M%S',time.localtime(time.time()))
        resDir = common.work_space.resDir()
        resDirSave = resDir + self.current_time
        if not os.path.exists(resDirSave):
            os.mkdir(resDirSave)

        boss_id = self.listBoss()[int(int_id)]

        cmd = 'boss SQL -query "select OUT_FILES from JOB where JOB.ID='+str(boss_id)+'"'
        cmd_out = runBossCommand(cmd)

        nline = 0
        for line in cmd_out.splitlines():
            if nline == 2:
                files = line.split(',')
                for i in files:
                    i=i.strip()
                    i=i.strip('{}')
                    i=i.strip()
                    i=i.strip('"')
                                    
                    if os.path.exists(self.outDir+'/'+i):
                        os.rename(self.outDir+'/'+i, resDirSave+'/'+i)
                        common.logger.message('Output file '+i+' moved to '+resDirSave)

                    if os.path.exists(self.logDir+'/'+i):
                        os.rename(self.logDir+'/'+i, resDirSave+'/'+i)
                        common.logger.message('Output file '+i+' moved to '+resDirSave)
            nline = nline + 1
        return


    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """

        return self.boss_scheduler.queryDetailedStatus(id)


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
#        dirGroup = string.split(common.work_space.topDir(), '/')
#        group = dirGroup[len(dirGroup)-2]
#        group = self.groupName
        allBoss_id = common.scheduler.listBoss()
        for i_id in int_id :
            if int(i_id) not in allBoss_id.keys(): 
                msg = 'Job # '+`int(i_id)`+' out of range for task '+ self.groupName
                common.logger.message(msg) 
            else: 
                dir = self.outDir 
                logDir = self.logDir
                boss_id = allBoss_id[int(i_id)] 
                cmd = 'bossSid '+str(boss_id)
                cmd_out = runCommand(cmd) 
                if common.scheduler.queryStatus(boss_id) == 'Done (Success)' or common.scheduler.queryStatus(boss_id) == 'Done (Abort)':   
                    cmd = 'boss getOutput -jobid '+str(boss_id) +' -outdir ' +dir 
                    cmd_out = runBossCommand(cmd)
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
                    exCode = common.scheduler.getExitStatus(jid)
                    Statistic.Monitor('retrieved',resFlag,jid,exCode)
                    common.jobDB.setStatus(int(i_id)-1, 'Y') 
                else:
                    msg = 'Job # '+`int(i_id)`+' has status '+common.scheduler.queryStatus(boss_id)+' not possible to get output'
                    common.logger.message(msg)
                dir += os.getlogin()
                dir += '_' + os.path.basename(str(boss_id))
            pass
        common.jobDB.save() 
        return dir   


    def cancel(self,int_id):
        """ Cancel the EDG job with id """
        common.jobDB.load() 
#        dirGroup = string.split(common.work_space.topDir(), '/')
#        group = dirGroup[len(dirGroup)-2] 
#        group = self.groupName
        allBoss_id = common.scheduler.listBoss() 
        for i_id in int_id :
            if int(i_id) not in allBoss_id.keys():
                msg = 'Job # '+`(i_id)`+' out of range for task '+self.groupName
                common.logger.message(msg)
            else:
                boss_id = allBoss_id[int(i_id)]
                status =  common.scheduler.queryStatus(boss_id) 
                if status == 'Done (Success)' or status == 'Aborted(BOSS)' or status == 'Killed(BOSS)' or status =='Cleared(BOSS)' or status ==  'Done (Aborted)' or status == 'Created(BOSS)':
                    msg = 'Job # '+`int(i_id)`+' has status '+status+' not possible to Kill it'
                    common.logger.message(msg) 
                else:
                    cmd = 'boss kill -jobid '+str(boss_id) 
                    cmd_out = runBossCommand(cmd)
                    common.jobDB.setStatus(int(i_id)-1, 'K')
                    common.logger.message("Killing job # "+`int(i_id)`)
                    pass
                pass
        common.jobDB.save()
        return #cmd_out    

    def getAttribute(self, id, attr):
        return self.boss_scheduler.getStatusAttribute_(id, attr)

    def getExitStatus(self, id):
        return self.boss_scheduler.getStatusAttribute_(id, 'exit_code')

    def queryDest(self, id):  
        return self.boss_scheduler.getStatusAttribute_(id, 'destination')

    def wsCopyInput(self):
        return self.boss_scheduler.wsCopyInput()

    def wsCopyOutput(self):
        return self.boss_scheduler.wsCopyOutput()

    def wsRegisterOutput(self):  
        return self.boss_scheduler.wsRegisterOutput()

    def boss_ID(self,int_ID):
        """convert internal ID into Boss ID """ 

        cmd = 'boss SQL -query "select JOB.ID from JOB,crabjob where crabjob.JOBID=JOB.ID and crabjob.INTERNAL_ID='+str(int_ID)+'"'
        cmd_out = runBossCommand(cmd)
        nline = 0
        for line in cmd_out.splitlines():
            if nline == 2:
               boss_ID = line
            nline = nline + 1
              
        return boss_ID 
     
    def boss_SID(self,int_ID):
        """ Return Sid of job """
                                                                                                                             
#        dirGroup = string.split(common.work_space.topDir(), '/')
#        group = self.groupName 

        cmd = 'boss SQL -query "select JOB.SID  from JOB,crabjob where crabjob.JOBID=JOB.ID and JOB.GROUP_N=\''+self.groupName+'\' and crabjob.INTERNAL_ID='+str(int_ID)+'"'
        cmd_out = runBossCommand(cmd)
        nline = 0
        for line in cmd_out.splitlines():
            if nline == 2:
               SID = string.strip(line)
            nline = nline + 1
    
        return SID

    def queryStatus(self,id):
        """ Query a status of the job with id """
                                                                                                                             
        self.boss_scheduler.checkProxy()
        EDGstatus={
            'H':'Hold(Condor)',
            'U':'Ready(Condor)',
            'I':'Scheduled(Condor)',
            'X':'Cancelled(Condor)',
            'W':'Created(BOSS)',
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
            'A?':'Aborted(BOSS)',
            'K':'Killed(BOSS)',
            'E':'Cleared(BOSS)',
            'NA':'Unknown(BOSS)',
            'I?':'Idle(BOSS)',
            'O?':'Done(BOSS)',
            'R?':'Running(BOSS)'             
            }
        cmd = 'boss q -statusOnly -jobid '+str(id)
        cmd_out = runBossCommand(cmd)
        js = cmd_out.split(None,2)
        return EDGstatus[js[1]]

    def listBoss(self):
        """
        Return a list of all boss_Id of a task
        """
        ListBoss_ID = {}
        cmd = 'boss SQL -query "select crabjob.INTERNAL_ID, JOB.ID from JOB,crabjob where crabjob.JOBID=JOB.ID and JOB.GROUP_N=\''+self.groupName+'\'"'

        cmd_out = runBossCommand(cmd,0)
        nline = 0
        for line in cmd_out.splitlines():
            if nline != 0 and nline != 1:
                (internal_Id, boss_Id) = string.split(line)
                ListBoss_ID[int(internal_Id)]=int(boss_Id)
            nline = nline + 1
        return ListBoss_ID 

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

