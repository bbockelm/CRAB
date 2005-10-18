from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import os, sys, tempfile

class SchedulerBoss(Scheduler):
    def __init__(self):
        Scheduler.__init__(self,"BOSS")
        self.checkBoss_()
        self.cwd = common.work_space.cwdDir()
        return


    def checkBoss_(self): 
        """
        Verify BOSS installation.
        """
        try:
            self.boss_dir = os.environ["BOSSDIR"]
        except:
            msg = "Error: the BOSSDIR is not set."
            msg = msg + " Did you source bossenv.sh/csh from your BOSS area?\n"
            raise CrabException(msg)


    def configRT_(self): 
        """
        Configure Boss RealTime monitor
        """

        # First I have to create a SQLiteConfig.clad file in the proper directory
        cwd = os.getcwd()
        os.chdir(common.work_space.shareDir())
        confSQLFileName = 'MySQLRTConfig.clad'
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
            msg = 'Warning: file '+ register_script + 'does not exist!\n'
            raise CrabException(msg)
        
        os.chdir(cwd)

        return

    def configRT_(self): 
        """
        Configure Boss RealTime monitor
        """

        # First I have to create a SQLiteConfig.clad file in the proper directory
        cwd = os.getcwd()
        os.chdir(common.work_space.shareDir())
        confSQLFileName = 'MySQLRTConfig.clad'
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
            msg = 'Warning: file '+ register_script + 'does not exist!\n'
            raise CrabException(msg)
        
        os.chdir(cwd)

        return

    def configure(self, cfg_params):
         
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
<<<<<<< SchedulerBoss.py
=======

>>>>>>> 1.13
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
        boss_scheduler_check = "boss showSchedulers"
        boss_out = runCommand(boss_scheduler_check)
        if string.find(boss_out, sched_name) == -1 :
            msg = sched_name + ' scheduler not registered in BOSs\n'
            msg = msg + 'Starting registration\n'
            common.logger.message(msg)
            # qui bisogna decidere un path che non deve cambiare con le versioni diverse di boss 
            # e se conviene fare noi la registrazione!!!!
            register_boss_scheduler = self.cwd + 'BossScript/register'+ string.upper(sched_name) + 'Scheduler'
            if os.path.exists(register_boss_scheduler):
                boss_out = runCommand(register_boss_scheduler)
                if string.find(boss_out, 'Usage') != -1 :
                    msg = 'Error: Problem with scheduler '+sched_name+' registration\n'
                    raise CrabException(msg)
            else:
                msg = 'Warning: file '+ register_boss_scheduler + 'does not exist!\n'
                msg = msg + 'Please create your scheduler plugins\n'
                raise CrabException(msg)
        return


    def checkJobtypeRegistration_(self, jobtype): 
        """
        Verify jobtype registration.
        """
        boss_jobtype_check = "boss showJobTypes"
        boss_out = runCommand(boss_jobtype_check)
        if string.find(boss_out, jobtype) == -1 :
            msg =  'Warning:' + jobtype + ' jobtype not registered in BOSS\n'
            msg = msg + 'Starting registration \n'
            common.logger.message(msg)
            register_boss_jobtype= self.cwd + 'BossScript/register' + string.upper(jobtype) + 'job'
            if os.path.exists(register_boss_jobtype):
                boss_out = runCommand(register_boss_jobtype)
                if string.find(boss_out, 'Usage') != -1 :
                    msg = 'Error: Problem with job '+jobtype+' registration\n'
                    raise CrabException(msg)
            else:
                msg = 'Warning: file '+ register_boss_jobtype + ' does not exist!\n'
                msg = msg + 'Will be used only JOB as default jobtype\n'
                common.logger.message(msg)
        return


    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return self.boss_scheduler.wsSetupEnvironment() 


    def createSchScript(self, nj):
        """
        Return script_scheduler file (JDL for EDG)
        """

        self.boss_scheduler.createSchScript(nj)
        sch_script_orig = common.job_list[nj].jdlFilename()
        sch_script = sch_script_orig+"_boss"
        cmd = 'cp '+sch_script_orig+' '+sch_script
        runCommand(cmd)
        
        # BOSS job declaration
        dir = string.split(common.work_space.topDir(), '/')
        sch = open(sch_script, 'a')
        types = 'jobtype=crabjob,orca'
        boss_scheduler_name = string.lower(self.boss_scheduler.name())
        # da decidere se lasciare come if ...
        if boss_scheduler_name == 'edg' :
            types += ',edg'
        sch.write(types+';\n')            
        # da decidere se questo valore va bene come group ...
        sch.write('group='+ dir[len(dir)-2]+';\n')
        sch.write('BossAttr=[')
        sch.write('crabjob.INTERNAL_ID=' + str(nj+1) + ';')
        sch.write('];\n')
        sch.close()
 
        dirlog = common.work_space.logDir()
  
        cmd = 'boss declare -group '+ dir[len(dir)-2] +' -classad '+ sch_script +' -log '+ dirlog + 'ORCA.sh_'+str(nj+1)+'.log'       
  
        msg = 'BOSS declaration:' + cmd
        common.logger.message(msg)
        cmd_out = runCommand(cmd)
        print 'cmd_out', cmd_out
        prefix = 'Job ID '
        index = string.find(cmd_out, prefix)
        if index < 0 :
            common.logger.message('ERROR: BOSS declare failed: no BOSS ID for job')
            raise CrabException(msg)
        else :
            index = index + len(prefix)
            boss_id = string.strip(cmd_out[index:])
            common.jobDB.setBossId(nj, boss_id)
            print "BOSS ID =  ", boss_id
        cmd = 'rm -f '+sch_script
        runCommand(cmd)
        return 


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
        common.logger.message(msg)
        cmd_out = runCommand(cmd)

        if not cmd_out : 
           msg = 'ERROR: BOSS submission failed: ' + cmd
           common.logger.message(msg)
           return  boss_scheduler_id

        prefix = 'Scheduler ID is '
        index = string.find(cmd_out, prefix)
        if index < 0 :
            common.logger.message('ERROR: BOSS submission failed: no BOSS Scheduler ID')
            return boss_scheduler_id
        else :
            index = index + len(prefix)
            boss_scheduler_id = string.strip(cmd_out[index:])
            #print "boss_scheduler_id", boss_scheduler_id
            #index = string.find(boss_scheduler_id,'\n')
            #print "index di a capo", index
            #boss_scheduler_id = string.strip(boss_scheduler_id[:index])
            print "BOSS Scheduler ID = ", boss_scheduler_id
        return boss_scheduler_id


    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """

        return self.boss_scheduler.queryDetailedStatus(id)


    def getOutput(self, int_id):
        """
        Get output for a finished job with id.
        Returns the name of directory with results.
        """
        dirGroup = string.split(common.work_space.topDir(), '/')
        group = dirGroup[len(dirGroup)-2]
        dir = common.work_space.resDir()
        for i_id in int_id :
            if (i_id) not in common.scheduler.listBoss(): 
                msg = 'Job # '+`(i_id+1)`+' out of range for task '+group
                common.logger.message(msg) 
            else: 
                dir,logDir = setOutLogDir(self.outDir,self.logDir)
                boss_id =  common.scheduler.boss_ID((i_id +1),group)
                if common.scheduler.queryStatus(boss_id) == 'Done (Success)' or common.scheduler.queryStatus(boss_id) == 'Done (Abort)':   
                    cmd = 'boss getOutput -jobid '+str(boss_id) +' -outdir ' +dir 
                    cmd_out = runCommand(cmd)
                    if logDir != dir:
                        cmd = 'mv '+str(dir)+'/*'+str(i_id+1)+'.std* '+str(dir)+'/.BrokerInfo '+str(dir)+'/*'+str(i_id+1)+'.log '+str(logDir)
                        cmd_out = runCommand(cmd) 
                        pass
                    msg = 'Results of Job # '+`(i_id+1)`+' are in '+dir
                    common.logger.message(msg)
                else:
                    msg = 'Job # '+`(i_id+1)`+' has status '+common.scheduler.queryStatus(boss_id)+' not possible to get output'
                    common.logger.message(msg)
                dir += os.getlogin()
                dir += '_' + os.path.basename(boss_id)
        
        return dir   


    def cancel(self, id):
        """ Cancel the EDG job with id """

        return self.boss_scheduler.cancel(id)

    def getExitStatus(self, id):

        return self.boss_scheduler.getStatusAttribute_(id, 'exit_code')

    def queryDest(self, id):  
        return self.boss_scheduler.getStatusAttribute_(id, 'destination')

    #### FEDE 
    def wsCopyOutput(self):
        return self.boss_scheduler.wsCopyOutput()

    def wsRegisterOutput(self):  
        return self.boss_scheduler.wsRegisterOutput()

    def boss_ID(self,int_ID,group):
        """convert internal ID into Boss ID """ 
        
        cmd = 'boss SQL -query "select JOB.ID  from JOB,crabjob where crabjob.JOBID=JOB.ID and JOB.GROUP_N=\''+group+'\' and crabjob.INTERNAL_ID='+str(int_ID)+'"'
        cmd_out = runCommand(cmd)
        nline = 0
        for line in cmd_out.splitlines():
            if nline == 2:
               boss_ID = line
            nline = nline + 1
              
        return boss_ID 


    def queryStatus(self,id):
        """ Query a status of the job with id """
                                                                                                                             
        EDGstatus={
            'W':'Created(BOSS)',
            'R':'Running',
            'SC':'Checkpointed',
            'SS':'Scheduled',
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
            'A':'Aborted(BOSS)',
            'K':'Killed(BOSS)',
            'E':'Cleared(BOSS)',
            'NA':'Unknown(BOSS)'
            }
        cmd = 'boss q -statusOnly -jobid '+id
        cmd_out = runCommand(cmd)
        js = cmd_out.split(None,2)
        return EDGstatus[js[1]]

    def listBoss(self):
        """
         Return a list of all boss_Id of a task
                                                                                                                             
        """
        dirGroup = string.split(common.work_space.topDir(), '/')
        group = dirGroup[len(dirGroup)-2]
        ListBoss_ID = []
        cmd = 'boss SQL -query "select crabjob.INTERNAL_ID from JOB,crabjob where crabjob.JOBID=JOB.ID and JOB.GROUP_N=\''+group+'\'"'
        cmd_out = runBossCommand(cmd,0)
        nline = 0
        for line in cmd_out.splitlines():
            if nline != 0 and nline != 1:
                ListBoss_ID.append(int(line)-1) 
            nline = nline + 1

        return ListBoss_ID
