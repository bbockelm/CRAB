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


    def configure(self, cfg_params):
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
        if (self.boss_scheduler.sched_parameter()):
           self.schclassad = common.work_space.shareDir()+'/'+self.boss_scheduler.param
 
        # check scheduler and jobtype registration in BOSS
        self.checkSchedRegistration_(self.boss_scheduler_name)
        self.checkJobtypeRegistration_(self.boss_jobtype) 
        
        return


    def checkSchedRegistration_(self, sched_name): 
        """
        Verify scheduler registration.
        """
        boss_scheduler_check = "boss showSchedulers"
        boss_out = runCommand(boss_scheduler_check)
        if string.find(boss_out, sched_name) == -1 :
            msg = sched_name + ' scheduler not registered in BOSS\n'
            msg = msg + 'Starting registration\n'
            common.logger.message(msg)
            # qui bisogna decidere un path che non deve cambiare con le versioni diverse di boss 
            # e se conviene fare noi la registrazione!!!!
            register_boss_scheduler = self.boss_dir + '/BossSched/bin/register'+ string.upper(sched_name) + 'Scheduler'
            if os.path.exists(register_boss_scheduler):
                runCommand(register_boss_scheduler)
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
            msg = msg + 'Starting registration (in the future...)\n'
            common.logger.message(msg)
            #register_boss_jobtype= self.boss_dir + '/jobtest/register'+string.upper(jobtype)+'Job'
            #if os.path.exists(register_boss_jobtype):
            #    runCommand(register_boss_jobtype)
            #else:
            #    msg = 'Warning: file '+ register_boss_jobtype + ' does not exist!\n'
            #    msg = msg + 'Will be used only JOB as default jobtype\n'
            #    common.logger.message(msg)
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
        sch_script = common.job_list[nj].jdlFilename() 

        # BOSS job declaration
        dir = string.split(common.work_space.topDir(), '/')
        # da decidere se questo valore va bene come group ...
        cmd = 'boss declare -group '+ dir[len(dir)-2] +' -classad '+ sch_script
        msg = 'BOSS declaration:' + cmd
        common.logger.message(msg)
        cmd_out = runCommand(cmd)
        # speriamo che l'output di BOSS non cambi ....
        prefix = 'Job ID '
        index = string.find(cmd_out, prefix)
        if index < 0 :
            common.logger.message('ERROR: BOSS declare failed: no BOSS ID for job')
            raise CrabException(msg)
        else :
            index = index + len(prefix)
            boss_id = string.strip(cmd_out[index:])
            common.jobDB.setBossId(nj, boss_id)
            print "BOSS ID = ", boss_id
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

        cmd = 'boss submit -scheduler '+boss_scheduler_name+' -schclassad '+self.schclassad+' -jobid '+common.jobDB.bossId(nj)
        msg = 'BOSS submission: ' + cmd
        common.logger.message(msg)
        cmd_out = runCommand(cmd)

        if not cmd_out : 
           msg = 'ERROR: BOSS submission failed: ' + cmd
           common.logger.message(msg)
           return  boss_scheduler_id

        # speriamo che l'output di BOSS non cambi ....
        prefix = 'Scheduler ID is '
        index = string.find(cmd_out, prefix)
        if index < 0 :
            common.log.message('ERROR: BOSS submission failed: no BOSS Scheduler ID')
            return boss_scheduler_id
        else :
            index = index + len(prefix)
            boss_scheduler_id = string.strip(cmd_out[index:])
            index = string.find(boss_scheduler_id,'\n')
            boss_scheduler_id = string.strip(boss_scheduler_id[:index])
            print "BOSS Scheduler ID = ", boss_scheduler_id
        return boss_scheduler_id

    def queryStatus(self, id):
        """ Query a status of the job with id """

        return self.boss_scheduler.queryStatus(id)

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """

        return self.boss_scheduler.queryDetailedStatus(id)

    def getOutput(self, id):
        """
        Get output for a finished job with id.
        Returns the name of directory with results.
        """
        return self.boss_scheduler.getOutput(id) 

    def cancel(self, id):
        """ Cancel the EDG job with id """

        return self.boss_scheduler.cancel(id)

    def getExitStatus(self, id):
        return self.boss_scheduler.getStatusAttribute_(id, 'exit_code')

    def queryStatus(self, id):
        return self.boss_scheduler.getStatusAttribute_(id, 'status')

    def queryDest(self, id):  
        return self.boss_scheduler.getStatusAttribute_(id, 'destination')
