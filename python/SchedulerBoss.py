from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import os, time, shutil
import Scram

from BossSession import *

class SchedulerBoss(Scheduler):
    def __init__(self):
        Scheduler.__init__(self,"BOSS")
        self.checkBoss_()
        self.schedRegistered = {}
        self.jobtypeRegistered = {}
        self.bossLogFile = "boss.log"

        # Map for Boss Status to Human Readable Status
        self.status={
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
            'S' :'Submitted (Boss)',
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
        configClad = common.work_space.shareDir()+"/BossConfig.clad"
        self.bossConfigDir = str(common.work_space.shareDir())
        if ( not os.path.exists(configClad)  ) :
            bossCfg = os.environ["HOME"]+"/.bossrc/BossConfig.clad"
            shutil.copyfile(bossCfg,configClad)
        self.boss_db_name = 'bossDB'
        if os.path.isfile(self.bossConfigDir+self.boss_db_name) :
            common.logger.debug(6,'BossDB already exist')
        else:
            common.logger.debug(6,'Creating BossDB in '+self.bossConfigDir+self.boss_db_name)

            # First I have to create a SQLiteConfig.clad file in the proper directory
            if not os.path.exists(self.bossConfigDir):
                os.mkdir(self.bossConfigDir)
            confSQLFileName = 'SQLiteConfig.clad'
            confFile = open(self.bossConfigDir+'/'+confSQLFileName, 'w')
            confFile.write('[\n')
            confFile.write('SQLITE_DB_PATH = "'+self.bossConfigDir+'";\n')
            confFile.write('DB_NAME = "'+self.boss_db_name+'";\n')
            confFile.write('DB_TIMEOUT = 300;\n')
            confFile.write(']\n')
            confFile.close()

            # then I have to run "bossAdmin configureDB"
#            out = runBossCommand('bossAdmin configureDB',0)
     
        return

    ###################### ---- OK for Boss4 ds
    def configRT_(self, bossAdmin): 
        """
        Configure Boss RealTime monitor
        """

        # check if RT is already configured
        boss_rt_check = []
        try:
            boss_rt_check = self.bossUser.RTMons()
        except:
            pass
   #     boss_rt_check = self.bossUser.defaultRTmon()
        if 'mysql' not in boss_rt_check:
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
            register_script = "MySQLRTMon.xml"
            register_path = self.boss_dir + '/'
            if os.path.exists(register_path+register_script):
                try :
                    bossAdmin.registerPlugins( register_path+register_script )
                except BossError,e:
                    common.logger.debug( 4, e.__str__() )
                    msg = 'Problem with RealTime monitor registration\n'
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
        
        self.cfg_params = cfg_params
        
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
            
        self.bossConfigDir = str("")
#       central db
        whichBossDb=0
        try:
            whichBossDb=int(cfg_params["USER.use_central_bossdb"])
        except KeyError:
            pass

        if ( whichBossDb == 1 ):
            pass
#       emulate -c option        
        elif ( whichBossDb == 2 ):
            self.bossConfigDir = str(cfg_params["USER.boss_clads"])
        else:
            self.configBossDB_()

        self.bossUser =BossSession(self.bossConfigDir, "0", common.work_space.logDir()+'/crab.log')       
       # self.bossUser.showConfigs()
        taskid = ""
        try:
            taskid = common.taskDB.dict('BossTaskId')
        except :
            pass
        self.bossTask = self.bossUser.makeBossTask(taskid)

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
  
        bossAdmin =  BossAdministratorSession(self.bossConfigDir, "0", common.work_space.logDir()+'/crab.log')
 
        try:
            self.bossUser.clientID() # BOSS DB already setup
        except:
            try:
                if (int(cfg_params["USER.use_central_bossdb"])==0):
                    if ( self.bossTask.id() == "" ) :
                        bossAdmin.configureDB()
            except KeyError:
                bossAdmin.configureDB()
            pass

        # check scheduler and jobtype registration in BOSS        
        try:
            if (int(cfg_params["USER.use_boss_rt"])==1): self.configRT_(bossAdmin)
        except KeyError:
            pass

        self.checkSchedRegistration_(self.boss_scheduler_name, bossAdmin)
        self.checkJobtypeRegistration_(self.boss_jobtype, bossAdmin) 
        self.checkJobtypeRegistration_('crab', bossAdmin)
        # ONLY SQLITE!!! if DB has changed, the connection needs a reset
        self.bossUser.resetDB()
        
        try: self.schedulerName = cfg_params['CRAB.scheduler']
        except KeyError: self.scheduler = ''

        return

    ###################### ---- OK for Boss4 ds
    def checkSchedRegistration_(self, sched_name, bossAdmin): 
        """
        Verify scheduler registration.
        """
        ## we don't need to test this at every call:
        if (self.schedRegistered.has_key(sched_name)): return

        try :
            register_path = self.boss_dir + '/'
            register_boss_scheduler = string.upper(sched_name) + '.xml'
            bossAdmin.registerPlugins( register_path+register_boss_scheduler )
        except BossError,e:
            msg = e.__str__() + '\nError: Problem with scheduler '+sched_name+' registration\n'
            raise CrabException(msg)
        
        # sched registered
        self.schedRegistered[sched_name] = 1
        return


    ###################### ---- OK for Boss4 ds
    def checkJobtypeRegistration_(self, jobtype, bossAdmin): 
        """
        Verify jobtype registration.
        """
        ## we don't need to test this at every call:
        if (self.jobtypeRegistered.has_key(jobtype)): return

        try :
            register_path = self.boss_dir + '/'
            register_boss_jobtype = string.upper(string.upper(jobtype)) + '.xml'
            bossAdmin.registerPlugins( register_path+register_boss_jobtype )
        except BossError,e:
            msg = e.__str__() + '\nError: Problem with jobtype '+jobtype+' registration\n'
            raise CrabException(msg)
        
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
        try:
            start = time.time()
            self.bossTask.declare(common.work_space.shareDir()+'/'+self.boss_jobtype+'.xml')
            stop = time.time()
            # debug
            msg = 'BOSS declaration:' + common.work_space.shareDir()+self.boss_jobtype+'.xml'
            common.logger.debug(4,msg)
            common.logger.write(msg)
            msg = 'BOSS declaration took ' +str(stop-start)
            common.logger.debug(1,msg)
            common.logger.write(msg)
        ###
            self.Task_id = self.bossTask.id()
            common.taskDB.setDict('BossTaskId',self.Task_id)
            common.logger.debug(4,"TASK ID =  "+self.Task_id)
            common.logger.write("TASK ID =  "+self.Task_id)
     
            # job counter, jobs in JobDB run from 0 - n-1
            num_job = 0
            task = self.bossTask.jobsDict()
#            for k, v in task.iteritems():
            for k in range(len(task)):
                common.jobDB.setBossId(num_job, str(k + 1))
                common.logger.debug(4,"CHAIN ID =  "+ str(k + 1) +" of job: "+str(num_job))
                num_job += 1
        except BossError,e:
            common.logger.message(e.__str__())
            raise CrabException(e.__str__())

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
            schcladstring=self.schclassad

        Tout = 120
        CEs=[]
        try:
            CEs=self.bossUser.schedListMatch( str(self.schedulerName), schcladstring, self.bossTask.id(), "", Tout)
            common.logger.debug(1,"CEs :"+str(CEs))
        except SchedulerError,e:
            common.logger.message( "Warning : Scheduler interaction in list-match operation failed for jobs:")
            common.logger.message( e.__str__())
            pass
        except BossError,e:
            raise CrabException("ERROR: listMatch failed with message" + e.__str__())
        stop = time.time()
        common.logger.debug(1,"listMatch time :"+str(stop-start))
        common.logger.write("listMatch time :"+str(stop-start))
        # print something about CEs
        sites = []
        if (len(CEs)!=0):
            #sites = []
            for it in CEs :
                it = it.split(':')[0]
                if not sites.count(it) :
                    sites.append(it)
            common.logger.debug(5,"All Sites :"+str(sites))
            common.logger.message("Matched Sites :"+str(sites))
        if len(sites) == 0:
            common.logger.message("No matched Sites :"+str(sites))
            common.logger.message("Printing info about CE close to SE storing data to analize")
            version = Scram.Scram(self.cfg_params).getSWVersion()
            SEs = common.jobDB.destination(nj)
            for i in SEs:
                common.logger.message("\nSE = " + i)
                cmd = "lcg-info --vo cms --list-se --attrs CloseCE --query \"SE=*" + i + "*\""
                fout, fin, ferr = popen2.popen3(cmd)
                try:
                    res = string.strip(fout.readlines()[1])
                    ce = re.match(".*CloseCE(.*):.*", res).group(1).strip()
                    common.logger.message("CloseCE = " + ce)
                except IndexError:
                    common.logger.message("No CloseCE info available") 
                    continue
                cmd = "lcg-info --vo cms --list-ce --attrs CEStatus,Tag --query \"CE=*" + ce + "*,Tag=*" + version + "*\""
                fout, fin, ferr = popen2.popen3(cmd)
                try:
                    res = fout.readlines()
                    b=re.findall(r"\'VO-cms-slc\d_ia\d+_gcc\d+\'",str(map(lambda x: x.strip(), res)))
                    print "\033[1;35m CloseCE %s status: \n %s "%(ce, str(res[1]))
                    print "\033[1;35m Software Tag %s available"%version
                    print "\033[1;35m Software Architecture set to %s"%str(b)
                    common.logger.write("Software Tag %s available "%version)
                    print "\033[0m"
                except IndexError:
                    print "\033[1;35m Software Tag %s not found on CloseCE %s"%(version, ce)
                    common.logger.write("Software Tag %s not found on CloseCE"%version)
                    print "\033[0m"
                    pass
            common.logger.write("\n")
        return len(sites)

    
    def submit(self,list):
        """
        Submit BOSS function.
        Submit one job. nj -- job number.
        """

        try:
            i = list[0]
        except IndexError:
            common.logger.message("No sites where to submit jobs")
            pass 
        jobsList = list[1]
        schcladstring = ''
        self.schclassad = common.work_space.shareDir()+'/'+'sched_param_'+str(i)+'.clad'# TODO add a check is file exist
        if os.path.isfile(self.schclassad):  
            schcladstring=self.schclassad
        try:
#            Tout = len(list[1])*120
            Tout = int(self.boss_scheduler.submitTout(list))
            self.bossTask.submit(string.join(jobsList,','), schcladstring, "", "" , "", Tout)
        except SchedulerError,e:
            common.logger.message("Warning : Scheduler interaction in submit operation failed for jobs:")
            common.logger.message(e.__str__())
            pass
        except BossError,e:
            common.logger.message("Error : BOSS command failed with message:")
            common.logger.message(1, e.__str__())
        
        jid=[]
        bjid = []
        self.bossTask.clear()
        range = str(jobsList[0]) + ":" + str(jobsList[len(jobsList) - 1])
        try:
            self.bossTask.load(ALL, range)
        except SchedulerError,e:
            common.logger.message("Warning : Scheduler interaction in query operation failed for jobs:")
            common.logger.message(e.__str__())
            pass
        except BossError,e:
            common.logger.message("Error : BOSS command failed with message:")
            common.logger.message(1, e.__str__())
        task = self.bossTask.jobsDict()
        for k, v in task.iteritems():
            if (v["STATUS"] != 'W'):
                jid.append(v["SCHED_ID"])
                bjid.append(k)
            pass
          #  if (v["STATUS"] == 'S'):
          #      jid.append(v["SCHED_ID"])
          #      bjid.append(k)
          #  pass
        return jid, bjid

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

        boss_id = str(int_id)
        try:
            self.bossTask.load (ALL, boss_id )
            cmd_out = self.bossTask.program(boss_id, '1')['OUTFILES']
        except BossError,e:
            common.logger.message( e.__str__() )
        
        files = cmd_out.split(',')
        for i in files:
            if os.path.exists(self.outDir+'/'+i):
                shutil.move(self.outDir+'/'+i, resDirSave+'/'+i+'_'+self.current_time)
                common.logger.message('Output file '+i+' moved to '+resDirSave)

            if os.path.exists(self.logDir+'/'+i):
                shutil.move(self.logDir+'/'+i, resDirSave+'/'+i+'_'+self.current_time)
                common.logger.message('Output file '+i+' moved to '+resDirSave)
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
        statusList = self.queryStatusList(bossTaskId, int_id)
        check = 0

        ## then loop over jobs and retrieve it if it's the case
        create= []
        run= []
        clear=[]
        abort=[]
        canc=[]
        read=[]
        wait=[]
        sched=[]
        kill=[]
        other=[]

        for i_id in int_id :
            if i_id not in allBoss_id:
                msg = 'Job # '+`int(i_id)`+' out of range for task '+ self.groupName
                common.logger.message(msg) 
            else:
                dir = self.outDir 
                logDir = self.logDir
                boss_id = i_id 
                #bossTaskIdStatus = common.scheduler.queryStatus(bossTaskId, boss_id)
                bossTaskIdStatus = statusList[boss_id]
                if bossTaskIdStatus == 'Done (Success)' or bossTaskIdStatus == 'Done (Abort)':   
                    check = 1
                    try:
                        Tout =120 
                        self.bossTask.getOutput (str(boss_id), str(dir), Tout)
                        if logDir != dir:
                            try:
                                toMove = str(dir)+'/*'+`int(i_id)`+'.std* '+str(dir)+'/*.log '+str(dir)+'/.BrokerInfo ' 
                                shutil.move(toMove, str(logDir))
                                msg = 'Results of Job # '+`int(i_id)`+' are in '+dir+' (log files are in '+logDir+')' 
                                common.logger.message(msg)
                            except:
                                msg = 'Problem with copy of job results' 
                                common.logger.message(msg)
                                pass  
                        else:   
                            msg = 'Results of Job # '+`int(i_id)`+' are in '+dir
                            common.logger.message(msg)
                        # resFlag = 0
                        # jid = common.scheduler.boss_SID(int(i_id)) 
                        #  try:
                        #      exCode = common.scheduler.getExitStatus(jid)
                        #  except:
                        #      exCode = ' '
                        # Statistic.Monitor('retrieved',resFlag,jid,exCode,'dest')
                        common.jobDB.setStatus(int(i_id)-1, 'Y') 
                    except SchedulerError,e:
                        common.logger.message("Warning : Scheduler interaction in getOutput operation failed for jobs:")
                        common.logger.message(e.__str__())
                        pass
                    except BossError,e:
                        common.logger.message(e.__str__())
                        msg = 'Results of Job # '+`int(i_id)`+' have been corrupted and could not be retrieved.'
                        common.logger.message(msg)
                        common.jobDB.setStatus(int(i_id)-1, 'Z') 
                elif bossTaskIdStatus == 'Running' :
                     run.append(i_id)
            #        msg = 'Job # '+`int(i_id)`+' has status '+bossTaskIdStatus+'. It is not possible yet to retrieve the output.'
            #        common.logger.message(msg)
                elif bossTaskIdStatus == 'Cleared' :
                     clear.append(i_id)
            #        msg = 'Job # '+`int(i_id)`+' has status '+bossTaskIdStatus+'. The output was already retrieved.'
            #        common.logger.message(msg)
                elif bossTaskIdStatus == 'Aborted' :
                     abort.append(i_id)
            #        msg = 'Job # '+`int(i_id)`+' has status '+bossTaskIdStatus+'. It is not possible to retrieve the output.'
            #        common.logger.message(msg)
                elif bossTaskIdStatus == 'Cleared' :
                     create.append(i_id)
                elif bossTaskIdStatus == 'Cancelled' :
                     canc.append(i_id)  
                elif bossTaskIdStatus == 'Ready' :
                     read.append(i_id)
                elif bossTaskIdStatus == 'Scheduled' :
                     sched.append(i_id)
                elif bossTaskIdStatus == 'Waiting' :
                     wait.append(i_id)
                elif bossTaskIdStatus == 'Killed' :
                     kill.append(i_id)
                else:
                     other.append(i_id)  
            #        msg = 'Job # '+`int(i_id)`+' has status '+bossTaskIdStatus+'. It is currently not possible to retrieve the output.'
            #        common.logger.message(msg)
                dir += os.environ['USER']
                dir += '_' + os.path.basename(str(boss_id))
            pass
        common.jobDB.save() 
        if check == 0: 
            msg = '\n\n*********No job in Done status. It is not possible yet to retrieve the output.\n'
            common.logger.message(msg)

        if len(clear)!=0: print str(len(clear))+' jobs already cleared'     
        if len(abort)!=0: print str(len(abort))+' jobs aborted'   
        if len(canc)!=0: print str(len(canc))+' jobs cancelled'
        if len(kill)!=0: print str(len(kill))+' jobs killed'
        if len(run)!=0: print str(len(run))+' jobs still running'
        if len(sched)!=0: print str(len(sched))+' jobs scheduled'
        if len(wait)!=0: print str(len(wait))+' jobs waiting'
        if len(read)!=0: print str(len(read))+' jobs ready'
        if len(other)!=0: print str(len(other))+' jobs submitted'
        if len(create)!=0: print str(len(create))+' jobs not yet submitted'

        print ' ' 
        return

    ###################### ---- OK for Boss4 ds
    def cancel(self,int_id):
        """
        Cancel the EDG job with id: if id == -1, means all jobs.
        """
        #print "CANCEL -------------------------"
        #print "int_id ",int_id," nSubmitted ", common.jobDB.nSubmittedJobs()
        
        subm_id = []

        nTot = common.jobDB.nJobs()
        for id in int_id:
            if nTot >= id: ## TODO check the number of jobs..else: 'IndexError: list index out of range'
                if ( common.jobDB.status(id-1) in ['S','R','A']) and (id not in subm_id):
                    subm_id.append(id)
            else:
                common.logger.message("Warning: job # "+str(id)+" doesn't exists! Not possible to kill it.")
        ## first get the status of all job in the list
        # bossTaskId = common.taskDB.dict('BossTaskId')
        # statusList = self.queryStatusList(bossTaskId, subm_id)
        common.jobDB.load() 
        if len( subm_id ) > 0:
            try:
                subm_id.sort()
                range = self.prepString( subm_id )
                common.logger.message("Killing job # " + str(subm_id).replace("[","",1).replace("]","",1) )
                Tout =len(subm_id)*60
                self.bossTask.kill( range, Tout )
                self.bossTask.load(ALL, range)
                task = self.bossTask.jobsDict()
                for k, v in task.iteritems():
                    k = int(k)
                    status = v['STATUS']
                    if k in subm_id and status == 'K':
                        common.jobDB.setStatus(k - 1, 'K')
            except SchedulerError,e:
                common.logger.message("Warning : Scheduler interaction on kill operation failed for jobs:"+ e.__str__())
                pass
            except BossError,e:
                common.logger.message( e.__str__() + "\nError killing jobs # "+str(subm_id)+" . See log for details")
            common.jobDB.save()
            pass
        else:
            common.logger.message("\nError killing jobs # "+str(int_id).replace("[","",1).replace("]","",1)+" . See log for details")
        common.jobDB.save()
        return

    def setFlag( self, list, index ):
        if len( list ) > (index + 1):
            if list[index + 1] == ( list[index] + 1 ):
                return -2
            return -1
        return list[ len(list) - 1 ]

    def prepString( self, list ):
        s = ""
        flag = 0
        for i in range( len( list ) ):
            if flag == 0:
                s = str( list[i] )
                flag = self.setFlag( list, i )
            elif flag == -1:
                s = s + "," + str( list[i] )
                flag = self.setFlag( list, i )
            elif flag == -2:
                flag = self.setFlag( list, i )
                if flag == -1:
                    s = s + ":" + str( list[i] )
        if flag > 0:
            s = s + ":" + str( list[i] )
        return s

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

    ##################################################
    def queryEverything(self,taskid):
        """
        Query needed info of all jobs with specified boss taskid
        """

        self.boss_scheduler.checkProxy()

        results = {}
        try:
            # fill dictionary { 'bossid' : 'status' , ... }
            nTot = common.jobDB.nJobs()
            Tout = nTot*20 
            self.bossTask.query( ALL, timeout = Tout )
            task = self.bossTask.jobsDict()
            for c, v in task.iteritems():
                k = int(c)
                results[k] = { 'SCHED_ID' : v['SCHED_ID'], 'STATUS' : self.status[v['STATUS']], 'EXEC_HOST' : ['EXEC_HOST'] }
                if v.has_key('STATUS_REASON') :
                    results[k]['STATUS_REASON'] = v['STATUS_REASON']
                if v.has_key('LAST_T') :
                    results[k]['LAST_T'] = v['LAST_T']
                if v.has_key('DEST_CE') :
                    results[k]['DEST_CE'] = v['DEST_CE']
                if v.has_key('LB_TIMESTAMP') :
                    results[k]['LB_TIMESTAMP'] = v['LB_TIMESTAMP']
                program = self.bossTask.specific(c, '1')
                results[k]['EXE_EXIT_CODE'] = program['EXE_EXIT_CODE']
                results[k]['JOB_EXIT_STATUS'] = program['JOB_EXIT_STATUS']
        except SchedulerError,e:
            common.logger.message("Warning : Scheduler interaction failed for jobs:")
            common.logger.message(e.__str__())
            pass
        except BossError,e:
            common.logger.message( e.__str__() )
            pass
                
        return results

    ##################################################
    ################################################## To change "much" when Boss4 store also this infos  DS.
    def queryEveryStatus(self,taskid):
        """ Query a status of all jobs with specified boss taskid """

        self.boss_scheduler.checkProxy()

        results = {}
        try:
            nTot = common.jobDB.nJobs()
            Tout = nTot*20 
            # fill dictionary { 'bossid' : 'status' , ... }
            self.bossTask.query( ALL, timeout = Tout )
            task = self.bossTask.jobsDict()
            for k, v in task.iteritems():
                results[k] = self.status[v['STATUS']]
        except SchedulerError,e:
            common.logger.message("Warning : Scheduler interaction on query operation failed for jobs:")
            common.logger.message(e.__str__())
            pass
        except BossError,e:
            common.logger.message( e.__str__() )
                
        return results

    ##################################################
    def queryStatusList(self,taskid,list_id):
        """ Query a status of the job with id """

        self.boss_scheduler.checkProxy()

        allBoss_id = common.scheduler.listBoss()
        tmpQ = ''
        if not len(allBoss_id)==len(list_id): tmpQ = string.join(map(str,list_id),",")

        results = {}
        try:
            Tout = len(list_id)*20 
            # fill dictionary { 'bossid' : 'status' , ... }
            self.bossTask.query( ALL, tmpQ,  timeout = Tout )
            task = self.bossTask.jobsDict()
            for k, v in task.iteritems():
                results[int(k)] = self.status[v['STATUS']]
        except SchedulerError,e:
            common.logger.message("Warning : Scheduler interaction on query operation failed for jobs:")
            common.logger.message( e.__str__() )
            pass
        except BossError,e:
            common.logger.message( e.__str__() )
                
        return results

    ###################### ---- OK for Boss4 ds
    def listBoss(self):
        """
        Return a list of all boss_Id of a task
        """
        ListBoss_ID = []
        task = self.bossTask.jobsDict()
        for k, v in task.iteritems():
            ListBoss_ID.append(int(k))
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

    ################## 
    def taskDeclared( self, taskName ):
        #mySession = BossSession(self.bossConfigDir)
        #taskDict =mySession.loadByName( common.work_space.shareDir() )
        taskDict = self.bossUser.loadByName( taskName )
        #common.logger.debug(5, "taskName: " + str(taskName) )
        #common.logger.debug(5, "\n\n task: " + str(taskDict) + "\n\n")
        #mySession.clear()
        if len(taskDict) > 0:
            return True
        return False

    def clean(self):
        """ destroy boss instance """
        #self.bossUser.destroyBossTask(self.bossTask)
        del self.bossUser
        return
