from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import os, time, shutil

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI


from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

from ProdCommon.BossLite.API.BossLiteAPISched import  BossLiteAPISched

class Boss:
    def __init__(self):

        return

    def __del__(self):
        """ destroy instance """
        del self.bossAdmin
        del self.bossUser
        return


#### Boss Configuration 

    def configure(self, cfg_params):

        self.cfg_params = cfg_params
        self.schedulerName =  cfg_params.get("CRAB.scheduler",'') # this should match with the bosslite requirements

        #schedulerConfig = { 'name' : self.schedulerName}  ## ToDo BL-DS
        schedulerConfig = { 'name' : 'SchedulerGLiteAPI' }
        self.schedSession = BossLiteAPISched( common.bossSession, schedulerConfig)

        self.outDir = cfg_params.get("USER.outputdir", common.work_space.resDir() )
        self.logDir = cfg_params.get("USER.logdir", common.work_space.resDir() )

        self.return_data = cfg_params.get('USER.return_data',0)
        
        return

#### End Boss Configuration

    def declareJob_(self, nj):       
        """
        BOSS declaration of jobs
        """
        index = nj - 1
        job = common.job_list[index]
        jbt = job.type()
        base = jbt.name()

        for id in range(nj):
            parameters={}
            jobs=[]
            out=[] 
            stdout = base +'_'+ str(id)+'.stdout'
            stderr = base +'_'+ str(id)+'.stderr'
            jobs.append(id)
            out=common._db.queryJob('outputFiles',jobs)[0]
            out.append(stdout)
            out.append(stderr)
            out.append('.BrokerInfo')
            parameters['outputFiles']=out 
            parameters['executable']=common._db.queryTask('scriptName') ## Should disappear... 
                                                                        ## we'll have ONLY 'executable'  
                                                                        ## as task field and not job field
            parameters['standardOutput'] = stdout
            parameters['standardError'] = stderr

            common._db.updateJob_(id,parameters)

        return 

    def task(self):
        """ return Boss Task """
        return self.bossTask
        
    ##########################################   ---- OK for Boss4 ds
    def listMatch(self, schedulerName, schcladstring):
        """
        Check the compatibility of available resources
        """
        Tout = 120
        CEs=[]
        try:
            CEs=self.bossUser.schedListMatch( schedulerName, schcladstring, self.bossTask.id(), "", Tout)
            common.logger.debug(1,"CEs :"+str(CEs))
        except SchedulerError,e:
            common.logger.message( "Warning : Scheduler interaction in list-match operation failed for jobs:")
            common.logger.message( e.__str__())
            pass
        except BossError,e:
            raise CrabException("ERROR: listMatch failed with message " + e.__str__())
        return CEs
  
    def submit(self, jobsList, schcladstring, Tout):
        """
        Submit BOSS function.
        Submit one job. nj -- job number.
        """

        try:
            self.bossTask.submit(string.join(jobsList,','), schcladstring, "", "" , "", Tout)
        except SchedulerError,e:
            common.logger.message("Warning : Scheduler interaction in submit operation failed for jobs:")
            common.logger.message(e.__str__())
            pass
        except BossError,e:
            common.logger.message("Error : BOSS command failed with message:")
            common.logger.message(e.__str__())
        
        jid=[]
        bjid = []
        self.bossTask.clear()
        range = str(jobsList[0]) + ":" + str(jobsList[-1])
        try:
            self.bossTask.load(ALL, range)
        except SchedulerError,e:
            common.logger.message("Warning : Scheduler interaction in query operation failed for jobs:")
            common.logger.message(e.__str__())
            pass
        except BossError,e:
            common.logger.message("Error : BOSS command failed with message:")
            common.logger.message(e.__str__())
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
        allBoss_id = self.list()
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
        Tout=180

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
                        self.bossTask.getOutput (str(boss_id), str(dir), Tout)
                        if logDir != dir:
                            try:
                                ######
                                cmd = 'mv '+str(dir)+'/*'+str(i_id)+'.std* '+str(dir)+'/.BrokerInfo '+str(dir)+'/*.log '+str(logDir)
                                cmd_out =os.system(cmd)
                                msg = 'Results of Job # '+str(i_id)+' are in '+dir+' (log files are in '+logDir+')' 
                                common.logger.message(msg)
                                #####
                                #toMove = str(dir)+'/*'+`int(i_id)`+'.std* '+str(dir)+'/*.log '+str(dir)+'/.BrokerInfo '
                                #shutil.move(toMove, str(logDir))
                                #####
                            except:
                                msg = 'Problem with copy of job results' 
                                common.logger.message(msg)
                                pass  
                        else:   
                            msg = 'Results of Job # '+`int(i_id)`+' are in '+dir
                            common.logger.message(msg)
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
                elif bossTaskIdStatus == 'Created' :
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
    def cancel(self,subm_id):
        """
        Cancel the EDG job with id: if id == -1, means all jobs.
        """
        #print "CANCEL -------------------------"
        #print "int_id ",int_id," nSubmitted ", common.jobDB.nSubmittedJobs()
        
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
            common.logger.message("\nNo job to be killed")
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
                if v.has_key('RB') :
                    results[k]['RB'] = v['RB']
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

        allBoss_id = self.list()
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
    def list(self):
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

    ################## 
    def taskDeclared( self, taskName ):
        taskDict = self.bossUser.loadByName( taskName )
        return (len(taskDict) > 0)

    def clean(self):
        """ destroy boss instance """
        del self.bossUser
        return
