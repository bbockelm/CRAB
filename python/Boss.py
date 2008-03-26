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
 
    def configure(self,cfg_params):  
        self.cfg_params = cfg_params
        self.schedulerName =  self.cfg_params.get("CRAB.scheduler",'') # this should match with the bosslite requirements
        self.rb_param_file=''
        if (cfg_params.has_key('EDG.rb')):
            self.rb_param_file=common.scheduler.rb_configure(cfg_params.get("EDG.rb"))
        self.wms_service=cfg_params.get("EDG.wms_service",'')        
 
        self.outDir = cfg_params.get("USER.outputdir", common.work_space.resDir() )
        self.logDir = cfg_params.get("USER.logdir", common.work_space.resDir() )

        self.return_data = cfg_params.get('USER.return_data',0)

        ## Add here the map for others Schedulers (LSF/CAF/CondorG)
        SchedMap = {'glite':'SchedulerGLiteAPI',          
                    'glitecoll':'SchedulerGLiteAPI',\
                    'condor_g':'',\
                    'lsf':'',\
                    'caf':''   
                    }         
                 
        schedulerConfig = {
              'name' : SchedMap[self.schedulerName], \
              'service' : self.wms_service, \
              'config' : self.rb_param_file  
              }

        self.schedSession = BossLiteAPISched( common.bossSession, schedulerConfig)
        
        return

    def declare(self, nj):       
        """
        BOSS declaration of jobs
        """
        index = nj - 1
        job = common.job_list[index]
        jbt = job.type()
        base = jbt.name()

        wrapper = os.path.basename(str(common._db.queryTask('scriptName')))
        listField=[]
        listID=[]  
        task=common._db.getTask()
        for id in range(nj):
            parameters={}
            jobs=[]
            out=[] 
            stdout = base +'_'+ str(id+1)+'.stdout'
            stderr = base +'_'+ str(id+1)+'.stderr'
            jobs.append(id)
            out=task.jobs[id]['outputFiles']
            out.append(stdout)
            out.append(stderr)
            out.append('.BrokerInfo')
            parameters['outputFiles']=out 
            parameters['executable']=wrapper
            parameters['standardOutput'] = stdout
            parameters['standardError'] = stderr
            listField.append(parameters)
            listID.append(id+1)     
        common._db.updateJob_( listID, listField)

        return 

    def listMatch(self, tags, dest, whiteL, blackL ):
        """
        Check the compatibility of available resources
        """
        sites = self.schedSession.lcgInfo(tags, dest, whiteL, blackL )
       
#        Tout = 120
#        CEs=[]
#        try:
#            CEs=self.bossUser.schedListMatch( schedulerName, schcladstring, self.bossTask.id(), "", Tout)
#            common.logger.debug(1,"CEs :"+str(CEs))
#        except SchedulerError,e:
#            common.logger.message( "Warning : Scheduler interaction in list-match operation failed for jobs:")
#            common.logger.message( e.__str__())
#            pass
#        except BossError,e:
#            raise CrabException("ERROR: listMatch failed with message " + e.__str__())
#        return CEs
        return len(sites)
  
    def submit(self, jobsList,req):
        """
        Submit BOSS function.
        Submit one job. nj -- job number.
        """
        task = common._db.getTask(jobsList)
        self.schedSession.submit( task,jobsList,req )
      #  try:
      #  except SchedulerError,e:
      #      common.logger.message("Warning : Scheduler interaction in submit operation failed for jobs:")
      #      common.logger.message(e.__str__())
      #      pass
      #  except BossError,e:
      #      common.logger.message("Error : BOSS command failed with message:")
      #      common.logger.message(e.__str__())
        
        return 

    def queryEverything(self,taskid):
        """
        Query needed info of all jobs with specified boss taskid
        """

        self.schedSession.query( str(taskid))
                
        return 

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




