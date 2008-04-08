from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import os, time, shutil
import traceback

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI


from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

from ProdCommon.BossLite.Common.Exceptions import  SchedulerError
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


        ## Add here the map for others Schedulers (LSF/CAF/CondorG)
        SchedMap = {'glite':'SchedulerGLiteAPI',
                    'glitecoll':'SchedulerGLiteAPI',\
                    'condor_g':'SchedulerCondorGAPI',\
                    'lsf':'',\
                    'caf':''
                    }

        self.schedulerConfig = {
              'name' : SchedMap[self.schedulerName], \
              'service' : self.wms_service, \
              'config' : self.rb_param_file
              }
        return

    def schedSession(self): 
        ''' 
        Istantiate BossLiteApi session
        '''  
        try: 
            session =  BossLiteAPISched( common.bossSession, self.schedulerConfig)
        except Exception, e :
            common.logger.debug(3, "Istantiate SchedSession: " +str(traceback.format_exc()))
            raise CrabException('Scheduler Session: '+str(e))
        return session

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
         #   out.append(stdout)
         #   out.append(stderr)
         #   out.append('.BrokerInfo')
            ## To be better understood if it is needed
            out.append('crab_fjr_'+str(id+1)+'.xml')
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
        schedSession = self.schedSession() 
        try:
            sites = schedSession.lcgInfo(tags, dest, whiteL, blackL )
        except SchedulerError, err :
            common.logger.message("Warning: List Match operation failed with message: " +str(err))
            common.logger.debug(3, "List Match failed: " +str(traceback.format_exc()))


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
        schedSession = self.schedSession() 
       # schedSession = BossLiteAPISched( common.bossSession, self.schedulerConfig)
        task = common._db.getTask(jobsList)
        try: 
            schedSession.submit( task,jobsList,req )
        except SchedulerError, err :
            common.logger.message("List Match: " +str(err))
            common.logger.debug(3, "List Match: " +str(traceback.format_exc()))
            raise CrabException('List Match: '+str(err))

        return

    def queryEverything(self,taskid):
        """
        Query needed info of all jobs with specified taskid
        """

        schedSession = self.schedSession() 
        try:
            statusRes =  schedSession.query( str(taskid))
        except SchedulerError, err :
            common.logger.message("Status Query : " +str(err))
            common.logger.debug(3, "Status Query : " +str(traceback.format_exc()))
            raise CrabException('Status Query : '+str(err))

        return statusRes

    def getOutput(self,taskId,jobRange, outdir):
        """
        Retrieve output of all jobs with specified taskid
        """
        schedSession = self.schedSession() 
        try: 
            schedSession.getOutput( taskId, jobRange, outdir )
        except SchedulerError, err :
            common.logger.message("GetOutput : " +str(err))
            common.logger.debug(3, "GetOutput : " +str(traceback.format_exc()))
            raise CrabException('GetOutput : '+str(err))
 
        return

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




