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

        self.session=None
        return

    def __del__(self):
        """ destroy instance """
        del self.session

    def configure(self,cfg_params):
        self.cfg_params = cfg_params
        self.schedulerName =  self.cfg_params.get("CRAB.scheduler",'') # this should match with the bosslite requirements
        self.rb_param_file=''
        if (cfg_params.has_key('EDG.rb')):
            self.rb_param_file=common.scheduler.rb_configure(cfg_params.get("EDG.rb"))
        self.wms_service=cfg_params.get("EDG.wms_service",'')


        ## Add here the map for others Schedulers (LSF/CAF/CondorG)
        SchedMap = {'glite':    'SchedulerGLiteAPI',
                    'glitecoll':'SchedulerGLiteAPI',\
                    'condor':   'SchedulerCondor',\
                    'condor_g': 'SchedulerCondorG',\
                    'glidein':  'SchedulerGlidein',\
                    'lsf':      'SchedulerLsf',\
                    'caf':      'SchedulerLsf',\
                    'sge':      'SchedulerSge'
                    }

        self.schedulerConfig = common.scheduler.realSchedParams(cfg_params)
        self.schedulerConfig['name'] =  SchedMap[(self.schedulerName).lower()]

        self.session = None
        return

    def schedSession(self):
        '''
        Istantiate BossLiteApi session
        '''
        if not self.session:
            try:
                self.session =  BossLiteAPISched( common.bossSession, self.schedulerConfig)
            except Exception, e :
                common.logger.debug(3, "Istantiate SchedSession: " +str(traceback.format_exc()))
                raise CrabException('Scheduler Session: '+str(e))
        return self.session

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

    def listMatch(self, tags, dest, whiteL, blackL, isFull):
        """
        Check the compatibility of available resources
        """
        try:
            sites = self.schedSession().lcgInfo(tags, seList=dest, blacklist=blackL, whitelist=whiteL, full=isFull)
        except SchedulerError, err :
            common.logger.message("Warning: List Match operation failed with message: " +str(err))
            common.logger.debug(3, "List Match failed: " +str(traceback.format_exc()))

        return sites

    def submit(self, taskId,  jobsList, req):
        """
        Submit BOSS function.
        Submit one job. nj -- job number.
        """
        try:
            task_sub =  self.schedSession().submit( taskId, jobsList,req )
            wms = task_sub.jobs[0].runningJob['service']
            common.logger.write("WMS : " +str(wms))
        except SchedulerError, err :
            common.logger.message("Submit: " +str(err))
            common.logger.debug(3, "Submit: " +str(traceback.format_exc()))
            raise CrabException('Submit: '+str(err))

        return

    def queryEverything(self,taskid):
        """
        Query needed info of all jobs with specified taskid
        """

        try:
            statusRes =  self.schedSession().query( str(taskid))
        except SchedulerError, err :
            common.logger.message("Status Query : " +str(err))
            common.logger.debug(3, "Status Query : " +str(traceback.format_exc()))
            raise CrabException('Status Query : '+str(err))

        return statusRes

    def getOutput(self,taskId,jobRange, outdir):
        """
        Retrieve output of all jobs with specified taskid
        """
        try:
            self.schedSession().getOutput( taskId, jobRange, outdir )
        except SchedulerError, err :
            common.logger.message("GetOutput : " +str(err))
            common.logger.debug(3, "GetOutput : " +str(traceback.format_exc()))
            raise CrabException('GetOutput : '+str(err))

        return

    def cancel(self,list):
        """
        Cancel the job with id from a list
        """
        task = common._db.getTask(list)
        try:
            self.schedSession().kill( task, list)
        except SchedulerError, err :
            common.logger.message("Kill: " +str(err))
            common.logger.debug(3, "Kill: " +str(traceback.format_exc()))
            raise CrabException('Kill: '+str(err))
        return

    def LoggingInfo(self,list_id,outfile):
        """
        query the logging info with id from a list and
        retourn the reults
        """
        try:
            self.schedSession().postMortem(1,list_id,outfile)
        except SchedulerError, err :
            common.logger.message("logginginfo: " +str(err))
            common.logger.debug(3, "logginginfo: " +str(traceback.format_exc()))
            raise CrabException('logginginfo: '+str(err))
        return

    def writeJDL(self, taskId,jobsList,req):
        """
        JDL description BOSS function.
        """
        try:
            jdl = self.schedSession().jobDescription( taskId,jobsList,req )
        except SchedulerError, err :
            common.logger.message("writeJDL: " +str(err))
            common.logger.debug(3, "writeJDL: " +str(traceback.format_exc()))
            raise CrabException('writeJDL: '+str(err))

        return jdl

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
