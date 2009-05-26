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
        self.deep_debug= self.cfg_params.get("USER.deep_debug",'0')
        server_check =  self.cfg_params.get("CRAB.server_name",None)
        if self.deep_debug == '1' and server_check != None :
            msg =  'You are asking the deep_debug, but it cannot works using the server.\n'
            msg += '\t The functionality will not have effect.'
            common.logger.info(msg) 
        self.schedulerName =  self.cfg_params.get("CRAB.scheduler",'') # this should match with the bosslite requirements
        self.rb_param_file=''
        if (not cfg_params.has_key('EDG.rb')):
            cfg_params['EDG.rb']='CERN'
        self.rb_param_file=common.scheduler.rb_configure(cfg_params.get("EDG.rb"))
        self.wms_service=cfg_params.get("EDG.wms_service",'')

        self.wrapper = cfg_params.get('CRAB.jobtype').upper()+'.sh'


        ## Add here the map for others Schedulers (LSF/CAF/CondorG)
        SchedMap = {'glite':    'SchedulerGLiteAPI',
                    'glitecoll':'SchedulerGLiteAPI',\
                    'condor':   'SchedulerCondor',\
                    'condor_g': 'SchedulerCondorG',\
                    'glidein':  'SchedulerGlidein',\
                    'lsf':      'SchedulerLsf',\
                    'caf':      'SchedulerLsf',\
                    'sge':      'SchedulerSge',
                    'arc':      'SchedulerARC'
                    }

        self.schedulerConfig = common.scheduler.realSchedParams(cfg_params)
        self.schedulerConfig['name'] =  SchedMap[(self.schedulerName).lower()]
        self.schedulerConfig['timeout'] = 180
        self.schedulerConfig['skipProxyCheck'] = True 
        self.schedulerConfig['logger'] = common.logger 

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
                common.logger.debug("Istantiate SchedSession: " +str(traceback.format_exc()))
                raise CrabException('Scheduler Session: '+str(e))
        return self.session

    def declare(self, listID):
        """
        BOSS declaration of jobs
        """
        index = len(listID) - 1
        job = common.job_list[index]
        jbt = job.type()
        base = jbt.name()

        listField=[]
        task=common._db.getTask()
        for id in listID:
            parameters={}
            jobs=[]
            out=[]
            stdout = base +'_'+ str(id)+'.stdout'
            stderr = base +'_'+ str(id)+'.stderr'
            jobs.append(id)
            out=task.jobs[id-1]['outputFiles']
            if self.deep_debug == '1':
                out.append(stdout)
                out.append(stderr)
         #   out.append('.BrokerInfo')
            ## To be better understood if it is needed
            out.append('crab_fjr_'+str(id)+'.xml')
            parameters['outputFiles']=out
            parameters['executable']=self.wrapper
            parameters['standardOutput'] = stdout
            parameters['standardError'] = stderr
            listField.append(parameters)
        common._db.updateJob_( listID, listField)

        return

    def listMatch(self, tags, voTags, dest, whiteL, blackL, isFull):
        """
        Check the compatibility of available resources
        """
        try:
            ## passing list for white-listing and black-listing
            sites = self.schedSession().getSchedulerInterface().lcgInfo(tags, voTags, seList=dest, blacklist=blackL, whitelist=whiteL, full=isFull)
        except SchedulerError, err :
            common.logger.info("Warning: List Match operation failed with message: " +str(err))
            common.logger.debug( "List Match failed: " +str(traceback.format_exc()))

        return sites

    def submit(self, taskId,  jobsList, req):
        """
        Submit BOSS function.
        Submit one job. nj -- job number.
        """
        try:
            task_sub =  self.schedSession().submit( taskId, jobsList,req )
            wms = task_sub.jobs[0].runningJob['service']
            collId = task_sub.jobs[0].runningJob['schedulerParentId']   
            msg = 'WMS : ' +str(wms)+'\n'
            msg+= 'Collection ID : ' +str(collId)
            common.logger.debug(msg)
        except SchedulerError, err :
            common.logger.info("Submit: " +str(err))
            common.logger.debug("Submit: " +str(traceback.format_exc()))
            raise CrabException('Submit: '+str(err))

        return

    def queryEverything(self,taskid):
        """
        Query needed info of all jobs with specified taskid
        """

        try:
            statusRes =  self.schedSession().query( str(taskid))
        except SchedulerError, err :
            common.logger.info("Status Query : " +str(err))
            common.logger.debug( "Status Query : " +str(traceback.format_exc()))
            raise CrabException('Status Query : '+str(err))

        return statusRes

    def getOutput(self,taskId,jobRange, outdir):
        """
        Retrieve output of all jobs with specified taskid
        """
        try:
            task = self.schedSession().getOutput( taskId, jobRange, outdir )
        except SchedulerError, err :
            common.logger.info("GetOutput : " +str(err))
            common.logger.debug( "GetOutput : " +str(traceback.format_exc()))
            raise CrabException('GetOutput : '+str(err))

        return task

    def cancel(self,list):
        """
        Cancel the job with id from a list
        """
        task = common._db.getTask(list)
        try:
            self.schedSession().kill( task, list)
        except SchedulerError, err :
            common.logger.info("Kill: " +str(err))
            common.logger.debug( "Kill: " +str(traceback.format_exc()))
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
            common.logger.info("logginginfo: " +str(err))
            common.logger.debug( "logginginfo: " +str(traceback.format_exc()))
            raise CrabException('logginginfo: '+str(err))
        return

    def writeJDL(self, taskId,jobsList,req):
        """
        JDL description BOSS function.
        """
        try:
            jdl = self.schedSession().jobDescription( taskId,jobsList,req )
        except SchedulerError, err :
            common.logger.info("writeJDL: " +str(err))
            common.logger.debug( "writeJDL: " +str(traceback.format_exc()))
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
