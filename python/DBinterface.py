from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import os, time, shutil
import traceback

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import DbError
from ProdCommon.BossLite.Common.Exceptions import TaskError

from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob


class DBinterface:
    def __init__(self, cfg_params):

        self.cfg_params = cfg_params

        self.db_type =  cfg_params.get("USER.use_db",'SQLite')
        return


    def configureDB(self):

        dbname = common.work_space.shareDir()+'crabDB'
        dbConfig = {'dbName':dbname
            }
        try: 
            common.bossSession = BossLiteAPI( self.db_type, dbConfig)
        except Exception, e :
            raise CrabException('Istantiate DB Session : '+str(e))

        try:
            common.bossSession.installDB('$CRABPRODCOMMONPYTHON/ProdCommon/BossLite/DbObjects/setupDatabase-sqlite.sql')     
        except Exception, e :
            raise CrabException('DB Installation error : '+str(e))
        return 
 
    def loadDB(self):

        dbname = common.work_space.shareDir()+'crabDB'
        dbConfig = {'dbName':dbname
            }
        try:
            common.bossSession = BossLiteAPI( self.db_type, dbConfig)
        except Exception, e :
            raise CrabException('Istantiate DB Session : '+str(e))

        return
 
    def getTask(self, jobsList='all'): 
        """
        Return task with all/list of jobs 
        """
        try:
            task = common.bossSession.load(1,jobsList)[0]
        except Exception, e :
            common.logger.debug(3, "Error while getting task : " +str(traceback.format_exc()))
            raise CrabException('Error while getting task '+str(e))
        return task

    def getJob(self, n): 
        """
        Return a task with a single job 
        """ 
        try:
            task = common.bossSession.load(1,str(n))[0]
        except Exception, e :
            common.logger.debug(3, "Error while getting job : " +str(traceback.format_exc()))
            raise CrabException('Error while getting job '+str(e))
        return task


    def createTask_(self, optsToSave):       
        """
        Task declaration
        with the first coniguration stuff 
        """
        opt={}
        if optsToSave.get('server_mode',0) == 1: opt['serverName']=optsToSave['server_name'] 
        opt['name']=common.work_space.taskName()  
     	task = Task( opt )
        try:
            common.bossSession.saveTask( task )
        except Exception, e :
           # common.logger.debug(3, "Error creating task : " +str(traceback.format_exc()))
           # raise CrabException('Error creating task '+str(e))
            raise CrabException('Error creating task '+str(traceback.format_exc()))
            
        return 

    def updateTask_(self,optsToSave):       
        """
        Update task fields   
        """
        task = self.getTask()
   
        for key in optsToSave.keys():
            task[key] = optsToSave[key]
        try:
            common.bossSession.updateDB( task )
        except Exception, e :
            raise CrabException('Error updating task '+str(traceback.format_exc()))

        return 

    def createJobs_(self, jobsL):
        """  
        Fill crab DB with  the jobs filed 
        """
        task = self.getTask()

        jobs = [] 
        for id in jobsL:
            parameters = {}
            parameters['jobId'] =  str(id)
            parameters['name'] = task['name'] + '_' + 'job' + str(id)
            job = Job(parameters)
            jobs.append(job) 
            common.bossSession.getRunningInstance(job)
            job.runningJob['status'] = 'C'
        task.addJobs(jobs)
        try:
            common.bossSession.updateDB( task )
        except Exception, e :
            raise CrabException('Error updating task '+str(traceback.format_exc()))

        return

    def updateJob_(self, jobsL, optsToSave):       
        """
        Update Job fields   
        """
        task = self.getTask(jobsL)
        id =0 
        for job in task.jobs:
            for key in optsToSave[id].keys():
                job[key] = optsToSave[id][key]
            id+=1
        try:
            common.bossSession.updateDB( task )
        except Exception, e :
            raise CrabException('Error updating task '+str(traceback.format_exc()))
        return 

    def updateRunJob_(self, jobsL, optsToSave):       
        """
        Update Running Job fields   
        """
        task = self.getTask(jobsL)

        id=0
        for job in task.jobs:
            common.bossSession.getRunningInstance(job)
            for key in optsToSave[id].keys():
                job.runningJob[key] = optsToSave[id][key]
            id+=1
        common.bossSession.updateDB( task )
        return 

    def nJobs(self,list=''):
        
        task = self.getTask()
        listId=[]
        if list == 'list':
            for job in task.jobs:listId.append(int(job['jobId']))  
            return listId
        else:
            return len(task.jobs) 

    def dump(self,jobs):
        """
         List a complete set of infos for a job/range of jobs   
        """
        task = self.getTask(jobs)

        Jobs = task.getJobs()
        print "--------------------------"
        for Job in Jobs:
            print "Id: ",Job['id']
            print "Dest: ", Job['dlsDestination']
            print "Output: ", Job['outputFiles']
            print "Args: ",Job['arguments']
            print "--------------------------"
        return      

    def serializeTask(self, tmp_task = None):
        if tmp_task is None:
            tmp_task = self.getTask()
        return common.bossSession.serialize(tmp_task)   
 
    def queryID(self,server_mode=0):
        '''
        Return the taskId if serevr_mode =1 
        Return the joblistId if serevr_mode =0 
        '''     
        header=''
        lines=[]
        task = self.getTask()
        if server_mode == 1:
            header= "Task Id = %-40s " %(task['name'])
        else:
            for job in task.jobs: 
                toPrint=''
                common.bossSession.getRunningInstance(job)
                toPrint = "%-5s %-50s " % (job['id'],job.runningJob['schedulerId'])
                lines.append(toPrint)
            header+= "%-5s %-50s " % ('Job:','ID' ) 
        displayReport(self,header,lines)
        return   

    def queryTask(self,attr):
        '''
        Perform a query over a generic task attribute
        '''
        task = self.getTask()
        return task[attr]

    def queryJob(self, attr, jobsL):
        '''
        Perform a query for a range/all/single job 
        over a generic job attribute 
        '''
        lines=[]
        task = self.getTask(jobsL)
        for job in task.jobs:
            lines.append(job[attr])
        return lines

    def queryRunJob(self, attr, jobsL):
        '''
        Perform a query for a range/all/single job 
        over a generic job attribute 
        '''
        lines=[]
        task = self.getTask(jobsL)
        for job in task.jobs:
            common.bossSession.getRunningInstance(job)
            lines.append(job.runningJob[attr])
        return lines

    def queryDistJob(self, attr):
        '''
        Returns the list of distinct value for a given job attributes 
        '''
        distAttr=[]
        try:
            task = common.bossSession.loadJobDist( 1, attr ) 
        except Exception, e :
            common.logger.debug(3, "Error loading Jobs By distinct Attr : " +str(traceback.format_exc()))
            raise CrabException('Error loading Jobs By distinct Attr '+str(e))

        for i in task: distAttr.append(i[attr])   
        return  distAttr

    def queryDistJob_Attr(self, attr_1, attr_2, list):
        '''
        Returns the list of distinct value for a given job attribute 
        '''
        distAttr=[]
        try:
            task = common.bossSession.loadJobDistAttr( 1, attr_1, attr_2, list ) 
        except Exception, e :
            common.logger.debug(3, "Error loading Jobs By distinct Attr : " +str(traceback.format_exc()))
            raise CrabException('Error loading Jobs By distinct Attr '+str(e))

        for i in task: distAttr.append(i[attr_1])  
        return  distAttr

    def queryAttrJob(self, attr, field):
        '''
        Returns the list of jobs matching the given attribute
        '''
        matched=[]
        try:
            task = common.bossSession.loadJobsByAttr(attr ) 
        except Exception, e :
            common.logger.debug(3, "Error loading Jobs By Attr : " +str(traceback.format_exc()))
            raise CrabException('Error loading Jobs By Attr '+str(e))
        for i in task:
            matched.append(i[field])
        return  matched


    def queryAttrRunJob(self, attr,field):
        '''
        Returns the list of jobs matching the given attribute
        '''
        matched=[]
        try:
            task = common.bossSession.loadJobsByRunningAttr(attr)
        except Exception, e :
            common.logger.debug(3, "Error loading Jobs By Running Attr : " +str(traceback.format_exc()))
            raise CrabException('Error loading Jobs By Running Attr '+str(e))
        for i in task:
            matched.append(i.runningJob[field])
        return matched 

    def newRunJobs(self,nj='all'):
        """
        Get new running instances
        """  
        task = self.getTask(nj)

        for job in task.jobs:
            if job.runningJob is not None :
                job.runningJob["closed"] = "Y"

        common.bossSession.updateDB(task)

        for job in task.jobs:
            job.runningJob = None
            common.bossSession.getRunningInstance(job)
        return        

    def deserXmlStatus(self, reportList):

        task = self.getTask()
 
        for job in task.jobs:
            if not job.runningJob:
                raise CrabException( "Missing running object for job %s"%str(job['id']) )

            id = str(job.runningJob['id'])
            # TODO linear search, probably it can be optized with binary search
            rForJ = None
            for r in reportList:
                if r.getAttribute('id') in [ id, 'all']:
                    rForJ = r
                    break

            # Data alignment
            jobStatus = str(job.runningJob['statusScheduler'])
            if rForJ.getAttribute('status') not in ['Created', 'Submitting'] and \
                     job.runningJob['statusScheduler'] != 'Cleared':
                job.runningJob['statusScheduler'] = str( rForJ.getAttribute('status') )
                jobStatus = str(job.runningJob['statusScheduler'])
                job.runningJob['status'] = str( rForJ.getAttribute('sched_status') )

            job.runningJob['destination'] = str( rForJ.getAttribute('site') )
            dest = str(job.runningJob['destination']).split(':')[0]

            job.runningJob['applicationReturnCode'] = str( rForJ.getAttribute('exe_exit') )
            exe_exit_code = str(job.runningJob['applicationReturnCode'])

            job.runningJob['wrapperReturnCode'] = str( rForJ.getAttribute('job_exit') )
            job_exit_code = str(job.runningJob['wrapperReturnCode'])

            if str( rForJ.getAttribute('resubmit') ).isdigit():
                job['submissionNumber'] = int(rForJ.getAttribute('resubmit'))
            # TODO cleared='0' field, how should it be handled/mapped in BL? #Fabio

        common.bossSession.updateDB( task )

        return
