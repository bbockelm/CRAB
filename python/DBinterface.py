from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import os, time, shutil

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI


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
 
        common.bossSession = BossLiteAPI( self.db_type, dbConfig)
        common.bossSession.installDB('$CRABPRODCOMMONPYTHON/ProdCommon/BossLite/DbObjects/setupDatabase-sqlite.sql')     
        
        return

    def loadDB(self):

        dbname = common.work_space.shareDir()+'crabDB'
        dbConfig = {'dbName':dbname
            }
        common.bossSession = BossLiteAPI( self.db_type, dbConfig)
        self.task = common.bossSession.load(1)[0]
        return
 
    def getTask(self, jobsList='all'): #, cfg_params):
        """
        Return task with all/list of jobs 
        """

        if jobsList == 'all':
            task = common.bossSession.load(1)[0]
        else: 
            if len(jobsList)>1: str_jobs=string.join(map(str,jobsList),",") 
            else: str_jobs=str(jobsList)  
            task = common.bossSession.load(1,jobsList)[0]
        return task

    def getJob(self, n): 
        """
        Return a task with a single job 
        """ 
        task = common.bossSession.load(1,str(n))[0]
        return task


    def createTask_(self, optsToSave):       
        """
        Task declaration
        with the first coniguration stuff 
        """
        opt={}
        opt['serverName']=optsToSave['server_name'] 
        opt[ 'name']=common.work_space.taskName()  
     	task = Task( opt )
      
        common.bossSession.saveTask( task )
        return 

    def updateTask_(self,optsToSave):       
        """
        Update task fields   
        """
        task = common.bossSession.load(1)[0]
         
        for key in optsToSave.keys():
            task[key] = optsToSave[key]
        common.bossSession.updateDB( task )
        return 

    def createJobs_(self, jobsL):
        """  
        Fill crab DB with  the jobs filed 
        """
        task = common.bossSession.loadTask(1) 
        jobs = [] 
        for id in jobsL:
            parameters = {}
            parameters['jobId'] =  str(id)
            parameters['name'] = 'job' + str(id)
            job = Job(parameters)
            jobs.append(job)  
        task.addJobs(jobs)
        common.bossSession.updateDB( task )
        return

    def updateJob_(self, jobsL, optsToSave):       
        """
        Update Job fields   
        """
        if len(jobsL)>1: str_jobs=string.join(map(str,jobsL),",")
        else: str_jobs=str(jobsL)
        task = common.bossSession.load(1,jobsL)[0]
        id =0 
        for job in task.jobs:
            for key in optsToSave[id].keys():
                job[key] = optsToSave[id][key]
            id+=1
        common.bossSession.updateDB( task )
        return 

    def updateRunJob_(self, jobsL, optsToSave):       
        """
        Update Running Job fields   
        """
        if len(jobsL)>1: str_jobs=string.join(map(str,jobsL),",")
        else: str_jobs=str(jobsL)
        task = common.bossSession.load(1,jobsL)[0]
        for job in task.jobs:
            common.bossSession.getRunningInstance(job)
            for key in optsToSave.keys():
                job.runningJob[key] = optsToSave[key]
        common.bossSession.updateDB( task )
        return 

    def nJobs(self,list=''):
        
        task = common.bossSession.load(1)[0]
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
        task = common.bossSession.load(1)[0]

        njobs = len(jobs)
        lines=[] 
        header=''
     #   ##query the DB asking the right infos for runningJobs  TODO  DS
     #   for job in jobs:
     #       ## here the query over runngJobs  
     #       pass


     #   ##Define Header to show and Pass the query results,
     #   ##  header and format to displayReport()   TODO  DS
     #   if njobs == 1: plural = ''
     #   else:          plural = 's'
     #   header += 'Listing %d job%s:\n' % (njobs, plural) 
     #   header += ' :\n' % (---) ## TODO DS 

     #   displayReport(header, lines):
        return      

    def serializeTask(self, tmp_task = None):
        if tmp_task is None:
            #tmp_task = common.bossSession.loadTaskByID(1)
            tmp_task = common.bossSession.load(1)[0]
        return common.bossSession.serialize(tmp_task)   
 
    def queryID(self,server_mode=0):
        '''
        Return the taskId if serevr_mode =1 
        Return the joblistId if serevr_mode =0 
        '''     
        header=''
        lines=[]
        task = common.bossSession.load(1)[0]
        if server_mode == 1:
            header= "Task Id = %-40s " %(task['name'])
        else:
            for i in range(len(task.job)): 
                common.bossSession.getRunningInstance(task.jobs[i])
                lines.append(task.jobs[i].runningJob['schedulerId'])
           
            header+= "Job: %-5s Id = %-40s: \n" 
        displayReport(header,lines)
        return   

    def queryTask(self,attr):
        '''
        Perform a query over a generic task attribute
        '''
        task = common.bossSession.loadTask(1)
        return task[attr]

    def queryJob(self, attr, jobs):
        '''
        Perform a query for a range/all/single job 
        over a generic job attribute 
        '''
        lines=[]
        str_jobs=string.join(map(str,jobs),",") 
        task = common.bossSession.load(1,str_jobs)[0]
        for job in task.jobs:
            lines.append(eval(job[attr]))
        return lines

    def queryRunJob(self, attr, jobs):
        '''
        Perform a query for a range/all/single job 
        over a generic job attribute 
        '''
        lines=[]
        str_jobs=string.join(map(str,jobs),",") 
        task = common.bossSession.load(1,str_jobs)[0]
        for job in task.jobs:
            common.bossSession.getRunningInstance(job)
            lines.append(job.runningJob[attr])
        return lines

    def queryDistJob(self, attr):
        '''
        Returns the list of distinct value for a given job attributes 
        '''
        distAttr=[]
        task = common.bossSession.loadJobDist( 1, attr ) 
        for i in task: distAttr.append(eval(i[attr]))   
        return  distAttr

    def queryDistJob_Attr(self, attr_1, attr_2, list):
        '''
        Returns the list of distinct value for a given job attribute 
        '''
        distAttr=[]
        task = common.bossSession.loadJobDistAttr( 1, attr_1, attr_2, list ) 
        for i in task: distAttr.append(eval(i[attr_1]))   
        return  distAttr

    def queryAttrJob(self, attr, field):
        '''
        Returns the list of jobs matching the given attribute
        '''
        matched=[]
        task = common.bossSession.loadJobsByAttr(attr ) 
        for i in task:
            matched.append(i[field])
        return  matched


    def queryAttrRunJob(self, attr,field):
        '''
        Returns the list of jobs matching the given attribute
        '''
        matched=[]
        task = common.bossSession.loadJobsByRunningAttr(attr)
        for i in task:
            matched.append(i[field])
        return matched 
