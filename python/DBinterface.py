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
        self.task = common.bossSession.loadTaskByID(1)
        return
 
    def getTask(self, jobsList='all'): #, cfg_params):

        #if jobsList == 'all':
        #    self.task = common.bossSession.loadTaskByID(1)
        #else: 
        #self.task = common.bossSession.load('1','5')  
        #return self.task[0]
        self.task = common.bossSession.loadTaskByID(1)
        return self.task

    def getJob(self, n): 

        self.job = common.bossSession.loadJobByID(1,n)
        return self.job


    def createTask_(self, optsToSave):       
        """
        Task declaration
        with the first coniguration stuff 
        {'server_name': 'crabas.lnl.infn.it/data1/cms/', '-scheduler': 'glite', '-jobtype': 'cmssw', '-server_mode': '0'}

        """
        opt={}
        if optsToSave['server_mode'] == 1: opt['serverName']=optsToSave['server_name'] 
        opt[ 'name']=common.work_space.taskName()  
     	task = Task( opt )
      
        common.bossSession.saveTask( task )
        return 

    def updateTask_(self,optsToSave):       
        """
        Update task fields   
        """
        #task = common.bossSession.loadTaskByName(common.work_space.taskName() )
        task = common.bossSession.loadTaskByID(1)
         
        for key in optsToSave.keys():
            task[key] = optsToSave[key]
            common.bossSession.updateDB( task )
        return 

    def createJobs_(self, nj):
        """  
        Fill crab DB with  the jobs filed 
        """
        #task = common.bossSession.loadTaskByName(common.work_space.taskName())
        task = common.bossSession.loadTaskByID(1)
        jobs = [] 
        for id in range(nj):
            parameters = {}
            parameters['name'] = 'job' + str(id)
            job = Job(parameters)
            jobs.append(job)    
        task.addJobs(jobs)
        common.bossSession.updateDB( task )
        return

    def updateJob_(self, nj, optsToSave):       
        """
        Update Job fields   
        """
        task = common.bossSession.loadTaskByID(1)
        #task = common.bossSession.loadTaskByName( common.work_space.taskName())
        for i in range(len(nj)):
           # jobs = common.bossSession.loadJob(task['id'],i)
            for key in optsToSave[i].keys():
                task.jobs[i][key] = optsToSave[i][key]
        common.bossSession.updateDB( task )
        return 

    def updateRunJob_(self, nj, optsToSave):       
        """
        Update Running Job fields   
        """
        task = common.bossSession.loadTaskByID(1)
        #task = common.bossSession.loadTaskByName( common.work_space.taskName())
        for i in nj:
            common.bossSession.getRunningInstance(task.jobs[i])
            for key in optsToSave.keys():
                task.jobs[i].runningJob[key] = optsToSave[key]
        common.bossSession.updateDB( task )
        return 

    def nJobs(self):
        
        task = common.bossSession.loadTaskByID(1)
        #task = common.bossSession.loadTaskByName( common.work_space.taskName())
        return len(task.jobs) 

    def dump(self,jobs):
        """
         List a complete set of infos for a job/range of jobs   
        """
        task = common.bossSession.loadTaskByID(1)
        #task = common.bossSession.loadTaskByName( common.work_space.taskName())

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
 
    def queryID(self,server_mode=0):
        '''
        Return the taskId if serevr_mode =1 
        Return the joblistId if serevr_mode =0 
        '''     
        header=''
        lines=[]
        task = common.bossSession.loadTaskByID(1)
        if server_mode == 1:
            header= "Task Id = %-40s " %(task['name'])
        else:
         #   task = common.bossSession.loadTaskByName(common.work_space.taskName() )
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
        task = common.bossSession.loadTaskByID(1)
        return task[attr]

    def queryJob(self, attr, njobs):
        '''
        Perform a query for a range/all/single job 
        over a generic job attribute 
        '''
        lines=[]
        task = common.bossSession.loadTaskByID(1)
        #task = common.bossSession.loadTaskByName( common.work_space.taskName())
        for i in njobs:
            jobs = common.bossSession.loadJob(task['id'],i+1)
            lines.append(task.jobs[i][attr])
        return lines

    def queryRunJob(self, attr, jobs):
        '''
        Perform a query for a range/all/single job 
        over a generic job attribute 
        '''
        lines=[]
        task = common.bossSession.loadTaskByID(1)
       # task = common.bossSession.loadTaskByName( common.work_space.taskName() )
        for i in jobs:
            common.bossSession.getRunningInstance(task.jobs[i-1])
            lines.append(task.jobs[i-1].runningJob[attr])
        return lines

    def queryDistJob(self, attr):
        '''
        Returns the list of distinct value for a given job attributes 
        '''
        distAttr=[]
        task = common.bossSession.loadJobDist( 1, attr ) 
        for i in task: distAttr.append(i[attr])   
        return  distAttr

    def queryDistJob_Attr(self, attr_1, attr_2, list):
        '''
        Returns the list of distinct value for a given job attributes 
        '''
        distAttr=[]
        task = common.bossSession.loadJobDistAttr( 1, attr_1, attr_2, list ) 
        for i in task: distAttr.append(i[attr_1])   
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
