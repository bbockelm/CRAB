from Submitter import Submitter
import common
from crab_util import *
from crab_exceptions import *

class Resubmitter(Submitter):
    def __init__(self, cfg_params, jobs):
        self.cfg_params = cfg_params

        nj_list = []
      
        nj_list = self.checkAlowedJob(jobs,nj_list)
 
        manageNewRunJobs(nj_list)   
       
        common.logger.message('Jobs '+str(nj_list)+' will be resubmitted')
        Submitter.__init__(self, cfg_params, nj_list, 'range')
 
        return

    def checkAlowedJob(self,jobs,nj_list):
        listRunField=[]
        run_jobToSave = {'status' :'C', \
                         'statusScheduler' : 'Created'}

        task=common._db.getTask(jobs)
        for job in task.jobs:
            st = job.runningJob['status']
            nj = job['id']
            if st in ['K','A','SE','E']:
                nj_list.append(int(nj))
                listRunField.append(run_jobToSave)
            elif st == 'C':
                common.logger.message('Job #'+`int(nj)`+' has status '+str(job.runningJob['statusScheduler'])+' not yet submitted!!!')
            elif st in ['SD','D']:
                common.logger.message('Job #'+`int(nj)`+' has status '+str(job.runningJob['statusScheduler'])+' must be retrieved before resubmission')
            else:
                common.logger.message('Job #'+`nj`+' has status '+str(job.runningJob['statusScheduler'])+' must be "killed" before resubmission')

        if len(nj_list) == 0 :
            msg='No jobs to resubmit'
            raise CrabException(msg)
        else:
            common._db.updateRunJob_(nj_list, listRunField ) 
            return nj_list
        
        if UseServer==1:  SubmitterServer.__init__(self, cfg_params, nj_list, 'range')

    def manageNewRunJobs(self,nj_list):
        """
        Get new running instances
        """
        common._db.newRunJobs(nj_list)
        return
