from Submitter import Submitter
import common
from crab_util import *
from crab_exceptions import *

class Resubmitter(Submitter):
    def __init__(self, cfg_params, jobs):
        self.cfg_params = cfg_params

        nj_list = []

        nj_list = self.checkAllowedJob(jobs,nj_list)

        common.logger.info('Jobs '+str(nj_list)+' will be resubmitted')
        Submitter.__init__(self, cfg_params, nj_list, 'range')

        return

    def checkAllowedJob(self,jobs,nj_list):
        listRunField=[]

        task=common._db.getTask(jobs)
        for job in task.jobs:
            st = job.runningJob['state']
            nj = int(job['jobId'])
            if st in ['KillSuccess','SubFailed','Cleared','Aborted']:
                #['K','A','SE','E','DA','NS']:
                nj_list.append(nj)
            elif st == 'Created':
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' not yet submitted: use -submit')
            elif st in ['Terminated']:
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' must be retrieved (-get) before resubmission')
            else:
                common.logger.info('Job #'+`nj`+' last action was '+str(job.runningJob['state'])+' actual status is '\
                        +str(job.runningJob['statusScheduler'])+' must be killed (-kill) before resubmission')
                if (job.runningJob['state']=='KillRequest'): common.logger.info('\t\tthe previous Kill request is being processed')


        if len(nj_list) == 0 :
            msg='No jobs to resubmit'
            raise CrabException(msg)

        common._db.updateJob_(nj_list, [{'closed':'N'}]*len(nj_list))
        # Get new running instances
        common._db.newRunJobs(nj_list)

        return nj_list
