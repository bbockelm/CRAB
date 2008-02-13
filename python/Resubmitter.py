from Submitter import Submitter
import common
from crab_util import *

class Resubmitter(Submitter):
    def __init__(self, cfg_params, jobs, UseServer=0):
        nj_list = []
        for nj in jobs:
            st = common.jobDB.status(int(nj)-1)
            if st in ['K','A']:
                nj_list.append(int(nj)-1)
                common.jobDB.setStatus(int(nj)-1,'C')
            elif st == 'Y':
                common.scheduler.moveOutput(nj)
                nj_list.append(int(nj)-1)
                st = common.jobDB.setStatus(int(nj)-1,'RC')
            elif st in ['C','X']:
                common.logger.message('Job #'+`int(nj)`+' has status '+crabJobStatusToString(st)+' not yet submitted!!!')
                pass
            elif st == 'D':
                common.logger.message('Job #'+`int(nj)`+' has status '+crabJobStatusToString(st)+' must be retrieved before resubmission')
            else:
                common.logger.message('Job #'+`nj`+' has status '+crabJobStatusToString(st)+' must be "killed" before resubmission')
                pass


        if len(nj_list) != 0:
            nj_list.sort()

            # remove job ids from the submission history file (for the server) # Fabio
            if (UseServer == 1):
                file = open(common.work_space.shareDir()+'/submit_directive','r')
                prev_subms = str(file.readlines()[0]).split('\n')[0]
                file.close()

                new_subms = []
                if prev_subms != 'all':
                    # remove the jobs in nj_list from the history
                    new_subms = [ j for j in eval(prev_subms) not in nj_list ]

                file = open(common.work_space.shareDir()+'/submit_directive','w')
                file.write(str(new_subms))
                file.close()
            pass
        pass
        Submitter.__init__(self, cfg_params, nj_list, 'range')
        pass
