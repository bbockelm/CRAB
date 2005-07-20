from Actor import *
from crab_util import *
import common

class Submitter(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        return
    
    def run(self):
        """
        The main method of the class.
        """

        common.logger.debug(5, "Submitter::run() called")
        
        # run a list-match on first job
        firstJob=self.nj_list[0]
        match = common.scheduler.listMatch(firstJob)

        common.logger.message("Found compatible resources")

        # Loop over jobs

        njs = 0
        for nj in self.nj_list:
            st = common.jobDB.status(nj)
            if st != 'C' and st != 'K':
                long_st = crabJobStatusToString(st)
                msg = "Job # %d is not submitted: status %s"%(nj+1, long_st)
                common.logger.message(msg)
                continue

            common.logger.message("Submitting job # "+`(nj+1)`)

            jid = common.scheduler.submit(nj)

            common.jobDB.setStatus(nj, 'S')
            common.jobDB.setJobId(nj, jid)
            njs += 1
            pass

        ####
        
        common.jobDB.save()

        msg = '\nTotal of %d jobs submitted'%njs
        if njs != len(self.nj_list) :
            msg += ' (from %d requested).'%(len(self.nj_list))
            pass
        else:
            msg += '.'
            pass
        common.logger.message(msg)
        return
    
