from Actor import *
import common

class Submitter(Actor):
    def __init__(self, cfg_params, nsjobs):
        self.cfg_params = cfg_params
        self.nsjobs = nsjobs
        return
    
    def run(self):
        """
        The main method of the class.
        """

        common.logger.debug(5, "Submitter::run() called")

        total_njobs = len(common.job_list)
        if total_njobs == 0 :
            msg = '\nTotal of 0 jobs submitted -- no created jobs found.\n'
            msg += "Maybe you forgot '-create' or '-continue' ?\n"
            common.logger.message(msg)
            return

        # Loop over jobs

        njs = 0
        for nj in range(total_njobs):
            if njs == self.nsjobs : break
            st = common.jobDB.status(nj)
            if st != 'C': continue

            common.logger.debug(6, "Submitter::run(): job # "+`nj`)

            jid = common.scheduler.submit(nj)

            common.jobDB.setStatus(nj, 'S')
            common.jobDB.setJobId(nj, jid)
            njs = njs + 1
            pass

        ####
        
        common.jobDB.save()

        msg = '\nTotal of %d jobs submitted'%njs
        if njs != self.nsjobs: msg = msg + ' from %d requested'%self.nsjobs
        msg = msg + '.\n'
        common.logger.message(msg)
        return
    
