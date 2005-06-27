from Actor import *
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

        # Loop over jobs

        for nj in self.nj_list:
            st = common.jobDB.status(nj)

            common.logger.debug(6, "Submitter::run(): job # "+`nj`)

            jid = common.scheduler.submit(nj)

            common.jobDB.setStatus(nj, 'S')
            common.jobDB.setJobId(nj, jid)
            pass

        ####
        
        common.jobDB.save()

        msg = '\nTotal of %d jobs submitted'%len(self.nj_list)+'.'
        common.logger.message(msg)
        return
    
