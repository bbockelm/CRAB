from Actor import *
import common

class Checker(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        return
    
    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "Checker::run() called")

        if len(self.nj_list)==0:
            common.logger.debug(5, "No jobs to check")
            return

        # run a list-match on first job
        for nj in self.nj_list:
            match = common.scheduler.listMatch(nj)
            flag = ''
            if not match: flag=' NOT '
            common.logger.message("Job #"+str(nj+1)+" does "+flag+" matches resources")
            pass

        return
