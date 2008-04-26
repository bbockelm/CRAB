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
        
        task=common._db.getTask(self.nj_list)
        allMatch={}
        for job in task.jobs:
            id_job = job['id'] 
            dest = job['dlsDestination']
            if dest in allMatch.keys():
                common.logger.message("As previous job: "+str(allMatch[dest]))
            else:
                match = common.scheduler.listMatch(dest)
             #   allMatch[job['dlsDestination']]= match 
                if len(match)>0:
                    common.logger.message("Found "+str(len(match))+" compatible site(s) for job "+str(id_job)+" : "+str(match))
                else:
                    common.logger.message("No compatible site found, will not submit jobs "+str(id_job))

        return
