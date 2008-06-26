import os, common, string
from Actor import *
from crab_util import *

class Killer(Actor):
    def __init__(self, cfg_params, range):
        self.cfg_params = cfg_params
        self.range = range
        return

    def run(self):
        """
        The main method of the class: kill a complete task
        """
        common.logger.debug(5, "Killer::run() called")
        task = common._db.getTask(self.range)
        toBeKilled = []
        for job  in task.jobs:
           if ( job.runningJob['status'] in ['SS','R','S','SR','SW']):
               toBeKilled.append(job['jobId'])
           else:
               common.logger.message("Not possible to kill Job #"+str(job['jobId'])+" : Status is "+str(job.runningJob['statusScheduler']))
           pass

        if len(toBeKilled)>0:
            common.scheduler.cancel(toBeKilled)
            common.logger.message("Jobs killed "+str(toBeKilled))