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

        jStatus=common._db.queryRunJob('status','all')
        human_status = common._db.queryRunJob('statusScheduler','all')
        toBeKilled = []
        for id in self.range:
            if id not in  common._db.nJobs("list"):
                common.logger.message("Warning: job # "+str(id)+" doesn't exists! Not possible to kill it.")
            else:
                if ( jStatus[id-1] in ['SS','R']):
                    toBeKilled.append(id)
                else:
                    common.logger.message("Not possible to kill Job #"+str(id)+" : Status is "+str(human_status[id-1]))
                pass
            pass

        if len(toBeKilled)>0:
            common.scheduler.cancel(toBeKilled)
            common.logger.message("Jobs killed "+str(toBeKilled))
