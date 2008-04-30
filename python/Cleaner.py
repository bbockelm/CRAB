from Actor import *
from crab_exceptions import *
from crab_logger import Logger
from Status import Status
import common
import string

class Cleaner(Actor):
    def __init__(self, cfg_params):
        """
        constructor
        """
        self.status = Status(cfg_params)

    def check(self):
        """
        Check whether no job is still running or not yet retrieved
        """

        task = common._db.getTask()
        upTask = common.scheduler.queryEverything(task['id'])
        self.status.compute(upTask) # compute the status

        countSub  = len(common._db.queryAttrRunJob({'status':'S'},'status'))
        countDone = len(common._db.queryAttrRunJob({'status':'SD'},'status'))

        if countSub or countDone:
            msg = "There are still "
            if countSub:
                msg= msg+str(countSub)+" jobs submitted. Kill them '-kill' before '-clean'"
            if (countSub and countDone):
                msg = msg + "and \nalso"
            if countDone:
                msg= msg+str(countDone)+" jobs Done. Get their outputs '-getoutput' before '-clean'"
            raise CrabException(msg)

        pass

    def run(self):
        """
        remove all
        """
        if common._db.nJobs()>0:
            self.check()

        # here I should first purge boss DB if central
        #common.scheduler.clean()
        common.work_space.delete()
        print 'directory '+common.work_space.topDir()+' removed'
