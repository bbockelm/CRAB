from crab_exceptions import *
from crab_logger import Logger
from StatusBoss import StatusBoss
from Status import Status
import common
import string

class Cleaner:
    def __init__(self, cfg_params):
        """
        constructor
        """
        self.status = StatusBoss(cfg_params)

    def check(self):
        """
        Check whether no job is still running or not yet retrieved
        """

        self.status.compute() # compute the status

        ## SL: What a ugly interface (I use here). I should try a dictionary or something similar...
        (ToTjob,countCreated,countReady,countSche,countRun,countCleared,countAbort,countCancel,countDone) = self.status.status()

        JobsOnGrid = countRun+countSche+countReady # job still on the grid
        if JobsOnGrid or countDone:
            msg = "There are still "
            if JobsOnGrid:
                msg= msg+str(JobsOnGrid)+" jobs submitted. Kill them '-kill' before '-clean'"
            if (JobsOnGrid and countDone):
                msg = msg + "and \nalso"
            if countDone:
                msg= msg+str(countDone)+" jobs Done. Get their outputs '-getoutput' before '-clean'"
            raise CrabException(msg)

        pass

    def clean(self):
        """
        remove all
        """
        if common.jobDB.nJobs()>0:
            self.check()

        # here I should first purge boss DB if central
        print 'directory '+common.work_space.topDir()+' removed'
        common.logger.close()
        common.work_space.delete()
