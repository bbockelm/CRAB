from Actor import *
from crab_exceptions import *
from crab_logger import Logger
import common
import string

class CleanerServer(Actor):
    def __init__(self, cfg_params):
        """
        constructor
        """
        self.cfg_params = cfg_params

    def check(self):
        """
        Check whether no job is still running or not yet retrieved
        """
        # get updated status from server 
        try:
            from StatusServer import StatusServer
            stat = StatusServer(self.cfg_params)
            stat.resynchClientSide()
        except:
            pass

    def run(self):
        """
        remove all

        if common.jobDB.nJobs()>0:
            self.check()

        countEnded = 0
        for nj in range(common.jobDB.nJobs()):
            if common.jobDB.status(nj) in ['Y','K', 'A', 'C']:
                countEnded += 1
        if countEnded == common.jobDB.nJobs():
            tempWorkSpace = common.work_space.topDir()
            common.scheduler.clean()
            common.work_space.delete()
            print ( 'crab. directory '+tempWorkSpace+' removed' )
        else:
            common.logger.message ( 'Impossible to remove: not all jobs are yet finished\n      (you maight kill these jobs and then clean the task)')
        """
        msg=''  
        msg+='functionality not yet available for the server. Work in progres \n' 
        msg+='planned for CRAB_2_5_0'
        common.logger.message(msg) 
        common.work_space.delete()
        print 'directory '+common.work_space.topDir()+' removed'
