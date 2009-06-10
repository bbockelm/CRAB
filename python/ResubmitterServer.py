from SubmitterServer import SubmitterServer
from Resubmitter  import Resubmitter
import common
from crab_util import *

class ResubmitterServer(SubmitterServer, Resubmitter):
    def __init__(self, cfg_params, jobs):
        self.cfg_params = cfg_params

        nj_list = []

        # get updated status from server 
        try:
            from StatusServer import StatusServer
            stat = StatusServer(self.cfg_params)
            stat.resynchClientSide()
        except:
            pass

        nj_list = self.checkAllowedJob(jobs,nj_list)
       
        SubmitterServer.__init__(self, cfg_params, nj_list, 'range')
 
        return

        
