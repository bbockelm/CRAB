from Actor import *
from crab_util import *
#import EdgLoggingInfo
#import CondorGLoggingInfo
import common
import string, os

class PostMortem(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list

        self.fname_base = common.work_space.jobDir() + self.cfg_params['CRAB.jobtype'].upper() + '_' 

        return
    
    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "PostMortem::run() called")

        self.collectLogging()


    def collectLogging(self):
        for id in self.nj_list:
            fname = self.fname_base + str(id) + '.LoggingInfo'
            if os.path.exists(fname):
                common.logger.message('Logging info for job ' + str(id) + ' already present in '+fname+'\nRemove it for update')
                continue
            common.scheduler.loggingInfo(id,self.fname_base+str(id))
            fl = open(fname, 'r')
            out = "".join(fl.readlines())  
            fl.close()
            reason = self.decodeLogging(out)
            common.logger.message('Logging info for job '+ str(id) +': '+str(reason)+'\n      written to '+str(fname) )
        return
        
    def decodeLogging(self, out):
        """
        """
        return  common.scheduler.decodeLogInfo(out)

