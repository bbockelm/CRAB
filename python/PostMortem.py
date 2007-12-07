from Actor import *
from crab_util import *
import EdgLoggingInfo
import CondorGLoggingInfo
import common
import string, os

class PostMortem(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list

        if common.scheduler.boss_scheduler_name == 'condor_g':
            # create hash of cfg file
            self.hash = makeCksum(common.work_space.cfgFileName())
        else:
            self.hash = ''
        
        return
    
    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "PostMortem::run() called")

        for c, v in self.nj_list.iteritems():
            id = int(c)
            out = common.scheduler.loggingInfo(v)
            # job = common.job_list[id - 1]
            jobnum_str = '%06d' % (id)
            fname = common.work_space.jobDir() + '/' + self.cfg_params['CRAB.jobtype'].upper() + '_' + jobnum_str + '.loggingInfo'
            if os.path.exists(fname):
                common.logger.message('Logging info for job ' + str(id) + ' already present in '+fname+'\nRemove it for update')
                continue
            jdl = open(fname, 'w')
            for line in out: jdl.write(line)
            jdl.close()

            reason = ''
            ## SL this if-elif is the negation of OO! Mus disappear ASAP
            if common.scheduler.boss_scheduler_name == "edg" or common.scheduler.boss_scheduler_name == "glite" or common.scheduler.boss_scheduler_name == "glitecoll":
                loggingInfo = EdgLoggingInfo.EdgLoggingInfo()
                reason = loggingInfo.decodeReason(out)
            elif common.scheduler.boss_scheduler_name == "condor_g" :
                loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
                reason = loggingInfo.decodeReason(out)
            else :
                reason = out

            common.logger.message('Logging info for job '+ str(id) +': '+str(reason)+'\n      written to '+str(fname) )
            
            # ML reporting
            jobId = ''
            if common.scheduler.boss_scheduler_name == 'condor_g':
                jobId = str(id) + '_' + self.hash + '_' + v
            else:
                jobId = str(id) + '_' + v

            params = {'taskId': self.cfg_params['taskId'], 'jobId':  jobId, \
                      'sid': v,
                      'PostMortemCategory': loggingInfo.getCategory(), \
                      'PostMortemReason': loggingInfo.getReason()}
            self.cfg_params['apmon'].sendToML(params)
            pass

        return

