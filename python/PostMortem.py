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
        return
    
    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "PostMortem::run() called")

        if len(self.nj_list)==0:
            common.logger.debug(5, "No jobs to check")
            return

        # run a list-match on first job
        for nj in self.nj_list:
            id = common.scheduler.boss_SID(nj)
            out = common.scheduler.loggingInfo(id)
            job = common.job_list[nj-1]
            jobnum_str = '%06d' % (int(nj))
            fname = common.work_space.jobDir() + '/' + self.cfg_params['CRAB.jobtype'].upper() + '_' + jobnum_str + '.loggingInfo'
            if os.path.exists(fname):
                common.logger.message('Logging info for job '+str(nj)+' already present in '+fname+' Remove it for update')
                continue
            jdl = open(fname, 'w')
            for line in out: jdl.write(line)
            jdl.close()

            reason = ''
            if common.scheduler.boss_scheduler_name == "edg" :
                loggingInfo = EdgLoggingInfo.EdgLoggingInfo()
                reason = loggingInfo.decodeReason(out)
            elif common.scheduler.boss_scheduler_name == "condor_g" :
                loggingInfo = CondorGLoggingInfo.CondorGLoggingInfo()
                reason = loggingInfo.decodeReason(out)
            else :
                reason = out

            common.logger.message('Logging info for job '+str(nj)+': '+reason+'\n      written to '+fname)
            
            # ML reporting
            jobId = ''
            if common.scheduler.boss_scheduler_name == 'condor_g':
                # create hash of cfg file
                hash = makeCksum(common.work_space.cfgFileName())
                jobId = str(nj) + '_' + hash + '_' + id
            else:
                jobId = str(nj) + '_' + id

            params = {'taskId': self.cfg_params['taskId'], 'jobId':  jobId, \
                      'sid': id,
                      'PostMortemCategory': loggingInfo.getCategory(), \
                      'PostMortemReason': loggingInfo.getReason()}
            self.cfg_params['apmon'].sendToML(params)
            pass

        return

