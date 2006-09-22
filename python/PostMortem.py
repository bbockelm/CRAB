from Actor import *
from crab_util import *
import EdgLoggingInfo
import CondorGLoggingInfo
import common
import string, os

class PostMortem(Actor):
    def __init__(self, cfg_params, nj_list, use_boss):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        self.flag_useboss = use_boss
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
            #nj parte da 1 --> nj = internal_id di boss 
            id = common.scheduler.boss_SID(nj)
            out = common.scheduler.loggingInfo(id)
            # job_list inizia a contare da zero
            job = common.job_list[nj-1]
            #print "job.jdlFilename()", job.jdlFilename()
            jdl_fname = string.replace(job.jdlFilename(),'jdl','loggingInfo')
            #print "jdl_fname = ", jdl_fname 
            if os.path.exists(jdl_fname):
                common.logger.message('Logging info for job '+str(nj)+' already present in '+jdl_fname+' Remove it for update')
                continue
            jdl = open(jdl_fname, 'w')
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

            common.logger.message('Logging info for job '+str(nj)+': '+reason+'\n      written to '+jdl_fname)
            
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

