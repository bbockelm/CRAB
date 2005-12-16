from Actor import *
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
            if self.flag_useboss == 1 :
                id = common.scheduler.boss_SID(nj)
                #print "id = ", id 
            #else: 
            #    id = common.jobDB.jobId(nj) 
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
            common.logger.message('Logging info for job '+str(nj)+' written to '+jdl_fname)
            
            pass

        return

