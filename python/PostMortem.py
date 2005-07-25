from Actor import *
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
            out = common.scheduler.loggingInfo(nj)
            job = common.job_list[nj]
            jdl_fname = string.replace(job.jdlFilename(),'jdl','loggingInfo')
            if os.path.exists(jdl_fname):
                common.logger.message('Logging info for job '+str(nj+1)+' already present in '+jdl_fname+' Remove it for update')
                continue
            jdl = open(jdl_fname, 'w')
            for line in out: jdl.write(line)
            jdl.close()
            common.logger.message('Logging info for job '+str(nj+1)+' written to '+jdl_fname)
            
            pass

        return

