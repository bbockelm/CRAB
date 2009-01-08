from Actor import *
import common
import string

class Checker(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser
        seWhiteList = cfg_params.get('EDG.se_white_list',[])
        seBlackList = cfg_params.get('EDG.se_black_list',[])
        self.blackWhiteListParser = SEBlackWhiteListParser(seWhiteList, seBlackList, common.logger)
        self.datasetpath=self.cfg_params['CMSSW.datasetpath']
        if string.lower(self.datasetpath)=='none':
            self.datasetpath = None
        return

    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "Checker::run() called")

        if len(self.nj_list)==0:
            common.logger.debug(5, "No jobs to check")
            return
        
        task=common._db.getTask(self.nj_list)
        allMatch={}

        for job in task.jobs:
            id_job = job['jobId'] 
            jobDest = job['dlsDestination']
            if not jobDest: jobDest=[]
            dest = self.blackWhiteListParser.cleanForBlackWhiteList(jobDest, True)

            # only if some dest i s available or if dataset is None
            if len(dest) > 0 or not self.datasetpath: 
                if ','.join(dest) in allMatch.keys():
                    common.logger.message("As previous job: "+str(allMatch[','.join(dest)]))
                else:
                    match = common.scheduler.listMatch(dest, True)
                    allMatch[','.join(dest)] = match 
                    if len(match)>0:
                        common.logger.message("Found "+str(len(match))+" compatible site(s) for job "+str(id_job)+" : "+str(match))
                    else:
                        common.logger.message("No compatible site found, will not submit jobs "+str(id_job))
                    pass
                pass
            else:
                common.logger.message("No compatible site found, will not submit jobs "+str(id_job))
        return
