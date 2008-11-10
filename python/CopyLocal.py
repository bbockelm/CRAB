from Actor import *
from crab_util import *
from crab_exceptions import *
from crab_logger import Logger
import common
import string

class CopyLocal(Actor):
    def __init__(self, cfg_params, nj_list):
        """
        constructor
        """
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        if (cfg_params.get('USER.copy_data',0) == '0') :
            raise CrabException("Cannot copy output locally if it has not \
                                 been stored to SE via USER.copy_data=1")

    def run(self):

        results = self.copyLocal() 
        
        self.parseResults( results )          

        return 

    def copyLocal(self):
        """
        prepare to copy the pre staged output to local
        """
        from PhEDExDatasvcInfo import PhEDExDatasvcInfo

        stageout = PhEDExDatasvcInfo(self.cfg_params)
        endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()

        # FIXME DS. we'll use the proper DB info once filled..
        # output files to be returned via sandbox or copied to SE
        self.output_file = []
        tmp = self.cfg_params.get('CMSSW.output_file',None)
        if tmp.find(',') >= 0:
            [self.output_file.append(x.strip()) for x in tmp.split(',')]
        else: self.output_file.append( tmp.strip() )

        # loop over jobs
        task=common._db.getTask(self.nj_list)
        allMatch={}
        InfileList = ''
        for job in task.jobs:
            id_job = job['jobId'] 
            if ( job.runningJob['status'] in ['E','UE'] and job.runningJob[ 'wrapperReturnCode'] == 0):
                for of in self.output_file:
                    a,b=of.split('.')
                    InfileList = '%s_%s.%s%s'%(a,id_job,b,',')
            elif ( job.runningJob['status'] in ['E','UE'] and job.runningJob['wrapperReturnCode'] != 0):
                common.logger.message("Not possible copy outputs of Job # %s : Wrapper Exit Code is %s" \
                                      %(str(job['jobId']),str(job.runningJob['wrapperReturnCode'])))
            else: 
                common.logger.message("Not possible copy outputs of Job # %s : Status is %s" \
                                       %(str(job['jobId']),str(job.runningJob['statusScheduler'])))
            pass

        if (InfileList == '') :
            raise CrabException("No files to be copyed")
       
        self.outDir = self.cfg_params.get('USER.outputdir' ,common.work_space.resDir())
        print InfileList 
        cmscpConfig = {
                        "source": endpoint,
                        "destinationDir": self.outDir,
                        "inputFileList": InfileList[:-1],
                        "protocol": 'srm-lcg',
                        "option": '-b -D srmv2  -t 2400 --verbose'
                      }  

        results = self.performCopy(cmscpConfig)
     
        return results

    def copyRemote(self):
        """ 
        prepare to copy from local to SE
        """ 
        results =  'still to be implemented' 

        # to be implemeted
        cmscpConfig = {
                        "source": '',
                        "inputFileList":'', 
                        "protocol":'',
                        "option":''
                      }  

   #     results = self.performCopy(cmscpConfig)
     
        return  results

    def performCopy(self, dict):
        """
        call the cmscp class and do the copy
        """
        from cmscp import cmscp

        doCopy = cmscp(dict)

        start = time.time()
        results = doCopy.run()
        stop = time.time()

        common.logger.debug(1, "CopyLocal Time: "+str(stop - start))
        common.logger.write("CopyLocal time :"+str(stop - start))

        return results

    def parseResults(self,results):
        ''' 
        take the results dictionary and
        print the results 
        '''
        for file, dict in results : 
            if file:
                txt = 'success' 
                if dict['erCode'] != 'failed':
                msg = 'Copy %s for file: %s \n'%(txt,file)
                if txt == 'failed': msg += 'Copy failed because : %s'%dict['reason']
            common.logger.message( msg )
        return 
