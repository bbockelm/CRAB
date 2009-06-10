from Actor import *
from crab_util import *
from crab_exceptions import *
import common
import string

class CopyData(Actor):
    def __init__(self, cfg_params, nj_list, StatusObj):
        """
        constructor
        """
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        if (cfg_params.get('USER.copy_data',0) == '0') :
            msg  = 'Cannot copy output locally if it has not \n'
            msg += '\tbeen stored to SE via USER.copy_data=1'
            raise CrabException(msg)

        # update local DB
        if StatusObj:# this is to avoid a really strange segv
            StatusObj.query(display=False)
      
        protocolDict = { 'CAF'      : 'rfio' , \
                         'LSF'      : 'rfio' , \
                         'CONDOR_G' : 'srmv2' , \
                         'GLITE'    : 'srm-lcg' , \
                         'CONDOR'    : 'srmv2',  \
                         'SGE'      : 'srmv2', \
                         'ARC'      : 'srmv2' \
                       }  
        self.protocol = protocolDict[common.scheduler.name().upper()]  
        self.destinationTier = None
        self.destinationDir= None
        self.destinationTURL = None
        target =None 
        if target:
	    if  target.find('://'):
	        self.destinationTier,self.destinationDir = target.split('://') 
	    elif target.find(':'):
	        if (target.find('T2_') + target.find('T1_') + target.find('T3_') >= -1) :          
	            self.destinationTier=target.split(":")[0]
                else:
                    self.destinationTURL = target
            else:
                self.destinationTURL = target
        else:
            pass
        

    def run(self):
 
        results = self.copyLocal() 
        self.parseResults( results )          

        return 

    def copyLocal(self):
        """
        prepare to copy the pre staged output to local
        """
        # FIXME DS. we'll use the proper DB info once filled..
        # output files to be returned via sandbox or copied to SE
        output_file = []
        tmp = self.cfg_params.get('CMSSW.output_file',None)
        if tmp.find(',') >= 0:
            [output_file.append(x.strip()) for x in tmp.split(',')]
        else: output_file.append( tmp.strip() )

        InfileList = self.checkAvailableList(output_file)

        from PhEDExDatasvcInfo import PhEDExDatasvcInfo

        stageout = PhEDExDatasvcInfo(self.cfg_params)
        endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()

        if not self.destinationTURL:
            self.destinationTURL = self.cfg_params.get('USER.outputdir' ,common.work_space.resDir())
         
        cmscpConfig = {
                        "source": endpoint,
                        "destinationDir": self.destinationTURL,
                        "inputFileList": InfileList,
                        "protocol": self.protocol
                      }  
        if self.protocol is "srmv2":
            cmscpConfig.setdefault("option", " -retry_timeout=480000 -retry_num=3 ")
        elif self.protocol is "srm-lcg":
            cmscpConfig.setdefault("option", "-b -D srmv2  -t 2400 --verbose")
        elif self.protocol is "rfio":
            pass

        results = self.performCopy(cmscpConfig)
     
        return results

    def copyRemote(self):
        """ 
        prepare to copy from SE1 o SE2
        """ 
        results = 'work in progress' 
        '''   
	if self.destinationTier :
		if (self.destinationDir)
			self.outDir = Phedex(self.destinationTier, self.destinationDir)
		else:
			self.uno, self.lfn ... = Phedex(self.destinationTier)
			self.outDir= self.uno + self.lfn
	else:	
                self.outDir = self.destinationTURL
		pass


        # to be implemeted
        cmscpConfig = {
                        "source": '',
                        "inputFileList":'', 
                        "protocol":'',
                        "option":''
                      }  

        results = self.performCopy(cmscpConfig)
        '''
        return  results

    def checkAvailableList(self, output_file):
        '''
        check if asked list of jobs 
        already produce output to move  
        '''
      
        # loop over jobs
        task=common._db.getTask(self.nj_list)
        allMatch={}
        InfileList = ''
        for job in task.jobs:
            id_job = job['jobId'] 
            if ( job.runningJob['status'] in ['E','UE'] and job.runningJob[ 'wrapperReturnCode'] == 0):
                for of in output_file:
                    InfileList += '%s,'%numberFile(of, id_job)
            elif ( job.runningJob['status'] in ['E','UE'] and job.runningJob['wrapperReturnCode'] != 0):
                common.logger.info("Not possible copy outputs of Job # %s : Wrapper Exit Code is %s" \
                                      %(str(job['jobId']),str(job.runningJob['wrapperReturnCode'])))
            else: 
                common.logger.info("Not possible copy outputs of Job # %s : Status is %s" \
                                       %(str(job['jobId']),str(job.runningJob['statusScheduler'])))
            pass

        if (InfileList == '') :
            raise CrabException("No files to copy")
          
        return InfileList[:-1] 


    def performCopy(self, dict):
        """
        call the cmscp class and do the copy
        """
        from cmscp import cmscp
        doCopy = cmscp(dict)

        common.logger.info("Starting copy...")

        start = time.time()
        results = doCopy.run()
        stop = time.time()

        common.logger.debug("CopyLocal Time: "+str(stop - start))

        return results

    def parseResults(self,results):
        ''' 
        take the results dictionary and
        print the results 
        '''
        for file, dict in results.items() : 
            if file:
                txt = 'success' 
                if dict['erCode'] != '0': txt = 'failed'
                msg = 'Copy %s for file: %s \n'%(txt,file)
                if txt == 'failed': msg += 'Copy failed because : %s'%dict['reason']
                common.logger.info( msg )
        return 
