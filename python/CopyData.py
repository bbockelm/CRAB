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
        common.logger.debug("CopyData __init__() : \n")
        
        if (cfg_params.get('USER.copy_data',0) == '0') :
            msg  = 'Cannot copy output if it has not \n'
            msg += '\tbeen stored to SE via USER.copy_data=1'
            raise CrabException(msg)
            
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        
        ### default is the copy local
        self.copy_local = 1
        self.dest_se = cfg_params.get("CRAB.dest_se", 'local')
        self.dest_endpoint = cfg_params.get("CRAB.dest_endpoint", 'local')
        
        if ((self.dest_se != 'local') and (self.dest_endpoint != 'local')):
            msg  = 'You can specify only a parameter with CopyData option \n'
            msg += '1) The dest_se in the case of official CMS SE (i.e -dest_se=T2_IT_Legnaro)\n'
            msg += '2) The complete endpoint for not official SE (i.e -dest_endpoint=srm://<se_name>:<port>/xxx/yyy/)\n'
            msg += '3) if you do not specify parameters, the output will be copied in your UI under crab_working_dir/res \n' 
            raise CrabException(msg)
            
        if ((self.dest_se != 'local') or (self.dest_endpoint != 'local')):
           self.copy_local = 0

        # update local DB
        if StatusObj:# this is to avoid a really strange segv
            StatusObj.query(display=False)
      
        protocolDict = { 'CAF'      : 'rfio' , \
                         'LSF'      : 'rfio' , \
                         'CONDOR_G' : 'srmv2' , \
                         'GLITE'    : 'srm-lcg' , \
                         'CONDOR'   : 'srmv2',  \
                         'SGE'      : 'srmv2', \
                         'ARC'      : 'srmv2' \
                       }  
        self.protocol = protocolDict[common.scheduler.name().upper()]  
        
        common.logger.debug("------ self.protocol = " + self.protocol)
        common.logger.debug("------ USER.storage_element = " + cfg_params.get("USER.storage_element"))
        common.logger.debug("------ self.copy_local = " + str(self.copy_local))  
           

    def run(self):
        """
        default is the copy of output to the local dir res in crabDir
        """

        results = self.copy()
        self.parseResults( results )          
        return 

    def copy(self):
        """
        1) local copy: it is the default. The output will be copied under crab_working_dir/res dir
        2) copy to a remote SE specifying -dest_se (official CMS remote SE) 
           or -dest_endpoint (not official, needed the complete endpoint) 
        """

        to_copy = {}
        results = {}
        
        lfn, to_copy = self.checkAvailableList()

        if (self.copy_local == 1):
            outputDir = self.cfg_params.get('USER.outputdir' ,common.work_space.resDir())
            print "Copy file locally:"
            print "------ outputDir = ",  outputDir
            dest = {"destinationDir": outputDir}
        else:
            if (self.dest_se != 'local'):
                from PhEDExDatasvcInfo import PhEDExDatasvcInfo
                phedexCfg={'storage_element': self.dest_se}
                stageout = PhEDExDatasvcInfo(config=phedexCfg)
                self.endpoint = stageout.getStageoutPFN()
                tmp = string.lstrip(lfn,'/store') + '/'
                common.logger.debug("------ tmp = " + tmp)
                self.endpoint = self.endpoint + tmp
            else:
                self.endpoint = self.dest_endpoint
                                 
            print "Copy file to remote SE:"
            print "------ endpoint = ", self.endpoint
            dest = {"destination": self.endpoint}
            
        
        for key in to_copy.keys():
            cmscpConfig = {
                        "source": key,
                        "inputFileList": to_copy[key],
                        "protocol": self.protocol
                      }  
            #if (self.protocol == "srmv2"):
            #    cmscpConfig.setdefault("option", " -retry_timeout=480000 -retry_num=3 ")
            #elif (self.protocol == "srm-lcg"):
            #    cmscpConfig.setdefault("option", " -b -D srmv2  -t 2400 --verbose ")
            #elif (self.protocol == "rfio"):
            #    pass
            cmscpConfig.update(dest)
            print "------ source = ", key
            print "------ files = ", to_copy[key]
            common.logger.debug("------ cmscpConfig = " + str(cmscpConfig))
            
            results.update(self.performCopy(cmscpConfig))
        return results
        
    def checkAvailableList(self):
        '''
        check if asked list of jobs 
        already produce output to move 
        returns a dictionary {with endpoint, fileList} 
        '''
      
        common.logger.debug("CopyData in checkAvailableList() ")
        # loop over jobs
        
        task=common._db.getTask(self.nj_list)
        allMatch={}
        to_copy={}
        
        for job in task.jobs:
            InfileList = ''
            if ( job.runningJob['status'] in ['E','UE'] and job.runningJob[ 'wrapperReturnCode'] == 0):
                id_job = job['jobId'] 
                common.logger.debug("------ id_job = " + str(id_job))
                endpoint = job.runningJob['storage']
                output_files = job.runningJob['lfn']
                common.logger.debug("------ output_files = " + str(job.runningJob['lfn']))
                for file in output_files:
                     InfileList += '%s,'%os.path.basename(file)
                     lfn = os.path.dirname(file)
                common.logger.debug("------ InfileList = " + str(InfileList))
                if to_copy.has_key(endpoint):     
                    to_copy[endpoint] = to_copy[endpoint] + InfileList
                else:
                    to_copy[endpoint] = InfileList
                     
            elif ( job.runningJob['status'] in ['E','UE'] and job.runningJob['wrapperReturnCode'] != 0):
                common.logger.info("Not possible copy outputs of Job # %s : Wrapper Exit Code is %s" \
                                      %(str(job['jobId']),str(job.runningJob['wrapperReturnCode'])))
            else: 
                common.logger.info("Not possible copy outputs of Job # %s : Status is %s" \
                                       %(str(job['jobId']),str(job.runningJob['statusScheduler'])))
            pass

        if (len(to_copy) == 0) :
            raise CrabException("No files to copy")
        
        for key in to_copy.keys():
            to_copy[key] = to_copy[key][:-1]
        common.logger.debug("------ to_copy = " + str(to_copy)) 
        return lfn, to_copy 

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
