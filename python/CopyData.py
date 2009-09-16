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
        
        ### default is the copy local
        self.copy_local = 1
        
        if common.logger.debug: print 'CopyData __init__() : \n'
        
        if (cfg_params.get('USER.copy_data',0) == '0') :
            msg  = 'Cannot copy output if it has not \n'
            msg += '\tbeen stored to SE via USER.copy_data=1'
            raise CrabException(msg)

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
        
        if (cfg_params.get("USER.storage_element").upper() != 'LOCAL'):
           self.copy_local = 0

        if common.logger.debug: 
            print "------ self.protocol = ", self.protocol
            print "------ USER.storage_element = ", cfg_params.get("USER.storage_element")
            print "------ self.copy_local = " , self.copy_local  
           

    def run(self):
        """
        default is the copy of output to the local dir res in crabDir
        """

        results = self.copy()
        self.parseResults( results )          
        return 

        
    def copy(self):
        """
        1) local copy:  USER.storage_element=local, the output will be copied under crab_working_dir/res dir
        2) copy to a remote SE: USER.storage_element, USER.storage_path and USER.user_remote_dir used as in the crab.cfg
           if the storage_path and the user_remote_dir are not specified by command line, the value written in the crab.cfg are used 
        """

        output_file = []
        tmp = self.cfg_params.get('CMSSW.output_file',None)
        if tmp.find(',') >= 0:
            [output_file.append(x.strip()) for x in tmp.split(',')]
        else: output_file.append( tmp.strip() )

        to_copy = {}
        results = {}
        
        to_copy = self.checkAvailableList(output_file)

        if (self.copy_local == 1):
            outputDir = self.cfg_params.get('USER.outputdir' ,common.work_space.resDir())
            print "Copy file locally:"
            print "------ outputDir = ",  outputDir
            dest = {"destinationDir": outputDir}
        else:
            from PhEDExDatasvcInfo import PhEDExDatasvcInfo
            stageout = PhEDExDatasvcInfo(self.cfg_params)
            endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()
            print "Copy file to remote SE:"
            print "------ endpoint = ", endpoint
            dest = {"destination": endpoint}
            
        
        for key in to_copy.keys():
            cmscpConfig = {
                        "source": key,
                        "inputFileList": to_copy[key],
                        "protocol": self.protocol
                      }  
            if (self.protocol == "srmv2"):
                cmscpConfig.setdefault("option", " -retry_timeout=480000 -retry_num=3 ")
            elif (self.protocol == "srm-lcg"):
                cmscpConfig.setdefault("option", " -b -D srmv2  -t 2400 --verbose ")
            elif (self.protocol == "rfio"):
                pass
            cmscpConfig.update(dest)
            print "------ source = ", key
            print "------ files = ", to_copy[key]
            if common.logger.debug: 
                print "------ cmscpConfig = ", cmscpConfig
            
            results.update(self.performCopy(cmscpConfig))
        return results
        
    def checkAvailableList(self, output_file):
        '''
        check if asked list of jobs 
        already produce output to move 
        returns a dictionary {with endpoint, fileList} 
        '''
      
        if common.logger.debug: print "CopyData in checkAvailableList() "
        # loop over jobs
        
        task=common._db.getTask(self.nj_list)
        allMatch={}
        to_copy={}
        
        for job in task.jobs:
            InfileList = ''
            if ( job.runningJob['status'] in ['E','UE'] and job.runningJob[ 'wrapperReturnCode'] == 0):
                id_job = job['jobId'] 
                if common.logger.debug: print "------ id_job = ", id_job
                endpoint = job.runningJob['storage']
                for of in output_file:
                     ### to be changed with os.path.basename(job.runningJob['lfn'])
                     files = numberFile(of, id_job)
                     InfileList += '%s,'%files
                if common.logger.debug: print "------ InfileList = ", InfileList
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
        if common.logger.debug: print "------ to_copy = ", to_copy 
        return to_copy 

    def performCopy(self, dict):
        """
        call the cmscp class and do the copy
        """
        print "--->>> in performCopy"
        
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
