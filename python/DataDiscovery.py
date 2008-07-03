#!/usr/bin/env python
import exceptions
import DBSAPI.dbsApi
from DBSAPI.dbsApiException import * 
import common
from crab_util import *
import os 


# #######################################
class DBSError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass
    
    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)

# #######################################
class DBSInvalidDataTierError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass
    
    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)

# #######################################
class DBSInfoError:
    def __init__(self, url):
        print '\nERROR accessing DBS url : '+url+'\n'
        pass

# ####################################
class DataDiscoveryError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
class NotExistingDatasetError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
class NoDataTierinProvenanceError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
# class to find and extact info from published data
class DataDiscovery:
    def __init__(self, datasetPath, cfg_params, skipAnBlocks):

        #       Attributes
        self.datasetPath = datasetPath
        self.cfg_params = cfg_params
        self.skipBlocks = skipAnBlocks

        self.eventsPerBlock = {}  # DBS output: map fileblocks-events for collection
        self.eventsPerFile = {}   # DBS output: map files-events
        self.blocksinfo = {}      # DBS output: map fileblocks-files 
        self.maxEvents = 0        # DBS output: max events
        self.parent = {}       # DBS output: max events

# ####################################
    def fetchDBSInfo(self):
        """
        Contact DBS
        """
        ## get DBS URL
        global_url="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        caf_url = "http://cmsdbsprod.cern.ch/cms_dbs_caf_analysis_01/servlet/DBSServlet"
        dbs_url_map  =   {'glite':    global_url,
                          'glitecoll':global_url,\
                          'condor':   global_url,\
                          'condor_g': global_url,\
                          'glidein':  global_url,\
                          'lsf':      global_url,\
                          'caf':      caf_url,\
                          'sge':      global_url
                          }

        dbs_url_default = dbs_url_map[(common.scheduler.name()).lower()]
        dbs_url=  self.cfg_params.get('CMSSW.dbs_url', dbs_url_default)
        common.logger.debug(3,"Accessing DBS at: "+dbs_url)

        ## check if runs are selected
        runselection = []
        if (self.cfg_params.has_key('CMSSW.runselection')):
            runselection = parseRange2(self.cfg_params['CMSSW.runselection'])

        common.logger.debug(6,"runselection is: %s"%runselection)
        ## service API
        args = {}
        args['url']     = dbs_url
        args['level']   = 'CRITICAL'

        ## check if has been requested to use the parent info
        useParent = self.cfg_params.get('CMSSW.use_parent',False)

        ## check if has been asked for a non default file to store/read analyzed fileBlocks   
        defaultName = common.work_space.shareDir()+'AnalyzedBlocks.txt'  
        fileBlocks_FileName = os.path.abspath(self.cfg_params.get('CMSSW.fileblocks_file',defaultName))
 
        api = DBSAPI.dbsApi.DbsApi(args)
        allowedRetriveValue = ['retrive_parent', 
                               'retrive_block',
                               'retrive_lumi',
                               'retrive_run'
                               ]
        try:
            if len(runselection) <= 0 :
                if useParent:
                    files = api.listFiles(path=self.datasetPath, retriveList=allowedRetriveValue)
                    common.logger.debug(5,"Set of input parameters used for DBS query : \n"+str(allowedRetriveValue)) 
                    common.logger.write("Set of input parameters used for DBS query : \n"+str(allowedRetriveValue)) 
                else:
                    files = api.listDatasetFiles(self.datasetPath)
            else :
                files=[]
                for arun in runselection:
                    try:
                        filesinrun = api.listFiles(path=self.datasetPath,retriveList=allowedRetriveValue,runNumber=arun)
                        files.extend(filesinrun)
                    except:
                        msg="WARNING: problem extracting info from DBS for run %s "%arun
                        common.logger.message(msg)
                        pass

        except DbsBadRequest, msg:
            raise DataDiscoveryError(msg)
        except DBSError, msg:
            raise DataDiscoveryError(msg)

        anFileBlocks = []
        if self.skipBlocks: anFileBlocks = readTXTfile(self, fileBlocks_FileName) 

        # parse files and fill arrays
        for file in files :
            parList = []
            # skip already analyzed blocks
            fileblock = file['Block']['Name']
            if fileblock not in anFileBlocks :
                filename = file['LogicalFileName']
                # asked retry the list of parent for the given child 
                if useParent: parList = [x['LogicalFileName'] for x in file['ParentList']] 
                self.parent[filename] = parList 
                if filename.find('.dat') < 0 :
                    events    = file['NumberOfEvents']
                    # number of events per block
                    if fileblock in self.eventsPerBlock.keys() :
                        self.eventsPerBlock[fileblock] += events
                    else :
                        self.eventsPerBlock[fileblock] = events
                    # number of events per file
                    self.eventsPerFile[filename] = events
            
                    # number of events per block
                    if fileblock in self.blocksinfo.keys() :
                        self.blocksinfo[fileblock].append(filename)
                    else :
                        self.blocksinfo[fileblock] = [filename]
            
                    # total number of events
                    self.maxEvents += events
        if  self.skipBlocks and len(self.eventsPerBlock.keys()) == 0:
            msg = "No new fileblocks available for dataset: "+str(self.datasetPath)
            raise  CrabException(msg)    

        saveFblocks='' 
        for block in self.eventsPerBlock.keys() :
            saveFblocks += str(block)+'\n' 
            common.logger.debug(6,"DBSInfo: total nevts %i in block %s "%(self.eventsPerBlock[block],block))
        writeTXTfile(self, fileBlocks_FileName , saveFblocks) 
                      
        if len(self.eventsPerBlock) <= 0:
            raise NotExistingDatasetError(("\nNo data for %s in DBS\nPlease check"
                                            + " dataset path variables in crab.cfg")
                                            % self.datasetPath)


# #################################################
    def getMaxEvents(self):
        """
        max events 
        """
        return self.maxEvents

# #################################################
    def getEventsPerBlock(self):
        """
        list the event collections structure by fileblock 
        """
        return self.eventsPerBlock

# #################################################
    def getEventsPerFile(self):
        """
        list the event collections structure by file 
        """
        return self.eventsPerFile

# #################################################
    def getFiles(self):
        """
        return files grouped by fileblock 
        """
        return self.blocksinfo        

# #################################################
    def getParent(self):
        """
        return parent grouped by file 
        """
        return self.parent        

########################################################################
