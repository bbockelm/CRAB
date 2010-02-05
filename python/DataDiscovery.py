#!/usr/bin/env python

__revision__ = "$Id: DataDiscovery.py,v 1.39 2010/02/04 16:32:38 ewv Exp $"
__version__ = "$Revision: 1.39 $"

import exceptions
import DBSAPI.dbsApi
from DBSAPI.dbsApiException import *
import common
from crab_util import *
from LumiList import LumiList
import os



class DBSError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass

    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)



class DBSInvalidDataTierError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass

    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)



class DBSInfoError:
    def __init__(self, url):
        print '\nERROR accessing DBS url : '+url+'\n'
        pass



class DataDiscoveryError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)



class NotExistingDatasetError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)



class NoDataTierinProvenanceError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)



class DataDiscovery:
    """
    Class to find and extact info from published data
    """
    def __init__(self, datasetPath, cfg_params, skipAnBlocks):

        #       Attributes
        self.datasetPath = datasetPath
        # Analysis dataset is primary/processed/tier/definition
        self.ads = len(self.datasetPath.split("/")) > 4
        self.cfg_params = cfg_params
        self.skipBlocks = skipAnBlocks

        self.eventsPerBlock = {}  # DBS output: map fileblocks-events for collection
        self.eventsPerFile = {}   # DBS output: map files-events
#         self.lumisPerBlock = {}   # DBS output: number of lumis in each block
#         self.lumisPerFile = {}    # DBS output: number of lumis in each file
        self.blocksinfo = {}      # DBS output: map fileblocks-files
        self.maxEvents = 0        # DBS output: max events
        self.maxLumis = 0         # DBS output: total number of lumis
        self.parent = {}          # DBS output: parents of each file
        self.lumis = {}           # DBS output: lumis in each file
        self.lumiMask = None

    def fetchDBSInfo(self):
        """
        Contact DBS
        """
        ## get DBS URL
        global_url="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        caf_url = "http://cmsdbsprod.cern.ch/cms_dbs_caf_analysis_01/servlet/DBSServlet"
        dbs_url_map  =   {'glite':    global_url,
                          'glite_slc5':global_url,\
                          'glitecoll':global_url,\
                          'condor':   global_url,\
                          'condor_g': global_url,\
                          'glidein':  global_url,\
                          'lsf':      global_url,\
                          'caf':      caf_url,\
                          'sge':      global_url,\
                          'arc':      global_url,\
                          'pbs':      global_url
                          }

        dbs_url_default = dbs_url_map[(common.scheduler.name()).lower()]
        dbs_url=  self.cfg_params.get('CMSSW.dbs_url', dbs_url_default)
        common.logger.info("Accessing DBS at: "+dbs_url)

        ## check if runs are selected
        runselection = []
        if (self.cfg_params.has_key('CMSSW.runselection')):
            runselection = parseRange2(self.cfg_params['CMSSW.runselection'])

        ## check if lumiMask is set
        self.lumiMask = self.cfg_params.get('CMSSW.lumi_mask',None)
        lumiList = None
        if self.lumiMask:
            lumiList = LumiList(filename=self.lumiMask)

        self.splitByRun = int(self.cfg_params.get('CMSSW.split_by_run', 0))

        common.logger.log(10-1,"runselection is: %s"%runselection)
        ## service API
        args = {}
        args['url']     = dbs_url
        args['level']   = 'CRITICAL'

        ## check if has been requested to use the parent info
        useparent = int(self.cfg_params.get('CMSSW.use_parent',0))

        ## check if has been asked for a non default file to store/read analyzed fileBlocks
        defaultName = common.work_space.shareDir()+'AnalyzedBlocks.txt'
        fileBlocks_FileName = os.path.abspath(self.cfg_params.get('CMSSW.fileblocks_file',defaultName))

        api = DBSAPI.dbsApi.DbsApi(args)
        self.files = self.queryDbs(api,path=self.datasetPath,runselection=runselection,useParent=useparent)

        anFileBlocks = []
        if self.skipBlocks: anFileBlocks = readTXTfile(self, fileBlocks_FileName)

        # parse files and fill arrays
        for file in self.files :
            parList  = []
            fileLumis = [] # List of tuples
            # skip already analyzed blocks
            fileblock = file['Block']['Name']
            if fileblock not in anFileBlocks :
                filename = file['LogicalFileName']
                # asked retry the list of parent for the given child
                if useparent==1:
                    parList = [x['LogicalFileName'] for x in file['ParentList']]
                if self.ads or self.lumiMask:
                    fileLumis = [ (x['RunNumber'], x['LumiSectionNumber'])
                                 for x in file['LumiList'] ]
                self.parent[filename] = parList
                # For LumiMask, intersection of two lists.
                if self.lumiMask:
                    self.lumis[filename] = lumiList.filterLumis(fileLumis)
                else:
                    self.lumis[filename] = fileLumis
                if filename.find('.dat') < 0 :
                    events    = file['NumberOfEvents']
                    # Count number of events and lumis per block
                    if fileblock in self.eventsPerBlock.keys() :
                        self.eventsPerBlock[fileblock] += events
                    else :
                        self.eventsPerBlock[fileblock] = events
                    # Number of events per file
                    self.eventsPerFile[filename] = events

                    # List of files per block
                    if fileblock in self.blocksinfo.keys() :
                        self.blocksinfo[fileblock].append(filename)
                    else :
                        self.blocksinfo[fileblock] = [filename]

                    # total number of events
                    self.maxEvents += events
                    self.maxLumis  += len(self.lumis[filename])

        if  self.skipBlocks and len(self.eventsPerBlock.keys()) == 0:
            msg = "No new fileblocks available for dataset: "+str(self.datasetPath)
            raise  CrabException(msg)

        saveFblocks=''
        for block in self.eventsPerBlock.keys() :
            saveFblocks += str(block)+'\n'
            common.logger.log(10-1,"DBSInfo: total nevts %i in block %s "%(self.eventsPerBlock[block],block))
        writeTXTfile(self, fileBlocks_FileName , saveFblocks)

        if len(self.eventsPerBlock) <= 0:
            raise NotExistingDatasetError(("\nNo data for %s in DBS\nPlease check"
                                            + " dataset path variables in crab.cfg")
                                            % self.datasetPath)


    def queryDbs(self,api,path=None,runselection=None,useParent=None):

        allowedRetriveValue = ['retrive_block', 'retrive_run']
        if self.ads or self.lumiMask:
            allowedRetriveValue.append('retrive_lumi')
        if useParent == 1: allowedRetriveValue.append('retrive_parent')
        common.logger.debug("Set of input parameters used for DBS query: %s" % allowedRetriveValue)
        try:
            if len(runselection) <=0 :
                if useParent==1 or self.splitByRun==1 or self.ads or self.lumiMask:
                    if self.ads:
                        files = api.listFiles(analysisDataset=path, retriveList=allowedRetriveValue)
                    else :
                        files = api.listFiles(path=path, retriveList=allowedRetriveValue)
                else:
                    files = api.listDatasetFiles(self.datasetPath)
            else :
                files=[]
                for arun in runselection:
                    try:
                        if self.ads:
                            filesinrun = api.listFiles(analysisDataset=path,retriveList=allowedRetriveValue,runNumber=arun)
                        else:
                            filesinrun = api.listFiles(path=path,retriveList=allowedRetriveValue,runNumber=arun)
                        files.extend(filesinrun)
                    except:
                        msg="WARNING: problem extracting info from DBS for run %s "%arun
                        common.logger.info(msg)
                        pass

        except DbsBadRequest, msg:
            raise DataDiscoveryError(msg)
        except DBSError, msg:
            raise DataDiscoveryError(msg)

        return files


    def getMaxEvents(self):
        """
        max events
        """
        return self.maxEvents


    def getMaxLumis(self):
        """
        Return the number of lumis in the dataset
        """
        return self.maxLumis


    def getEventsPerBlock(self):
        """
        list the event collections structure by fileblock
        """
        return self.eventsPerBlock


    def getEventsPerFile(self):
        """
        list the event collections structure by file
        """
        return self.eventsPerFile


    def getFiles(self):
        """
        return files grouped by fileblock
        """
        return self.blocksinfo


    def getParent(self):
        """
        return parent grouped by file
        """
        return self.parent


    def getLumis(self):
        """
        return lumi sections grouped by file
        """
        return self.lumis


    def getListFiles(self):
        """
        return parent grouped by file
        """
        return self.files
