#!/usr/bin/env python
import exceptions
import DBSAPI.dbsApi
from DBSAPI.dbsApiException import * 
import common
from crab_util import *


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
    def __init__(self, datasetPath, cfg_params):

        #       Attributes
        self.datasetPath = datasetPath
        self.cfg_params = cfg_params

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
        dbs_url="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        if (self.cfg_params.has_key('CMSSW.dbs_url')):
            dbs_url=self.cfg_params['CMSSW.dbs_url']

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
        if (self.cfg_params.has_key('CMSSW.runselection')):
            runselection = parseRange2(self.cfg_params['CMSSW.runselection'])

        useParent = self.cfg_params.get('CMSSW.use_parent',False)
    
        allowedRetriveValue = [
                        'retrive_child', 
                        'retrive_block',
                        'retrive_lumi',
                        'retrive_run'
                        ]
        if useParent:  allowedRetriveValue.append('retrive_parent') 
        common.logger.debug(5,"Set of input parameters used for DBS query : \n"+str(allowedRetriveValue)) 
        common.logger.write("Set of input parameters used for DBS query : \n"+str(allowedRetriveValue)) 
        api = DBSAPI.dbsApi.DbsApi(args)
        try:
            if len(runselection) <= 0 :
                files = api.listFiles(path=self.datasetPath,retriveList=allowedRetriveValue)
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

        # parse files and fill arrays
        for file in files :
            parList = []
            filename = file['LogicalFileName']
            # asked retry the list of parent for the given child 
            if useParent: parList = [x['LogicalFileName'] for x in file['ParentList']] 
            self.parent[filename] = parList 
            if filename.find('.dat') < 0 :
                fileblock = file['Block']['Name']
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

        for block in self.eventsPerBlock.keys() :
            common.logger.debug(6,"DBSInfo: total nevts %i in block %s "%(self.eventsPerBlock[block],block))

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
