#!/usr/bin/env python
import exceptions
import DBSAPI.dbsApi
from DBSAPI.dbsApiException import * 
import common


# #######################################
class DBSError_DBS2(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass
    
    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)

# #######################################
class DBSInvalidDataTierError_DBS2(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass
    
    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)

# #######################################
class DBSInfoError_DBS2:
    def __init__(self, url):
        print '\nERROR accessing DBS url : '+url+'\n'
        pass

# ####################################
class DataDiscoveryError_DBS2(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
class NotExistingDatasetError_DBS2(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
class NoDataTierinProvenanceError_DBS2(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
# class to find and extact info from published data
class DataDiscovery_DBS2:
    def __init__(self, datasetPath, cfg_params):

        #       Attributes
        self.datasetPath = datasetPath
        self.cfg_params = cfg_params

        self.eventsPerBlock = {}  # DBS output: map fileblocks-events for collection
        self.eventsPerFile = {}   # DBS output: map files-events
        self.blocksinfo = {}      # DBS output: map fileblocks-files 
        self.maxEvents = 0        # DBS output: max events

# ####################################
    def fetchDBSInfo(self):
        """
        Contact DBS
        """

        ## get DBS URL
        try:
            dbs_url=self.cfg_params['CMSSW.dbs_url']
        except KeyError:
            dbs_url="http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"

        ## get DBS URL
        try:
            dbs_version=self.cfg_params['CMSSW.dbs_version']
        except KeyError:
            dbs_version="v00_00_05"

        ## service API
        args = {}
        args['url']     = dbs_url
        args['version'] = dbs_version

        common.logger.debug(3,"Accessing DBS at: "+dbs_url+" with version: "+dbs_version)

        api = DBSAPI.dbsApi.DbsApi(args)
        try:
            files = api.listFiles(self.datasetPath)
        except DbsBadRequest, msg:
            raise DataDiscoveryError_DBS2(msg)
        except DBSError_DBS2, msg:
            raise DataDiscoveryError_DBS2(msg)
        
        # parse files and fill arrays
        for file in files :
            filename = file['LogicalFileName']
            events = file['NumberOfEvents']
            fileblock = file['Block']['Name']

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

        if len(self.eventsPerBlock) <= 0:
            raise NotExistingDatasetError_DBS2 (("\nNo data for %s in DBS\nPlease check"
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

########################################################################
