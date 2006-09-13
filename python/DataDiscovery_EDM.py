#!/usr/bin/env python
import sys, os, string, re
from DBSInfo_EDM import *


# ####################################
class DataDiscoveryError(exceptions.Exception):
    def __init__(self, errorMessage):
        args=errorMessage
        exceptions.Exception.__init__(self, args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
class NotExistingDatasetError(exceptions.Exception):
    def __init__(self, errorMessage):
        args=errorMessage
        exceptions.Exception.__init__(self, args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
class NoDataTierinProvenanceError(exceptions.Exception):
    def __init__(self, errorMessage):
        args=errorMessage
        exceptions.Exception.__init__(self, args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
# class to find and extact info from published data
class DataDiscovery_EDM:
    def __init__(self, datasetPath, dataTiers, cfg_params):

#       Attributes
        self.datasetPath = datasetPath
        self.dataTiers = dataTiers
        self.cfg_params = cfg_params

        self.eventsPerBlock = {}  # DBS output: map fileblocks-events for collection
        self.eventsPerFile = {}   # DBS output: map files-events
        self.blocksinfo = {}  # DBS output: map fileblocks-files 
#DBS output: max events computed by method getMaxEvents 

# ####################################
    def fetchDBSInfo(self):
        """
        Contact DBS
        """

        ## get DBS URL
        try:
            dbs_url=self.cfg_params['CMSSW.dbs_url']
        except KeyError:
            # This URL is deprecated and no longer supported by the DBS team
            #dbs_url="http://cmsdoc.cern.ch/cms/aprom/DBS/CGIServer/prodquery"
            # This URL is supported by the DBS team and is 'stable'
            dbs_url="http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquery"
            # This URL is used for testing of the most recent changes by the DBS team
            #dbs_url = "http://cmsdoc.cern.ch/cms/test/aprom/DBS/CGIServer/prodquerytest"

        ## get info about the requested dataset
        try:
            dbs_instance=self.cfg_params['CMSSW.dbs_instance']
        except KeyError:
            dbs_instance="MCGlobal/Writer"
 
        dbs = DBSInfo_EDM(dbs_url, dbs_instance)
        self.datasets = dbs.getMatchingDatasets(self.datasetPath)
        if len(self.datasets) == 0:
            raise DataDiscoveryError("DatasetPath=%s unknown to DBS" %self.datasetPath)
        if len(self.datasets) > 1:
            raise DataDiscoveryError("DatasetPath=%s is ambiguous" %self.datasetPath)

        try:
            self.dbsdataset = self.datasets[0].get('datasetPathName')

            self.eventsPerBlock = dbs.getEventsPerBlock(self.dbsdataset)
            self.blocksinfo = dbs.getDatasetFileBlocks(self.dbsdataset)
            self.eventsPerFile = dbs.getEventsPerFile(self.dbsdataset)
        except DBSError, ex:
            raise DataDiscoveryError(ex.getErrorMessage())
        
        if len(self.eventsPerBlock) <= 0:
            raise NotExistingDatasetError (("\nNo data for %s in DBS\nPlease check"
                                            + " dataset path variables in crab.cfg")
                                            % self.dbsdataset)



# #################################################
    def getMaxEvents(self):
        """
        max events 
        """
        ## loop over the event collections 
        nevts=0       
        for evc_evts in self.eventsPerBlock.values():
            nevts=nevts+evc_evts

        return nevts

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


