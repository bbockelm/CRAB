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

        self.evcinfo = {}  # DBS output: map fileblocks-events for collection
        self.blocksinfo = {}  # DBS output: map fileblocks-files 
#DBS output: max events computed by method getMaxEvents 

# ####################################
    def fetchDBSInfo(self):
        """
        Contact DBS
        """

        ## get info about the requested dataset
        try:
            dbs_instance=self.cfg_params['CMSSW.dbs_instance']
        except KeyError:
            dbs_instance="MCLocal/Writer"
 
        dbs = DBSInfo_EDM(dbs_instance)
	self.datasets = dbs.getMatchingDatasets(self.datasetPath)
	if len(self.datasets) == 0:
	  raise DataDiscoveryError("DatasetPath=%s unknown to DBS" %self.datasetPath)
	if len(self.datasets) > 1:
	  raise DataDiscoveryError("DatasetPath=%s is ambiguous" %self.datasetPath)

        try:
          self.dbsdataset = self.datasets[0].get('datasetPathName')

          self.evcinfo = dbs.getDatasetContents(self.dbsdataset)
          self.blocksinfo = dbs.getDatasetFileBlocks(self.dbsdataset)
        except DBSError, ex:
          raise DataDiscoveryError(ex.getErrorMessage())
        
        if len(self.evcinfo) <= 0:
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
        for evc_evts in self.evcinfo.values():
          nevts=nevts+evc_evts

        return nevts

# #################################################
    def getEVC(self):
        """
         list the event collections structure by fileblock 
        """
        print "To be used by a more complex job splitting... TODO later... "
        print "it requires changes in what's returned by DBSInfo.getDatasetContents and then fetchDBSInfo"

# #################################################
    def getFiles(self):
        """
         return files grouped by fileblock 
        """
        return self.blocksinfo        

########################################################################


