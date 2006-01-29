#!/usr/bin/env python2
import sys, os, string, re
from DBSInfo import *


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
class DataDiscovery:
    def __init__(self, owner, dataset, dataTiers, cfg_params):

#       Attributes
        self.dbsdataset='/'+dataset+'/datatier/'+owner
        self.dataTiers = dataTiers
        self.cfg_params = cfg_params

        self.dbspaths= []     # DBS output: list of dbspaths for all data
        self.allblocks = []   # DBS output: list of map fileblocks-totevts for all dataset-owners
        self.blocksinfo = {}  # DBS output: map fileblocks-totevts for the primary block, used internally to this class
#DBS output: max events computed by method getMaxEvents 

# ####################################
    def fetchDBSInfo(self):
        """
        Contact DBS
        """
        parents = []
        parentsblocksinfo = {}

        ## add the PU among the required data tiers if the Digi are requested
        if (self.dataTiers.count('Digi')>0) & (self.dataTiers.count('PU')<=0) :
          self.dataTiers.append('PU')

        ## get info about the requested dataset 
        dbs=DBSInfo(self.dbsdataset,self.dataTiers)
        try:
          self.blocksinfo=dbs.getDatasetContents()
        except DBSError, ex:
          raise DataDiscoveryError(ex.getErrorMessage())
        
        if len(self.blocksinfo)<=0:
         msg="\nERROR Data %s do not exist in DBS! \n Check the dataset/owner variables in crab.cfg !"%self.dbsdataset
         raise NotExistingDatasetError(msg)

        currentdatatier=string.split(self.blocksinfo.keys()[0],'/')[2]
        fakedatatier=string.split(self.dbsdataset,'/')[2]
        currentdbsdataset=string.replace(self.dbsdataset, fakedatatier, currentdatatier)  

        self.dbspaths.append(currentdbsdataset)    # add the requested dbspath

        ## get info about the parents
        try:
          parents=dbs.getDatasetProvenance()
        except DBSInvalidDataTierError, ex:
          msg=ex.getErrorMessage()+' \n Check the data_tier variable in crab.cfg !\n'
          raise DataDiscoveryError(msg)
        except DBSError, ex:
          raise DataDiscoveryError(ex.getErrorMessage())

        ## check that the user asks for parent Data Tier really existing in the DBS provenance
        self.checkParentDataTier(parents, self.dataTiers, currentdbsdataset) 

        ## for each parent get the corresponding fileblocks
        for aparent in parents:
           ## fill a list of dbspaths
           parentdbsdataset=aparent.getDatasetPath()
           self.dbspaths.append(parentdbsdataset)
           pdbs=DBSInfo(parentdbsdataset,[])
           try:
             parentsblocksinfo=pdbs.getDatasetContents()
           except DBSError, ex:
            raise DataDiscoveryError(ex.getErrorMessage())

           self.allblocks.append(parentsblocksinfo.keys()) # add parent fileblocksinfo

        ## all the required blocks
        self.allblocks.append(self.blocksinfo.keys()) # add also the current fileblocksinfo


# #################################################
    def checkParentDataTier(self, parents, user_datatiers, currentdbsdataset ):
        """
         check that the data tiers requested by the user really exists in the provenance of the given dataset
        """

        current_datatier=string.split(currentdbsdataset,'/')[2]

        parent_datatypes=[]
        for aparent in parents:
          parent_datatypes.append(aparent.getDataType())

        for datatier in user_datatiers:
          if parent_datatypes.count(datatier)<=0:
             # the current datatier is not supposed to be in the provenance
             if not (datatier == current_datatier):  
              msg="\nERROR Data %s not published in DBS with asked data tiers : the data tier not found is %s !\n  Check the data_tier variable in crab.cfg !"%(currentdbsdataset,datatier)
              raise  NoDataTierinProvenanceError(msg)


# #################################################
    def getMaxEvents(self):
        """
         max events of the primary dataset-owner
        """
        ## loop over the fileblocks of the primary dataset-owner
        nevts=0       
        for blockevts in self.blocksinfo.values():
          nevts=nevts+blockevts

        return nevts

# #################################################
    def getDBSPaths(self):
        """
         list the DBSpaths for all required data
        """
        return self.dbspaths

# #################################################
    def getEVC(self):
        """
         list the event collections structure by fileblock 
        """
        print "To be used by a more complex job splitting... TODO later... "
        print "it requires changes in what's returned by DBSInfo.getDatasetContents and then fetchDBSInfo"

# #################################################
    def getFileBlocks(self):
        """
         fileblocks for all required dataset-owners 
        """
        return self.allblocks        

########################################################################


