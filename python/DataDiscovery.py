#!/usr/bin/env python2
import sys, os, string, re
from DBSInfo import *

# ####################################
class DataDiscoveryError:
    def __init__(self):
        print '\nERROR accessing Data Discovery\n'
        pass
# ####################################
class DatasetContentsError:
    def __init__(self):
        print '\nERROR accessing Data Discovery : getDatasetContents\n'
        pass

# ####################################
class DatasetProvenanceError:
    def __init__(self):
        print '\nERROR accessing Data Discovery : getDatasetProvenance\n'
        pass

# ####################################
# class to find and extact info from published data
class DataDiscovery:
    def __init__(self, owner, dataset, dataTiers, cfg_params):

#       Attributes
        self.dbsdataset=dataset+'/datatier/'+owner
        self.dataTiers = dataTiers
        self.cfg_params = cfg_params

        self.dbspaths= []  # DBS output: list of dbspaths for all data
        self.allblocks = []   # DBS output: list of map fileblocks-totevts for all dataset-owners
        self.blocksinfo = {}     # DBS output: map fileblocks-totevts for the primary block, used internally to this class
#DBS output: max events computed by method getMaxEvents 

# ####################################
    def fetchDBSInfo(self):
        """
        Contact DBS
        """
        parents = []
        parentsblocksinfo = {}
        self.dbspaths.append("/"+self.dbsdataset) # add the primary dbspath
                                                  # it might be replaced if one get from DBSAPI the primary dbspath as well

        dbs=DBSInfo(self.dbsdataset,self.dataTiers)
        try:
          self.blocksinfo=dbs.getDatasetContents()
        except dbs.DBSError:
          raise DataDiscoveryError
        try:
          parents=dbs.getDatasetProvenance()
        except:
          raise DataDiscoveryError

        ## for each parent get the corresponding fileblocks
        for aparent in parents:
           ## fill a list of dbspaths
           parentdbsdataset=aparent.getDatasetPath()
           self.dbspaths.append(parentdbsdataset)
           #tmppath=str(parentdbsdataset[1:-1])
           pdataset=string.split(parentdbsdataset,'/')[1]
           pdt=string.split(parentdbsdataset,'/')[2]
           powner=string.split(parentdbsdataset,'/')[3]
           tmppath=pdataset+'/'+pdt+'/'+powner
           ## get the fileblocks of the parents : FIXME for the time being the first / in the path has to be removed
           pdbs=DBSInfo(tmppath,[])
           try:
             parentsblocksinfo=pdbs.getDatasetContents()
           except:
            raise DataDiscoveryError

           self.allblocks.append(parentsblocksinfo.keys()) # add parent fileblocksinfo

        ## all the required blocks
        self.allblocks.append(self.blocksinfo.keys()) # add also the primary fileblocksinfo


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


