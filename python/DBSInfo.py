#!/usr/bin/env python
import sys, os, string, re, commands
import exceptions
import common
from crab_exceptions import *
try:
    import dbsCgiApi
    import dbsApi
except:
    try:
        Crabpydir=commands.getoutput('which crab')
        Topdir=string.replace(Crabpydir,'/python/crab','')
        sys.path.append(Topdir+'/DBSAPI')
        import dbsCgiApi
        import dbsApi
    except:
        msg="ERROR no DBS API available"
        raise CrabException(msg)

# #######################################
class DBSError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass

    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# #######################################
class DBSInvalidDataTierError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, args)
        pass
                                                                                      
    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
class DBSInfoError:
    def __init__(self, url):
        print '\nERROR accessing DBS url : '+url+'\n'
        pass

##################################################################################
# Class to extract info from DBS 
###############################################################################

class DBSInfo:
    def __init__(self):
        # Construct api object
        self.api = dbsCgiApi.DbsCgiApi()
        # Configure api logging level
        self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_QUIET_)
        #if common.logger.debugLevel() >= 4:
        # self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_INFO_)
        #if common.logger.debugLevel() >= 10:          
        # self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_ALL_)

# ####################################
    def getMatchingDatasets (self, owner, dataset):
        """ Query DBS to get provenance """
        try:
            list = self.api.listDatasets("/%s/*/%s" % (dataset, owner))
        except dbsApi.InvalidDataTier, ex:
            raise DBSInvalidDataTierError(ex.getClassName(),ex.getErrorMessage())
        except dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        return list

# ####################################
    def getDatasetProvenance(self, path, dataTiers):
        """
        query DBS to get provenance
        """
        try:
            datasetParentList = self.api.getDatasetProvenance(path,dataTiers)
        except dbsApi.InvalidDataTier, ex:
            raise DBSInvalidDataTierError(ex.getClassName(),ex.getErrorMessage())  
        except dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        return datasetParentList                                                                                                            
        #parent = {}
        #for aparent in datasetParentList:
        #  common.logger.debug(6, "DBSInfo: parent path is "+aparent.getDatasetPath()+" datatier is "+aparent.getDataTier())
        #  parent[aparent.getDatasetPath()]=aparent.getDataTier()
        #
        #return parent

# ####################################
    def getDatasetContents(self, path):
        """
        query DBS to get event collections
        """
        try:
            fileBlockList = self.api.getDatasetContents(path)
        except dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        ## get the fileblock and event collections
        nevtsbyblock= {}
        for fileBlock in fileBlockList:
            ## get the event collections for each block
            eventCollectionList = fileBlock.getEventCollectionList()
            nevts=0
            for eventCollection in eventCollectionList:
                common.logger.debug(20,"DBSInfo:  evc: "+eventCollection.getCollectionName()+" nevts:%i"%eventCollection.getNumberOfEvents()) 
                nevts=nevts+eventCollection.getNumberOfEvents()
            common.logger.debug(6,"DBSInfo: total nevts %i in block %s "%(nevts,fileBlock.getBlockName()))
            nevtsbyblock[fileBlock.getBlockName()]=nevts

        # returning a map of fileblock-nevts  will be enough for now
        # TODO: in future the EvC collections grouped by fileblock should be returned
        
        return nevtsbyblock

