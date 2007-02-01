#!/usr/bin/env python
import sys, os, string, commands
import exceptions
import common
from crab_exceptions import *
import DBS1API.dbsCgiApi
import DBS1API.dbsApi
                                                                                              
## for python 2.2 add the pyexpat.so to PYTHONPATH
pythonV=sys.version.split(' ')[0]
if pythonV.find('2.2') >= 0 :
    Crabpydir=commands.getoutput('which crab')
    Topdir=string.replace(Crabpydir,'/python/crab','')
    extradir=Topdir+'/DLSAPI/extra'
    if sys.path.count(extradir) <= 0:
        if os.path.exists(extradir):
            sys.path.insert(0, extradir)

# #######################################
class DBSError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        self.args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, self.args)
        pass
    
    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)

# #######################################
class DBSInvalidDataTierError(exceptions.Exception):
    def __init__(self, errorName, errorMessage):
        self.args='\nERROR DBS %s : %s \n'%(errorName,errorMessage)
        exceptions.Exception.__init__(self, self.args)
        pass
    
    def getErrorMessage(self):
        """ Return error message """
        return "%s" % (self.args)

# #######################################
class DBSInfoError:
    def __init__(self, url):
        print '\nERROR accessing DBS url : '+url+'\n'
        pass

##################################################################################
# Class to extract info from DBS 
###############################################################################

class DBSInfo:
    def __init__(self, dbs_url, dbs_instance):
        """
        Construct api object.
        """
        ## cgi service API
        args = {}
        args['instance']=dbs_instance

        common.logger.debug(3,"Accessing DBS at: "+dbs_url+" "+dbs_instance)

        self.api = DBS1API.dbsCgiApi.DbsCgiApi(dbs_url, args)
        ## set log level
        # self.api.setLogLevel(DBS1API.dbsApi.DBS_LOG_LEVEL_INFO_)
        #self.api.setLogLevel(DBS1API.dbsApi.DBS_LOG_LEVEL_QUIET_)

    def getMatchingDatasets (self, datasetPath):
        """ Query DBS to get provenance """
        try:
            result = self.api.listProcessedDatasets("%s" %datasetPath)
        except DBS1API.dbsApi.InvalidDataTier, ex:
            raise DBSInvalidDataTierError(ex.getClassName(),ex.getErrorMessage())
        except DBS1API.dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        except DBS1API.dbsCgiApi.DbsCgiToolError , ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        except DBS1API.dbsCgiApi.DbsCgiBadResponse , ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())

        return result


    def getDatasetProvenance(self, path, dataTiers):
        """ Query DBS to get provenance """
        try:
            datasetParentList = self.api.getDatasetProvenance(path,dataTiers)
        except DBS1API.dbsApi.InvalidDataTier, ex:
            raise DBSInvalidDataTierError(ex.getClassName(),ex.getErrorMessage())
        except DBS1API.dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        return datasetParentList                                                                                                            

    def getEventsPerBlock(self, path):
        """ Query DBS to get event collections """
        # count events per block
        nevtsbyblock = {}
        try:
            contents = self.api.getDatasetContents(path)
        except DBS1API.dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        except DBS1API.dbsCgiApi.DbsCgiBadResponse, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        for fileBlock in contents:
            ## get the event collections for each block
            nevts = 0
            eventCollectionList = fileBlock.get('eventCollectionList')
            for evc in eventCollectionList:
                nevts = nevts + evc.get('numberOfEvents')

            common.logger.debug(6,"DBSInfo: total nevts %i in block %s "%(nevts,fileBlock.get('blockName')))
            nevtsbyblock[fileBlock.get('blockName')]=nevts

        # returning a map of fileblock-nevts  will be enough for now
        # TODO: in future the EvC collections grouped by fileblock should be returned
        return nevtsbyblock

    def getEventsPerFile(self, path):
        """ Query DBS to get a dictionary of files:(events/file) """
        numEventsByFile = {}
        try:
            contents = self.api.getDatasetContents(path)
        except DBS1API.dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        for fileBlock in contents:
            numEvents = 0
            eventCollectionList = fileBlock.get('eventCollectionList')
            for evc in eventCollectionList:
                numEvents = evc.get('numberOfEvents')
                fileList = evc.get('fileList')
                # As of 2006-08-10, event collections contain only one file
                # => fileList contains only one dictionary
                if len(fileList)>1:
                    msg = "Event collection contains more than one file!  Exiting.\n"
                    msg = msg + "CRAB and DBS must be upgraded to handle event collections with multiple files.\n"
                    raise CrabException(msg)
                fileDict = fileList[0]
                fileName = fileDict.get('logicalFileName')
                numEventsByFile[fileName] = numEvents
        return numEventsByFile

    def getDatasetFileBlocks(self, path):
        """ Query DBS to get files/fileblocks """
        try:
            FilesbyBlock={}
            try:
                allBlocks = self.api.getDatasetFileBlocks(path)
            except DBS1API.dbsCgiApi.DbsCgiBadResponse, ex:
                raise DBSError(ex.getClassName(), ex.getErrorMessage())
            for fileBlock in allBlocks:
                blockname=fileBlock.get('blockName')
                filesinblock=[]
                for files in fileBlock.get('fileList'):
                    #print "  block %s has file %s"%(blockname,files.getLogicalFileName())
                    filesinblock.append(files.get('logicalFileName'))
                FilesbyBlock[blockname]=filesinblock
        except DBS1API.dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())

        return FilesbyBlock
