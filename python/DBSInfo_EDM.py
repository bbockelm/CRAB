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

##################################################################################
# Class to extract info from DBS 
###############################################################################

class DBSInfo_EDM:
    def __init__(self, dbs_url, dbs_instance):
        """
        Construct api object.
        """
        ## cgi service API
        args = {}
        args['instance']=dbs_instance

        common.logger.debug(3,"Accessing DBS at: "+dbs_url+" "+dbs_instance)

        self.api = dbsCgiApi.DbsCgiApi(dbs_url, args)
        ## set log level
        # self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_INFO_)
        #self.api.setLogLevel(dbsApi.DBS_LOG_LEVEL_QUIET_)

    def getMatchingDatasets (self, datasetPath):
        """ Query DBS to get provenance """
        try:
            list = self.api.listProcessedDatasets("%s" %datasetPath)
        except dbsApi.InvalidDataTier, ex:
            raise DBSInvalidDataTierError(ex.getClassName(),ex.getErrorMessage())
        except dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        except dbsCgiApi.DbsCgiToolError , ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        except dbsCgiApi.DbsCgiBadResponse , ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())

        return list


    def getDatasetProvenance(self, path, dataTiers):
        """ Query DBS to get provenance """
        try:
            datasetParentList = self.api.getDatasetProvenance(path,dataTiers)
        except dbsApi.InvalidDataTier, ex:
            raise DBSInvalidDataTierError(ex.getClassName(),ex.getErrorMessage())
        except dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())
        return datasetParentList                                                                                                            

    def getDatasetContents(self, path):
        """ Query DBS to get event collections """
        # count events per block
        nevtsbyblock = {}
        #print "FileBlock :",str(self.api.getDatasetContents (path))
        try:
            for fileBlock in self.api.getDatasetContents (path):
                ## get the event collections for each block
                nevts = 0
                for evc in fileBlock.get('eventCollectionList'):
                    nevts = nevts + evc.get('numberOfEvents')
                    common.logger.debug(6,"DBSInfo: total nevts %i in block %s "%(nevts,fileBlock.get('blockName')))
                    #print "BlockName ",fileBlock.get('blockName')
                    ## SL temp hack to get rid of a mismatch between block names as returned by DBS
                    tmp = string.split(fileBlock.get('blockName'),"/")
                    if (len(tmp)==4): del tmp[2]
                    blockName=string.join(tmp,"/")
                    #print "TMP ",blockName
                    # end hack
                    
                    nevtsbyblock[blockName]=nevts
                pass
        except dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())

        # returning a map of fileblock-nevts  will be enough for now
        # TODO: in future the EvC collections grouped by fileblock should be returned
        return nevtsbyblock


    def getDatasetFileBlocks(self, path):
        """ Query DBS to get files/fileblocks """
        try:
            FilesbyBlock={}
            for fileBlock in self.api.getDatasetFileBlocks(path):
                blockname=fileBlock.get('blockName')
                filesinblock=[]
                for files in fileBlock.get('fileList'):
                    #print "  block %s has file %s"%(blockname,files.getLogicalFileName())
                    filesinblock.append(files.get('logicalFileName'))
                FilesbyBlock[blockname]=filesinblock
        except dbsApi.DbsApiException, ex:
            raise DBSError(ex.getClassName(),ex.getErrorMessage())

        return FilesbyBlock
