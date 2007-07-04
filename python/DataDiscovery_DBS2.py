#!/usr/bin/env python
import exceptions
import DBSAPI.dbsApi
from DBSAPI.dbsApiException import * 
import common
from crab_util import *


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
            dbs_url="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"

        common.logger.debug(3,"Accessing DBS at: "+dbs_url)

        ## check if runs are selected
        try:
            runselection = parseRange2(self.cfg_params['CMSSW.runselection'])
        except:
            runselection = []

        ## service API
        args = {}
        args['url']     = dbs_url
        args['level']   = 'CRITICAL'

        api = DBSAPI.dbsApi.DbsApi(args)
        try:
            if len(runselection) <= 0 :
                files = api.listDatasetFiles(self.datasetPath)
            else :
                files = api.listFiles(path=self.datasetPath, details=True)
        except DbsBadRequest, msg:
            raise DataDiscoveryError_DBS2(msg)
        except DBSError_DBS2, msg:
            raise DataDiscoveryError_DBS2(msg)

        # parse files and fill arrays
        for file in files :
            filename = file['LogicalFileName']
            if filename.find('.dat') < 0 :
                fileblock = file['Block']['Name']
                events    = file['NumberOfEvents']
                continue_flag = 0
                if len(runselection) > 0 :
                    runslist = file['RunsList']
                    for run in runslist :
                        runnumber = run['RunNumber']
                        for selected_run in runselection :
                            if runnumber == selected_run :
                                continue_flag = 1
                else :
                    continue_flag = 1

                if continue_flag == 1 :
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
