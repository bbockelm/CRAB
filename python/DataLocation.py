#!/usr/bin/env python
import os, string, re
import common
from DLSInfo import *

# ####################################
class DataLocationError(exceptions.Exception):
    def __init__(self, errorMessage):
        self.args=errorMessage
        exceptions.Exception.__init__(self, self.args)
        pass
                                                                                      
    def getErrorMessage(self):
        """ Return exception error """
        return "%s" % (self.args)

# ####################################
# class to extact data location
class DataLocation:
    def __init__(self, Listfileblocks, cfg_params):

#       Attributes
        self.Listfileblocks = Listfileblocks  # DLS input: list of fileblocks lists

        self.cfg_params = cfg_params

        self.SelectedSites = {}        # DLS output: list of sites hosting fileblocks
                                       #  retrieved using method getSites

# #######################################################################
    def fetchDLSInfo(self):
        """
        Contact DLS
        """
        countblock=0

        dlstype='dbs'

        DLS_type="DLS_TYPE_%s"%dlstype.upper()

        ## find the replicas for each block
        blockSites = {}
        failCount = 0
        for fileblocks in self.Listfileblocks:
            countblock=countblock+1

            dls=DLSInfo(DLS_type,self.cfg_params)
            try:
                replicas=dls.getReplicas(fileblocks)
                common.logger.debug(5,"sites are %s"%replicas)
                if len(replicas)!=0:
                    blockSites[fileblocks] = replicas
                else:
                    # add empty entry if no replicas found
                    blockSites[fileblocks] = ''

            except DLSNoReplicas, ex:
                common.logger.debug(5,str(ex.getErrorMessage()))
                common.logger.debug(5,"Block not hosted by any site, continuing.\n")
                blockSites[fileblocks] = ''
                failCount = failCount + 1
            except:
                raise DataLocationError('')

        if countblock == failCount:
            msg = "All data blocks encountered a DLS error.  Quitting."
            raise DataLocationError(msg)

        self.SelectedSites = blockSites

# #######################################################################
    def getSites(self):
        """
        get the sites hosting all the needed data 
        """
        return self.SelectedSites
    
#######################################################################
    def uniquelist(self, old):
        """
        remove duplicates from a list
        """
        nd={}
        for e in old:
            nd[e]=0
        return nd.keys()
