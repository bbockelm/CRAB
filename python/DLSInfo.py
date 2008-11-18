#!/usr/bin/env python
import sys, os, commands,string, re
import exceptions
from crab_exceptions import *
from crab_util import *
import common

import dlsApi
import dlsClient
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
                                                                                            
class DLSError:
    def __init__(self, fileblocks):
        print '\nERROR accessing DLS for fileblock '+fileblocks+'\n'
        pass


class DLSNoReplicas(exceptions.Exception):
    def __init__(self, FileBlock):
        self.args ="No replicas exists for fileblock: "+FileBlock+"\n"
        exceptions.Exception.__init__(self, self.args)
        pass

    def getClassName(self):
        """ Return class name. """
        return "%s" % (self.__class__.__name__)

    def getErrorMessage(self):
        """ Return exception error. """
        return "%s" % (self.args)


##############################################################################
# Class to extract info from DLS 
##############################################################################

class DLSInfo:
    def __init__(self, type, cfg_params):
        self.cfg_params = cfg_params
        self.showCAF = False
        if type=="DLS_TYPE_DBS":
            # use dbs_url as dls_endpoint if dls_type is dbs
            try:
                endpoint=self.cfg_params['CMSSW.dbs_url']
            except KeyError:
                endpoint="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        elif type=="DLS_TYPE_PHEDEX":
            try:
                endpoint=self.cfg_params['CMSSW.dls_phedex_url']
            except KeyError:
                endpoint='http://cmsweb.cern.ch/phedex/datasvc/xml/prod/' 
            if self.cfg_params['CRAB.scheduler'].upper() == 'CAF':  self.showCAF = True
        else:
            msg = "DLS type %s not among the supported DLS ( DLS_TYPE_DLI and DLS_TYPE_MYSQL ) "%type
            raise CrabException(msg)
        common.logger.debug(5,"DLS interface: %s Server %s"%(type,endpoint))       
        try:
            self.api = dlsClient.getDlsApi(dls_type=type,dls_endpoint=endpoint)
        except dlsApi.DlsApiError, inst:
            msg = "Error when binding the DLS interface: %s  Server %s"%(str(inst),endpoint)
            #print msg
            raise CrabException(msg)
 

    def getReplicasBulk(self,fileblocks):
        """
        query DLS to get replicas
        """
        ##
        try:
            entryList=self.api.getLocations(fileblocks,longList=True)
        except dlsApi.DlsApiError, inst:
            raise DLSNoReplicas(fileblocks)
        results = {} 
        for entry in entryList: 
            ListSites=[] 
            for loc in entry.locations:
                ListSites.append(str(loc.host))
            if len(ListSites)<=0:
                raise DLSNoReplicas(fileblocks)
            results[entry.fileBlock.name]=ListSites

        return results         
# ####################################
    def getReplicas(self,fileblocks):
        """
        query DLS to get replicas
        """
        ##
        try:
            entryList=self.api.getLocations([fileblocks],showCAF=self.showCAF)
        except dlsApi.DlsApiError, inst:
            raise DLSNoReplicas(fileblocks)

        ListSites=[] 
        for entry in entryList:
            for loc in entry.locations:
                ListSites.append(str(loc.host))
        if len(ListSites)<=0:
            raise DLSNoReplicas(fileblocks)

        return ListSites         
