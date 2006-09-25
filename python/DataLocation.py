#!/usr/bin/env python
import sys, os, string, re
import urllib, urllister
import urllib2
import common
from DLSInfo import *

# ####################################
class DataLocationError(exceptions.Exception):
    def __init__(self, errorMessage):
        args=errorMessage
        exceptions.Exception.__init__(self, args)
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

        CEBlackList = []
        try:
            tmpBad = string.split(self.cfg_params['EDG.ce_black_list'],',')
            for tmp in tmpBad:
                tmp=string.strip(tmp)
                CEBlackList.append(tmp)
        except KeyError:
            pass
        common.logger.debug(5,'CEBlackList: '+str(CEBlackList))
        self.reCEBlackList=[]
        for bad in CEBlackList:
            self.reCEBlackList.append(re.compile( string.lower(bad) ))

        CEWhiteList = []
        try:
            tmpGood = string.split(self.cfg_params['EDG.ce_white_list'],',')
            for tmp in tmpGood:
                tmp=string.strip(tmp)
                CEWhiteList.append(tmp)
        except KeyError:
            pass
        common.logger.debug(5,'CEWhiteList: '+str(CEWhiteList))
        self.reCEWhiteList=[]
        for good in CEWhiteList:
            self.reCEWhiteList.append(re.compile( string.lower(good) ))

        self.osgSitesDictionary = {"Wisconsin":"cmssrm.hep.wisc.edu", \
                                   "Purdue":"dcache.rcac.purdue.edu", \
                                   "Florida":"ufdcache.phys.ufl.edu", \
                                   "Nebraska":"thpc-1.unl.edu", \
                                   "Caltech":"cithep59.ultralight.org", \
                                   "UCSD":"t2data2.t2.ucsd.edu", \
                                   "fnal":"cmssrm.fnal.gov", \
                                   "MIT":"se01.cmsaf.mit.edu"}

# #######################################################################
    def fetchDLSInfo(self):
        """
        Contact DLS
        """
        countblock=0
        Sites = []
        allblockSites = []

        try:
            dlstype=self.cfg_params['CMSSW.dls_type']
        except KeyError:
            dlstype='dli'
        #DLS_type="DLS_TYPE_MYSQL"
        DLS_type="DLS_TYPE_%s"%dlstype.upper()

        ## find the replicas for each block
        blockSites = {}
        failCount = 0
        for fileblocks in self.Listfileblocks:
            countblock=countblock+1
            #dbspath=string.split(afileblock,'#')[0]
            #(null,ds,tier,ow)=string.split(dbspath,'/')
            #datablock=ow+"/"+ds
            #
            dls=DLSInfo(DLS_type,self.cfg_params)
            try:
                replicas=dls.getReplicas(fileblocks)
                common.logger.debug(5,"sites are %s"%replicas)
                replicas = self.checkOSGt2(replicas)
                replicas = self.checkBlackList(replicas, fileblocks)
                if len(replicas)!=0:
                    replicas = self.checkWhiteList(replicas, fileblocks)
                if len(replicas)!=0:
                    blockSites[fileblocks] = replicas
            except DLSNoReplicas, ex:
                common.logger.debug(5,str(ex.getErrorMessage()))
                common.logger.debug(5,"Proceeding without this block.\n")
                failCount = failCount + 1
            except:
                raise DataLocationError('')

        if countblock == failCount:
            msg = "All data blocks encountered a DLS error.  Quitting."
            raise DataLocationError(msg)

        if len(blockSites)==0:
            msg = 'No sites remaining that host any part of the requested data! Exiting... '
            raise CrabException(msg)

        self.SelectedSites = blockSites

# #######################################################################
    def getSites(self):
        """
        get the sites hosting all the needed data 
        """
        return self.SelectedSites

# #######################################################################
    def checkBlackList(self, Sites, fileblocks):
        """
        select sites that are not excluded by the user (via CE black list)
        """
        goodSites = []
        for aSite in Sites:
            common.logger.debug(10,'Site '+aSite)
            good=1
            for re in self.reCEBlackList:
                if re.search(string.lower(aSite)):
                    common.logger.debug(5,'CE in black list, skipping site '+aSite)
                    good=0
                pass
            if good: goodSites.append(aSite)
        if len(goodSites) == 0:
            msg = "No sites hosting the block %s after BlackList" % fileblocks
            common.logger.debug(5,msg)
            common.logger.debug(5,"Proceeding without this block.\n")
        else:
            common.logger.debug(5,"Selected sites for block "+str(fileblocks)+" via BlackList are "+str(goodSites)+"\n")
        return goodSites

# #######################################################################
    def checkWhiteList(self, Sites, fileblocks):
        """
        select sites that are defined by the user (via CE white list)
        """
        if len(self.reCEWhiteList)==0: return Sites
        goodSites = []
        for aSite in Sites:
            good=0
            for re in self.reCEWhiteList:
                if re.search(string.lower(aSite)):
                    common.logger.debug(5,'CE in white list, adding site '+aSite)
                    good=1
                pass
            if good: goodSites.append(aSite)
        if len(goodSites) == 0:
            msg = "No sites hosting the block %s after WhiteList" % fileblocks
            common.logger.debug(5,msg)
            common.logger.debug(5,"Proceeding without this block.\n")
        else:
            common.logger.debug(5,"Selected sites for block "+str(fileblocks)+" via WhiteList are "+str(goodSites)+"\n")
        return goodSites 

# #######################################################################
    def checkOSGt2(self, sites):
        fixedSites = []
        osgKeys = self.osgSitesDictionary.keys()
        for site in sites:
            fix = 0
            for osgSite in osgKeys:
                if string.lower(site) == string.lower(osgSite):
                    fixedSites.append(self.osgSitesDictionary[osgSite])
                    fix = 1
            if (fix == 0):
                fixedSites.append(site)

        return fixedSites

#######################################################################
    def uniquelist(self, old):
        """
        remove duplicates from a list
        """
        nd={}
        for e in old:
            nd[e]=0
        return nd.keys()
