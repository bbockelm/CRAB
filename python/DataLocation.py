#!/usr/bin/env python2
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

        self.SelectedSites = []        # DLS output: list of sites hosting all the needed fileblocks
                                       #  retireved using method getSites

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
            self.reCEBlackList.append(re.compile( bad ))

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
            self.reCEWhiteList.append(re.compile( good ))


# #######################################################################
    def fetchDLSInfo(self):
        """
        Contact DLS
        """
        countblock=0
        Sites = []
        allblockSites = []

        DLS_type="DLS_TYPE_MYSQL"
        #DLS_type="DLS_TYPE_DLI"

        ## find the replicas for each block 
        for fileblocks in self.Listfileblocks:
            #print fileblocks
            for afileblock in fileblocks:
                countblock=countblock+1
                dbspath=string.split(afileblock,'#')[0]
                (null,ds,tier,ow)=string.split(dbspath,'/')
                datablock=ow+"/"+ds
                #
                dls=DLSInfo(DLS_type)
                try:
                    replicas=dls.getReplicas(datablock)
                except DLSNoReplicas, ex:
                    raise DataLocationError(ex.getErrorMessage())
                except:
                    raise DataLocationError('')

                for replica in replicas:
                    Sites.append(replica)

        ## select only sites that contains _all_ the fileblocks
        allblockSites=self.SelectSites(countblock,Sites)
        #for as in allblockSites:
        #   print " site is "+as
        ## select sites that are not in a BlackList , if any
        self.SelectedNotBlackSites=self.checkBlackList(allblockSites)
        ## select sites there are in the white list , if any
        self.SelectedSites=self.checkWhiteList(self.SelectedNotBlackSites)

# #######################################################################
    def getSites(self):
        """
        get the sites hosting all the needed data 
        """
        return self.SelectedSites

# #######################################################################
    def SelectSites(self, countblock, Sites ):
        """
        select only sites that contains _all_ the fileblocks
        """ 
        goodSites=[]
        for aSite in Sites :
            if ( Sites.count(aSite)==countblock ) :
                goodSites.append(aSite)

        return self.uniquelist(goodSites)

# #######################################################################
    def checkBlackList(self, Sites):
        """
        select sites that are not excluded by the user (via CE black list)
        """
        goodSites = []
        for aSite in Sites:
            good=1
            for re in self.reCEBlackList:
                if re.search(aSite):
                    common.logger.message('CE in black list, skipping site '+aSite)
                    good=0
                pass
            if good: goodSites.append(aSite)
        if len(goodSites) == 0:
            common.logger.debug(3,"No selected Sites")
        return goodSites

# #######################################################################
    def checkWhiteList(self, Sites):
        """
        select sites that are defined by the user (via CE white list)
        """
        if len(self.reCEWhiteList)==0: return Sites
        goodSites = []
        for aSite in Sites:
            good=0
            for re in self.reCEWhiteList:
                if re.search(aSite):
                    common.logger.message('CE in white list, adding site '+aSite)
                    good=1
                pass
            if good: goodSites.append(aSite)
        if len(goodSites) == 0:
            common.logger.debug(3,"No selected Sites")
        return goodSites 

#######################################################################
    def uniquelist(self, old):
        """
        remove duplicates from a list
        """
        nd={}
        for e in old:
            nd[e]=0
        return nd.keys()

