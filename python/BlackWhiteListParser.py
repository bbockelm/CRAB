from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import os, sys, time

class BlackWhiteListParser:
    def __init__(self,cfg_params):
        self.configure(cfg_params)
        return

    def configure(self, cfg_params):

        SEBlackList = []
        try:
            tmpBad = string.split(cfg_params['EDG.se_black_list'],',')
            for tmp in tmpBad:
                tmp=string.strip(tmp)
                SEBlackList.append(tmp)
        except KeyError:
            pass
        common.logger.debug(5,'SEBlackList: '+str(SEBlackList))
        self.reSEBlackList=[]
        for bad in SEBlackList:
            self.reSEBlackList.append(re.compile( string.lower(bad) ))

        SEWhiteList = []
        try:
            tmpGood = string.split(cfg_params['EDG.se_white_list'],',')
            for tmp in tmpGood:
                tmp=string.strip(tmp)
                SEWhiteList.append(tmp)
        except KeyError:
            pass
        common.logger.debug(5,'SEWhiteList: '+str(SEWhiteList))
        self.reSEWhiteList=[]
        for good in SEWhiteList:
            self.reSEWhiteList.append(re.compile( string.lower(good) ))

    def checkBlackList(self, Sites, fileblocks):
        """
        select sites that are not excluded by the user (via SE black list)
        """
        goodSites = []
        for aSite in Sites:
            common.logger.debug(10,'Site '+aSite)
            good=1
            for re in self.reSEBlackList:
                if re.search(string.lower(aSite)):
                    common.logger.debug(5,'SE in black list, skipping site '+aSite)
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

    def checkWhiteList(self, Sites, fileblocks):
        """
        select sites that are defined by the user (via SE white list)
        """
        if len(self.reSEWhiteList)==0: return Sites
        goodSites = []
        for aSite in Sites:
            good=0
            for re in self.reSEWhiteList:
                if re.search(string.lower(aSite)):
                    common.logger.debug(5,'SE in white list, adding site '+aSite)
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
    
    def cleanForBlackWhiteList(self,destinations):
        """
        clean for black/white lists using parser
        """

        return ','.join(self.checkWhiteList(self.checkBlackList(destinations,''),''))

