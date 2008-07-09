#!/usr/bin/env python
"""
_BlackWhiteListParser_

Parsing for black and white lists, both SE and CE

Large parts of the July 2008 re-write come from Brian Bockelman

"""

__revision__ = "$Id: SiteDB.py,v 1.5 2008/07/08 22:19:00 ewv Exp $"
__version__ = "$Revision: 1.5 $"


import os
import sys
import sets
import time
import types
import fnmatch

from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
from ProdCommon.SiteDB.SiteDB import SiteDBJSON

class BlackWhiteListParser(object):

    """
    A class which applies blacklist and whitelist; designed to allow the user
    to filter out sites.  Allows users to specify only the CMS name from SiteDB
    (and simple wildcards), but internally filters only on the CE/SE name.
    """

    def __init__(self, cfg_params):
        self.kind = 'se'
        self.siteDBAPI = SiteDBJSON()

    def configure(self, cfg_params):
        """
        Load up the black and white list from the configuation parameters
           * EDG.%s_black_list
           * EDG.%s_white_list
        and expand things that SiteDB knows the CMS names for
        """ % (self.kind, self.kind)

        self.blacklist = []
        if cfg_params.has_key('EDG.%s_black_list' % self.kind):
            user_input = cfg_params['EDG.%s_black_list' % self.kind]
            self.blacklist = self.expandList(user_input)
        common.logger.debug(5,'Converted %s blacklist: %s' % (self.kind, ', '.join(self.blacklist)))

        self.whitelist = []
        if cfg_params.has_key('EDG.%s_white_list' % self.kind):
            user_input = cfg_params['EDG.%s_white_list' % self.kind]
            self.whitelist = self.expandList(user_input)
        common.logger.debug(5, 'Converted %s whitelist: %s' % (self.kind, ', '.join(self.whitelist)))

        self.blacklist = sets.Set(self.blacklist)
        self.whitelist = sets.Set(self.whitelist)
        #print "User's %s blacklist: %s" % (self.kind,self.blacklist)
        #print "User's %s whitelist: %s" % (self.kind,self.whitelist)

    def expandList(self, userInput):
      userList = userInput.split(',')
      expandedList = []
      for item in userList:
        item = item.strip()
        expandedItem = self.mapper(item)
        if expandedItem:
          expandedList.extend(expandedItem)
        else:
          expandedList.append(item)

      return expandedList

    def checkBlackList(self, Sites, fileblocks=''):
        """
        Select sites that are not excluded by the user (via blacklist)

        The sites returned are the input sites minus the contents of the
        self.blacklist

        @param Sites: The sites which will be filtered
        @keyword fileblocks: The block this is used for; only used in a pretty
           debug message.
        @returns: The input sites minus the blacklist.
        """
        Sites = sets.Set(Sites)
        #print "Sites:",Sites
        blacklist = self.blacklist
        blacklist = sets.Set(self.match_list(Sites, self.blacklist))
        #print "Black list:",blacklist
        goodSites = Sites.difference(blacklist)
        #print "Good Sites:",goodSites,"\n"
        goodSites = list(goodSites)
        if not goodSites and fileblocks:
            msg = "No sites hosting the block %s after blackList" % fileblocks
            common.logger.debug(5,msg)
            common.logger.debug(5,"Proceeding without this block.\n")
        elif fileblocks:
            common.logger.debug(5,"Selected sites for block %s via blacklist " \
                "are %s.\n" % (', '.join(fileblocks), ', '.join(goodSites)))
        return goodSites

    def checkWhiteList(self, Sites, fileblocks=''):
        """
        Select sites that are defined by the user (via white list).

        The sites returned are the intersection of the input sites and the
        contents of self.whitelist

        @param Sites: The sites which will be filtered
        @keyword fileblocks: The block this is applied for; only used for a
           pretty debug message
        @returns: The intersection of the input Sites and self.whitelist.
        """
        if not self.whitelist:
            return Sites
        whitelist = self.whitelist
        whitelist = self.match_list(Sites, self.whitelist)
        #print "White list:",whitelist
        Sites = sets.Set(Sites)
        goodSites = Sites.intersection(whitelist)
        #print "Good Sites:",goodSites,"\n"
        goodSites = list(goodSites)
        if not goodSites and fileblocks:
            msg = "No sites hosting the block %s after whiteList" % fileblocks
            common.logger.debug(5,msg)
            common.logger.debug(5,"Proceeding without this block.\n")
        elif fileblocks:
            common.logger.debug(5,"Selected sites for block %s via whitelist "\
                " are %s.\n" % (', '.join(fileblocks), ', '.join(goodSites)))

        return goodSites

    def cleanForBlackWhiteList(self,destinations,list=False):
        """
        Clean for black/white lists using parser.

        Take the input list and apply the blacklist, then the whitelist that
        the user specified.

        @param destinations: A list of all the input sites
        @keyword list: Set to True or the string 'list' to return a list
           object.  Set to False or the string '' to return a string object.
           The default is False.
        @returns: The list of all input sites, first filtered by the blacklist,
           then filtered by the whitelist.  If list=True, returns a list; if
           list=False, return a string.
        """
        if list:
            return self.checkWhiteList(self.checkBlackList(destinations))
        else:
            return ','.join(self.checkWhiteList(self.checkBlackList( \
                destinations)))


    def match_list(self, names, match_list):
        """
        Filter a list of names against a comma-separated list of expressions.

        This uses the `match` function to do the heavy lifting

        @param names: A list of input names to filter
        @type names: list
        @param match_list: A comma-separated list of expressions
        @type match_list: str
        @returns: A list, filtered from `names`, of all entries which match an
          expression in match_list
        @rtype: list
        """
        results = []
        if isinstance(match_list, types.StringType):
            match_list = match_list.split(',')

        for expr in match_list:
            expr = expr.strip()
            matching = self.match(names, expr)
            if matching:
                results.extend(matching)
            else:
                results.append(expr)
        return results


    def match(self, names, expr):
        """
        Return all the entries in `names` which match `expr`

        First, try to apply wildcard-based filters, then look at substrings,
        then interpret expr as a regex.

        @param names: An input list of strings to match
        @param expr: A string expression to use for matching
        @returns: All entries in the list `names` which match `expr`
        """

        results = fnmatch.filter(names, expr)
        results.extend([i for i in names if i.find(expr) >= 0])
        try:
            my_re = re.compile(expr)
        except:
            my_re = None
        if not my_re:
            return results
        results.extend([i for i in names if my_re.search(i)])
        return results



class SEBlackWhiteListParser(BlackWhiteListParser):
    """
    Use the BlackWhiteListParser to filter out the possible list of SEs
    from the user's input; see the documentation for BlackWhiteListParser.
    """

    def __init__(self, cfg_params):
        super(SEBlackWhiteListParser, self).__init__(cfg_params)
        self.kind = 'se'
        self.mapper = self.siteDBAPI.CMSNametoSE
        self.configure(cfg_params)



class CEBlackWhiteListParser(BlackWhiteListParser):
    """
    Use the BlackWhiteListParser to filter out the possible list of CEs
    from the user's input; see the documentation for BlackWhiteListParser.
    """

    def __init__(self,cfg_params):
        super(CEBlackWhiteListParser, self).__init__(cfg_params)
        self.kind = 'ce'
        self.mapper = self.siteDBAPI.CMSNametoCE
        self.configure(cfg_params)
