#!/usr/bin/env python
"""
_LFNBaseName_
"""

from crab_exceptions import *
from crab_util import runCommand
import common
import os, string


def LFNBase(ProcessedDataset,merged=True):
    """
    """
    lfnbase = "/store" 
    if not merged:
        lfnbase = os.path.join(lfnbase,"tmp")
    
    lfnbase = os.path.join(lfnbase, "user", gethnUserName(), ProcessedDataset )
    return lfnbase

def PFNportion(ProcessedDataset):
    pfnpath = os.path.join(gethnUserName(), ProcessedDataset )
    return pfnpath

def getDN():
    """
    extract DN from user proxy's identity
    """
    try:
        userdn = runCommand("voms-proxy-info -identity")
        userdn = string.strip(userdn)
    except:
        msg = "Error. Problem with voms-proxy-info -identity command"
        raise CrabException(msg)
    return userdn 

def gethnUserName():
    """
    extract user name from SiteDB
    """
    import urllib
    hnUserName = None
    userdn = getDN()
    try:
        sitedburl="https://cmsweb.cern.ch/sitedb/sitedb/json/index/dnUserName"
        params = urllib.urlencode({'dn': userdn })
        f = urllib.urlopen(sitedburl,params)
        udata = f.read()
        try:
            userinfo= eval(udata)
        except StandardError, ex:
            msg = "Error. Problem extracting user name from %s : %s"%(sitedburl,ex)
            raise CrabException(msg)
        hnUserName = userinfo['user']
    except:
        msg = "Error. Problem extracting user name from %s"%sitedburl
        msg += "Check that you are registered in SiteDB, see https://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB" 
        raise CrabException(msg)
    if not hnUserName:
        msg = "Error. There is no user name associated to DN %s in %s. You need to register in SiteDB with the instructions at https://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB"%(userdn,sitedburl)
        print msg
        raise CrabException(msg)
    return hnUserName

if __name__ == '__main__' :
    """
    """
    from crab_logger import Logger
    from WorkSpace import *
    continue_dir="/bohome/fanfani/CRAB"
    cfg_params={'USER.logdir' : continue_dir }
    common.work_space = WorkSpace(continue_dir, cfg_params)
    log = Logger()
    common.logger = log

    baselfn = LFNBase("datasetstring")
    print baselfn    

    unmergedlfn = LFNBase("datasetstring",merged=False)
    print unmergedlfn  
    print PFNportion("datasetstring")
