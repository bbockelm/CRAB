#!/usr/bin/env python
"""
_LFNBaseName_
"""

from crab_exceptions import *
from crab_util import runCommand
import common
import os, string
from ProdCommon.SiteDB.SiteDB import SiteDBJSON


def LFNBase(ProcessedDataset,merged=True,LocalUser=False):
    """
    """
    lfnbase = "/store"
    if not merged:
        lfnbase = os.path.join(lfnbase,"tmp")
    lfnbase = os.path.join(lfnbase, "user", getUserName(LocalUser=LocalUser), ProcessedDataset )

    return lfnbase

def PFNportion(ProcessedDataset,LocalUser=False):
    pfnpath = os.path.join(getUserName(LocalUser=LocalUser), ProcessedDataset )
    return pfnpath

def getUnixUserName():
    """
    extract username from whoami
    """
    try:
        UserName = runCommand("whoami")
        UserName = string.strip(UserName)
    except:
        msg = "Error. Problem with whoami command"
        raise CrabException(msg)
    return UserName

def getDN():
    """
    extract DN from user proxy's identity
    """
    try:
        userdn = runCommand("voms-proxy-info -identity")
        userdn = string.strip(userdn)
        #search for a / to avoid picking up warning messages
        userdn = userdn[userdn.find('/'):]
    except:
        msg = "Error. Problem with voms-proxy-info -identity command"
        raise CrabException(msg)
    return userdn.split('\n')[0]

def gethnUserName():
    """
    extract user name from SiteDB
    """
    hnUserName = None
    userdn = getDN()
    mySiteDB = SiteDBJSON()

    try:
        hnUserName = mySiteDB.dnUserName(dn=userdn)
    except:
        msg = "Error. Problem extracting user name from SiteDB"
        msg += "\n Check that you are registered in SiteDB, see https://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB"
        raise CrabException(msg)
    if not hnUserName:
        msg = "Error. There is no user name associated to DN %s in SiteDB. You need to register in SiteDB with the instructions at https://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB" % userdn
        print msg
        raise CrabException(msg)
    return hnUserName

def getUserName(LocalUser=False):
    """
    extract user name from either SiteDB or Unix
    """
    if LocalUser:
       common.logger.message("==> Using as username the Unix user name")
       UserName=getUnixUserName()
       return UserName

    UserName=gethnUserName()
    return UserName

if __name__ == '__main__' :
    """
    """
    from crab_logger import Logger
    from WorkSpace import *
    continue_dir = os.path.expanduser("~")
    cfg_params={'USER.logdir' : continue_dir }
    common.work_space = WorkSpace(continue_dir, cfg_params)
    log = Logger()
    common.logger = log

    print "xx %s xx"%getUserName()
    baselfn = LFNBase("datasetstring")
    print baselfn

    unmergedlfn = LFNBase("datasetstring",merged=False)
    print unmergedlfn
    print PFNportion("datasetstring")
