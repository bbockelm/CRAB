#!/usr/bin/env python
"""
_LFNBaseName_
"""

from crab_exceptions import *
from crab_util import runCommand
import common
import os, string, time
from ProdCommon.SiteDB.SiteDB import SiteDBJSON


def LFNBase(PrimaryDataset='',ProcessedDataset='',merged=True,LocalUser=False,publish=False):
    """
    """
    lfnbase = "/store"
    if not merged:
        lfnbase = os.path.join(lfnbase,"tmp")
    if (PrimaryDataset == 'null'):
        PrimaryDataset = ProcessedDataset
    lfnbase = os.path.join(lfnbase, "user", getUserName(LocalUser=LocalUser), PrimaryDataset, ProcessedDataset )

    return lfnbase

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

def gethnUserNameFromSiteDB():
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


def gethnUserName():
    """
    cache the username extracted from SiteDB for 24hours
    """
    userconfigFileName="SiteDBusername.conf"
    if not os.path.exists(userconfigFileName):
        common.logger.debug(5,"Downloading from SiteDB username into %s"%userconfigFileName)
        nameuser = gethnUserNameFromSiteDB()
        userfile = open(userconfigFileName, 'w')
        userfile.write(nameuser)
        userfile.close()
    else:
        statinfo = os.stat(userconfigFileName)
        ## if the file is older then 24 hours it is re-downloaded to update the configuration
        oldness = 24*3600
        if (time.time() - statinfo.st_ctime) > oldness:
           common.logger.debug(5,"Downloading from SiteDB username into %s"%userconfigFileName)
           nameuser = gethnUserNameFromSiteDB()
           userfile = open(userconfigFileName, 'w')
           userfile.write(nameuser)
           userfile.close()
        else:
           userfile = open(userconfigFileName, 'r')
           for line in userfile.readlines():
               nameuser = line
           userfile.close()
           nameuser = string.strip(nameuser)
    return nameuser  

def getUserName(LocalUser=False):
    """
    extract user name from either SiteDB or Unix
    """
    if LocalUser:
       common.logger.debug(10,"Using as username the Unix user name")
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
