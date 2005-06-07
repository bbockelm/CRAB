#!/usr/bin/env python
import sys, os, string, re
import urllib, urllister
import urllib2

# ####################################
# Exception with use of refDB
class RefDBError:
  def __init__(self, owner, dataset):
    print '\nERROR accessing RefDB for Owner/Dataset: '+owner+'/'+dataset+'\n'
    pass

# ####################################
class PubDBError:
  def __init__(self, url):
    print '\nERROR accessing PubDB at '+url+'\n'
    pass
  
# ####################################
class NoPHPError:
  def __init__(self, url):
    #print '\nERROR accessing PHP at '+url+' \n'
    print 'ERROR accessing PHP: ',url,'isn\'t updated version \n'
    pass
  
# ####################################
class pubDBResult:
  def __init__(self,
               contents):
    self.contents=contents

    
  def dump(self):
    print 'Contents : ',self.contents

# ####################################
# class to access PubDBs
class PubDB:
  def __init__(self, owner, dataset, dataTiers):
    self.owner = owner
    self.dataset = dataset
    self.dataTiers = dataTiers
    
    self.RefDBurl_ = 'http://cmsdoc.cern.ch/cms/production/www/'
    self.RefDBphp_ = 'PubDB/GetIdCollection.php'
    self.RefDBMotherphp_ = 'cgi/SQL/CollectionTree.php'

    self.PubDBCentralUrl_ = 'http://cmsdoc.cern.ch/cms/production/www/PubDB/'
    self.PubDBCentralPhp_ = 'GetPublishedCollectionInfoFromRefDB.php'

    self.PubDBAnalysisPhp_ = 'get-pubdb-analysisinfo.php'
    self.PubDBAnalysisPhpOld_ = 'get-pubdb-analysisinfo.php'

    try:
      self.collid=self.findAllCollId()
    except PubDBError:
      raise RefDBError(self.owner, self.dataset)

########################################################################
  def findAllCollId(self):
    collId=self.findCollId() 


    NeededCollID = []
    NeededCollID.append(collId)
    #dataTypeReq = ['Digi' ]#, 'Digi', 'Hit', 'PU']

    if len(self.dataTiers)>0:
      dataTypeReq = self.dataTiers
      CollInfos=self.findMotherCollId(collId)
      while (CollInfos[1][2]!='PU'):

        for TypeReq in dataTypeReq:
          for CollInfo in CollInfos[1:]:
            if TypeReq==CollInfo[2]:
              NeededCollID.append(CollInfo[0])
              break
          pass
        CollInfos=self.findMotherCollId(CollInfo[0])
        ### no more parents
        if len(CollInfos)==1:
          break

    
    print NeededCollID
    return NeededCollID
          
      

########################################################################
  def findCollId(self):
    """
    Contact RefDB and get CollId given Dataset and Owner
    """
   
    #anche questa info viene dal cfg. E' PubDB centrale 
    url = self.RefDBurl_+self.RefDBphp_+'?Owner=' + self.owner + '&Dataset=' + self.dataset

    try:
      f = urllib.urlopen(url)
    except IOError:
      # print 'Cannot access URL: '+url
      raise PubDBError(url)

    line = f.read()
    try:
      collid = string.split(line,": ")
      #part = string.strip(part[1])
      collid = string.split(collid[1],"<")
      collid = string.strip(collid[0])
    except IndexError:
      raise PubDBError(url)

    print 'CollectionId: '+collid+' \n'
    return collid

########################################################################
  def findMotherCollId(self, collid):
    """
    Contact RefDB and get CollId of mother of current Dataset Owner (eg. Digi if DST, Hit if Digi)
    """

    url = self.RefDBurl_+self.RefDBMotherphp_+'?cid=' + collid

    try:
      f = urllib.urlopen(url)
    except IOError:
      # print 'Cannot access URL: '+url
      raise PubDBError(url)

    reEmptyLine = re.compile( r'^$' )

    collInfos = []
    for line in f.readlines():
      #print '#',line,'#'
      line = string.strip(line)
      if reEmptyLine.match(line):
        pass
      else:
        #print '#',line,'#'
        keys = string.split(line,',')
        #print '#',keys,'#'
        collInfo = []
        for key in keys:
          collInfo.append(string.split(key, '=')[1])
        collInfos.append(collInfo)

    return collInfos

########################################################################
  def getPubDBInfo(self, url):
    """
    Contact a local PubDB to collect all the relevant information
    """

    result = []
    end=string.rfind(url,'/')
    lastEq=string.rfind(url,'=')
    urlphp=url[:end+1]+self.PubDBAnalysisPhp_+'?CollID='+url[lastEq+1:]
    # print 'PHP URL: '+urlphp+' \n'

    reOld=re.compile( r'V24' )
    #print urlphp,'Old PubDB ',reOld.search(urlphp)
    if reOld.search(urlphp):
      raise NoPHPError(urlphp)
      # try:
      #   urldev=string.replace(urlphp,'V24','V3_1')
      #   print "urldev URL: ",urldev
      #   f = urllib2.urlopen(urldev)
      # except urllib2.HTTPError, msg:
      #   raise NoPHPError(urldev)
    else:
      try:
        f = urllib2.urlopen(urlphp) 
      except urllib2.HTTPError, msg:
        raise NoPHPError(urlphp)
    
    content = f.read()
    return pubDBResult(content)
   
########################################################################
  def findPubDBsUrls(self):
    """
    Find the URL of the PubDB of all the sites which publish the collid
    """
    
    completeResult = []

### first collId is the primary one, (Dataset/Owner asker by user)
### The other CollIDs are the parents

### Get all the pubDb's URL containig the primary collection _and_ the requested parents
    primaryCollId=self.collid[0]

    primaryUrl = self.PubDBCentralUrl_+self.PubDBCentralPhp_+'?CollID=' + primaryCollId
    #print "primaryUrl=", primaryUrl

    try:
      sock = urllib.urlopen(primaryUrl)
    except IOError:
      raise PubDBError(primaryUrl)

    parser = urllister.URLLister()
    parser.feed(sock.read())
    sock.close()
    parser.close()

# this are all the href links found in the page
    for url in parser.linksList: 
# return only those which contains "CollID"
      result=[]
      if string.find(url, primaryCollId) != -1 :
        #print 'URL ',url
        result.append(url)
        
        try:
          for tmp in self.checkPubDBs(url):
            result.append(tmp)
        except PubDBError:
          continue

        completeResult.append(result)
          
    return completeResult

#########################################################
  def checkPubDBs(self, url):
    """
    Check if the given PubDB contains also the CollId's collections
    """ 
 
    result = []
    primaryCollId=self.collid[0]
    
    reNotCollId=re.compile( r'no such collection' )
    reDBError=re.compile( r'DB Error: connect failed' )
    for collid in self.collid[1:]:
      newurl = string.replace(url,primaryCollId,collid)
      try:
        sock = urllib.urlopen(newurl)
        for line in sock.readlines():
          line = string.strip(line)
          if reNotCollId.search(line):
            raise PubDBError(newurl)
          if reDBError.search(line):
            raise PubDBError(newurl)
      except IOError:
        raise PubDBError(newurl)
      sock.close()
      result.append(newurl)

    return result

#########################################################
  def getAllPubDBsInfo(self):
    """
    Prepare the file to send in InputSandbox, with the info retrieved by local PubDBs
    """ 

# get all the URLs of PubDBs which publish CollId
    pubDBUrlsSet = self.findPubDBsUrls()
    # print 'pubDBUrls ',pubDBUrlsSet

# get the contents of each PubDBs
    completeResult = []
    for pubDBUrls in pubDBUrlsSet:
      result = []
      for url in pubDBUrls:
        # print 'URL ',url
        try:
          result.append(self.getPubDBInfo(url))
        except NoPHPError:
          continue
      # for r in result:
      #   r.dump()
      if len(result)>0: completeResult.append(result)
    
    # print 'getAllPubDBsInfo ',completeResult
    # for result in completeResult:
    #   print '.....'
    #   for r in result:
    #     r.dump()
    #   print '.....'
    return completeResult
