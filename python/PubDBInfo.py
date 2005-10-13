#!/usr/bin/env python
import sys, os, string, re
import urllib, urllister
import urllib2
from UnserializePHP import *
from orcarcBuilder import *

class PubDBInfoError:
  def __init__(self, Collections):
    print '\nERROR accessing PubDB for Collections: '+Collections+'\n'
    pass
class PubDBInfoNoCollectionError:
  def __init__(self, Collections):
    print '\nERROR No Collections found in PubDB : '+Collections+'\n'
    pass
class NoPHPError:
  def __init__(self, url):
    #print '\nERROR accessing PHP at '+url+' \n'
    print 'ERROR accessing PHP: ',url,'isn\'t updated version \n'
    pass
class PubDBInfoResult:
  def __init__(self,
               contents):
    self.contents=contents

################################################################################
# Class to connect to PubDB interface for the analysis and download the data in one shot using the serialized PHP data.
################################################################################
class PubDBInfo:
  def __init__(self, pubdburl, Collections):
         self.Collections= Collections
         self.PrimaryCollID=string.split(Collections,'-')[0]
         self.PubDBurl_ = pubdburl
         self.PubDBInfophp_ = 'pubdb-get-analysisinfo.php'
         self.protocolPrio_ = ['http', 'rfio', 'mysql' , 'gridftp']

  ##########################################################################   
  def GetPubDBInfo(self):
    """
    Get all the catalogues-related info from PubDB and select the best ones if multiple choices are possible 
    """
    ### extract catalogues related info from pubDB
    cataloguecoll_map = self.ExtractPubDBInfo()
    ### select the Best catalogues
    cataloguesinfos=self.SelectBestPubDBInfo(cataloguecoll_map)
    return cataloguesinfos

  #########################################################################
  def ExtractPubDBInfo(self):
    """
    Extract all the information from the PubDB analysis interface
    """
    try:
             #print " contacting PubDb... "+self.PubDBurl_+self.PubDBInfophp_+'?collid='+self.Collections+"\n"
        f = urllib.urlopen(self.PubDBurl_+self.PubDBInfophp_+'?collid='+self.Collections)
    except IOError:
        raise PubDBInfoError(self.Collections)

    data = f.read()
    #print data
    if len(data)>0:
       if data[0]=='<':
              raise PubDBInfoNoCollectionError(self.Collections)
    try:
        catalogues = PHPUnserialize().unserialize(data)
    except IOError:
        raise PHPUnserializeError(data)
    try:
        catinfos=[]
        collmap={}
        for k in catalogues.keys():
           CollId=catalogues[k]['CollectionId']
           ## get also the collection type
           CollType=catalogues[k]['CollectionType']
           ## set primary collection flag
           PrimaryCollFlag=0
           if ( CollId == self.PrimaryCollID ) : PrimaryCollFlag=1
           colllist=[]
           #print ">>> Catalogues for Collection: "+CollId+"\n"
          
           cat=catalogues[k]['Catalogue']
           for kcat in cat.keys():
                ##print ("key %s, val %s" %(kcat,cat[kcat]))
                ContactString=cat[kcat]['ContactString']
                ContactProtocol=cat[kcat]['ContactProtocol']
                CatalogueType=cat[kcat]['CatalogueType']
                ValidationStatus=cat[kcat]['ValidationStatus']
                #print "CS: "+ContactString
                #print "CP: "+ContactProtocol
                #print "CT: "+CatalogueType
                #print "VS: "+ValidationStatus
                ce=cat[kcat]['CEs']
                CElist=[]
                for kce in ce.keys():
                   ##print ("key %s, val %s" %(kce,ce[kce]))
                   CE=ce[kce]
                   CElist.append(ce[kce])
                #print " CE list :"
                #for aCE in CElist:
                #  print " CE : "+aCE
                cc=cat[kcat]['CatalogueContents']
                for kcc in cc.keys():
                   ##print ("key %s, val %s" %(kcc,cc[kcc]))
                   FileType=cc[kcc]['FileType']
                   SE=cc[kcc]['SE']
                   #print "FT: "+FileType
                   #print "SE: "+SE 
                   if cc[kcc]['Variables']==None:
                     Variables=''
                   else:
                    for kvar in cc[kcc]['Variables'].keys():
                     Variables=kvar+"="+cc[kcc]['Variables'][kvar]
                     #print "Variables: "+Variables
                   run=cc[kcc]['RunRange']
                   for krun in run.keys():
                     ##print ("key %s, val %s" %(krun,run[krun]))
                     reTot = re.compile(r'TotalEvents=(\d*)')
                     TotalEvents=reTot.search(run[krun]).group(1)
                     reFirst= re.compile(r'FirstRun=(\d*)')
                     FirstRun = reFirst.search(run[krun]).group(1)
                     reLast= re.compile(r'LastRun=(\d*)')
                     LastRun = reLast.search(run[krun]).group(1)
                     #print "Nevents: "+TotalEvents
                     #print "First: "+FirstRun
                     #print "Last: "+LastRun
                     #print "----------------------------------"

                     ## fill a catlogue entry
#                     acatalogue=catalogEntryNew(FileType,ValidationStatus,ContactString,ContactProtocol,CatalogueType,SE,CElist,TotalEvents,FirstRun+'-'+LastRun,Variables)          
                     ## store collection type and primarycollection flag
                     acatalogue=catalogEntryNew(CollType,PrimaryCollFlag,FileType,ValidationStatus,ContactString,ContactProtocol,CatalogueType,SE,CElist,TotalEvents,FirstRun+'-'+LastRun,Variables) 

                     ## list the catalogues belonging to a given collection
                     colllist.append(acatalogue) 
                     
           ## dictionary grouping catalogues by CollectionID
           collmap[CollId]=colllist
           
    except IOError:
       raise PHPUnserializeError(data)
    
    return collmap

  ########################################################################
  def SelectBestPubDBInfo(self,cataloglist):
    """
    Select the lists of needed catalogues (from a set of catalogues refering to the same collection and FileType selects the best on based on protocol)
    """

    selectcatalogues=[]
 
    ### for each collection   
    for collid in cataloglist.keys():
      #print ("key %s, val %s" %(collid,cataloglist[collid]))
      ### get all the possible FileTypes 
      filetypes=[]
      for catalog in cataloglist[collid]:
        if catalog.FileType not in filetypes :
          filetypes.append(catalog.FileType) 
      ### dictionary grouping catalogues by FileType 
      ftmap={}
      for afiletype in filetypes:
        #print ' filetype is '+afiletype+' for collid='+collid 
        sameFileType=[]
        for catalog in cataloglist[collid]:
          if catalog.FileType==afiletype :
            sameFileType.append(catalog)
        ftmap[afiletype]=sameFileType        
      ### select only one catalogue among the catalouges with the same FileType
      for ft in ftmap.keys():
       #print ("key %s, val %s" %(ft,ftmap[ft])) 
       bestcatalog=self.SelectBestCatalog(ftmap[ft])
       selectcatalogues.append(bestcatalog)

    ### return the minimal list of needed catalogues
    return selectcatalogues 

  ####################################################
  def SelectBestCatalog(self,ftcat):
    """
    From a set of catalogues with the same FileTypeand different access protocol, select the one according to access protocols
    """
    sortedProtocols = self.protocolPrio_

    ### if just one catalog, just return it!
    if (len(ftcat)==1):
      #print '----- Just one catalogue, no selection based on protocol needed'
      #ftcat[0].dump()
      #print '---------------------'
       return ftcat[0]
    ### oterwise select the best catalogue based on protocol   
    for prot in sortedProtocols:
      for cat in ftcat:
        if cat.ContactProtocol==prot:
          #print '----- Catalogue selected based on protocol : '+prot
          #cat.dump()
          #print '---------------------'
          return cat
        
