#!/usr/bin/env python
import sys, os, string, re
import urllib, urllister
import urllib2
from UnserializePHP import *

class RefDBError:
  def __init__(self, owner, dataset):
    print '\nERROR accessing RefDB for Owner/Dataset: '+owner+'/'+dataset+'\n'
    pass
class RefDBNoCollectionError:
  def __init__(self, owner, dataset):
    print '\nERROR No Collection Owner/Dataset found in RefDB : '+owner+'/'+dataset+'\n'
    pass
class NoPHPError:
  def __init__(self, url):
    #print '\nERROR accessing PHP at '+url+' \n'
    print 'ERROR accessing PHP: ',url,'isn\'t updated version \n'
    pass
class RefDBResult:
  def __init__(self,
               contents):
    self.contents=contents

##################################################################################
# Class to connect to RefDB and download the data in one shot using the serialized PHP data.
# Python doesn't allow to un-serialize PHP serialized objects. An "ad hoc" un-serialization is thus used.  
###############################################################################

class RefDBInfo:
     def __init__(self, owner, dataset):
          self.owner = owner
          self.dataset = dataset
          self.RefDBurl_ = 'http://cmsdoc.cern.ch/cms/production/www/'
          self.RefDBMotherphp_ = 'cgi/SQL/CollectionTree.php'
     

     def GetRefDBInfo(self):
         try:
             f = urllib.urlopen(self.RefDBurl_+self.RefDBMotherphp_+'?format=serialized&owner='+self.owner+'&dataset='+self.dataset)
         except IOError:
             raise RefDBError(self.owner,self.dataset)

         data = f.read()
         if len(data)>0:
             if data[0]=='<':
                 if (data.find("down") > -1) :
                    print "\n WARNING: RefDB is temporarily down for a short maintenace \n" 
                    raise RefDBError(self.owner,self.dataset)
                 else: 
                    raise RefDBNoCollectionError(self.owner,self.dataset)
         try:
             collections = PHPUnserialize().unserialize(data)
         except IOError:
             raise PHPUnserializeError(data)
         collinfos=[]
         try:
             for k in collections.keys():
                 #if collections[k]['type']!='PU':
                     collinfos.append([collections[k]['id'],collections[k]['name'],collections[k]['type'],collections[k]['oname'],collections[k]['dname']])
         except IOError:
             raise PHPUnserializeError(data)
         return collinfos
         





          
