#!/usr/bin/env python2
import sys, os, string, re
import urllib, urllister
import urllib2
import common
from RefDBInfo import RefDBInfo
from PubDBInfo import *

# ####################################
class PubDBError:
    def __init__(self, url):
        print '\nERROR accessing PubDB at '+url+'\n'
        pass

# ####################################
class PubDBGetAnalysisError:
  def __init__(self, url,Collections):
    print '\nERROR extracting info for collections '+Collections+' from PubDB '+url+'.\n'
    pass
  
# ####################################
class RefDBmapError:
    def __init__(self, url):
        print '\nERROR accessing RefDB-PubDBs map at '+url+'\n'
        pass 

# ####################################
class NoPHPError:
    def __init__(self, url):
        #print '\nERROR accessing PHP at '+url+' \n'
        print 'ERROR accessing PHP: ',url,' \n'
        pass
  
# ####################################
class pubDBResult:
    def __init__(self,
                 contents):
        self.contents=contents

    
    def dump(self):
        print 'Contents : ',self.contents
        pass

# ####################################
# class to access PubDBs
class PubDB:
    def __init__(self, owner, dataset, dataTiers, cfg_params):

#       Attributes
        self.owner = owner
        self.dataset = dataset
        self.dataTiers = dataTiers
        self.NeededdataTiers=[]
        self.cfg_params = cfg_params
    
        self.RefDBurl_ = 'http://cmsdoc.cern.ch/cms/production/www/'
        self.RefDBphp_ = 'PubDB/GetIdCollection.php'
        self.RefDBMotherphp_ = 'cgi/SQL/CollectionTree.php'

        self.PubDBCentralUrl_ = 'http://cmsdoc.cern.ch/cms/production/www/PubDB/'
        self.PubDBCentralPhp_ = 'GetPublishedCollectionInfoFromRefDB.php'

        self.PubDBAnalysisPhp_ = 'get-pubdb-analysisinfo.php'
        self.PubDBAnalysisPhpOld_ = 'get-pubdb-analysisinfo.php'
    
##      link to the modified RefDB-PubDBs map script that allow the display option
        self.RefDBPubDBsmapPhp_ = 'GetPublishedCollectionInfoFromRefDB.mod.php?display=1'

#       Costructor procedures

        CEBlackList = []
        try:
            tmpBad = string.split(self.cfg_params['EDG.ce_black_list'],',')
            #tmpBad = ['fnal']
            for tmp in tmpBad:
                tmp=string.strip(tmp)
                if (tmp == 'cnaf'): tmp = 'webserver' ########## warning: temp. patch              
                CEBlackList.append(tmp)
        except KeyError:
            pass

        CEWhiteList = []
        try:
            tmpGood = string.split(self.cfg_params['EDG.ce_white_list'],',')
            for tmp in tmpGood:
                tmp=string.strip(tmp)
                CEWhiteList.append(tmp)
        except KeyError:
            pass

        #print 'CEWhiteList: ',CEWhiteList
        self.reCEWhiteList=[]
        for Good in CEWhiteList:
            self.reCEWhiteList.append(re.compile( Good ))
        #print 'ReGood: ',self.reCEWhiteList

        common.logger.debug(5,'CEBlackList: '+str(CEBlackList))
        common.logger.debug(5,'CEWhiteList: '+str(CEWhiteList))
        self.reCEBlackList=[]
        for bad in CEBlackList:
            self.reCEBlackList.append(re.compile( bad ))
        #print 'ReBad: ',self.reCEBlackList


########################################################################
    def findAllCollections(self):
        """
        Contact RefDB and find the CollID of all the user required collections 
        """
        ## download from RefDB all the info about the given dataset-owner  
        refdb=RefDBInfo(self.owner,self.dataset)
        #print refdb.GetRefDBInfo()
        try:
            collInfos=refdb.GetRefDBInfo()
        except :
            sys.exit(10)
        #print "collInfos=", collInfos 
        
        first=1
        NeededCollID=[]
        refdbdataTiers=[]
        for coll in collInfos:
            ## select the primary collection
            if first:
                NeededCollID.append(coll[0])
                self.NeededdataTiers.append(coll[2])
                refdbdataTiers.append(coll[2])
                common.logger.message("\n --> primary collection for owner "+self.owner+" is: ID="+coll[0]+" DataTier="+coll[2])
                first=0
            else:
                ## select only the parents collections corresponding to data-tiers requested by the user 
                if  self.dataTiers.count(coll[2]):
                    NeededCollID.append(coll[0])
                    self.NeededdataTiers.append(coll[2])
                    common.logger.message(" --> further collection required: ID="+coll[0]+" DataTier="+coll[2])
                refdbdataTiers.append(coll[2])
           
        ## check that the user asks for Data Tier really existing in RefDB, otherwise give a warning message
        for dt in self.dataTiers:
            if refdbdataTiers.count(dt)<=0:
                msg = "ERROR: Data Tier ( =>",dt,"<= ) not existing for dataset/owner "+self.dataset+"/"+self.owner+"! "
                msg = str(msg) + 'Owner Dataset not published with asked dataTiers! '+\
                       self.owner+' '+ self.dataset+' '+str(self.dataTiers)+'\n'
                msg = str(msg) + ' Check the data_tier variable in crab.cfg !\n'
                common.logger.message(msg) 
                return []
        
        #print 'Needed Collections are ', NeededCollID
        #return collInfos
        #print "NeededCollID= ", NeededCollID
        return NeededCollID
  
########################################################################
    def findPubDBsbyCollID(self,CollID):
        """
         Find the list of PubDB URLs having a given Collection 
        """
        ### contact the RefDB-PubDBs map to discovery where the given CollID is
        url = self.PubDBCentralUrl_+self.RefDBPubDBsmapPhp_+'&CollID=' + CollID
        # print "%s"%(url)
        try:
            f = urllib.urlopen(url)
        except IOError:
            # print 'Cannot access URL: '+url
            raise RefDBmapError(url)
        
        ### search for the PubDBURL string
        reURLLine=re.compile( r'PubDBURL=(\S*)' )
       
        PubDBURLs = []
        for line in f.readlines():
            #print '#',line,'#'
            if reURLLine.search(line) :
                URLLine=reURLLine.search(line).group()
                #print  string.split(URLLine,'=')[1]
                PubDBURLs.append(string.split(URLLine,'=')[1])
        
        ### return the list of PubDBURL where the collection is present
        #return PubDBURLs 
        return  self.uniquelist(PubDBURLs)
  
################################################################
    def findPubDBs(self,CollIDs):
        """
         Find the list of PubDB URLs having ALL the required collections
        """
        ### loop over all the required collections 
        #pubdbmap={}
        allurls=[]
        countColl=0
        for CollID in CollIDs :
            countColl=countColl+1
            ### map the CollectionID with the list of PubDB URLs
            #pubdbmap[CollID]=self.findPubDBsbyCollID(CollID)
            ### prepare a list all PubDB urls for all collections  
            allurls.extend(self.findPubDBsbyCollID(CollID))
        #print pubdbmap.values()
       
        ### select only PubDB urls that contains all the collections
        unique_urls=self.uniquelist(allurls)
        SelectedPubDBURLs=[]
        # loop on a unique list of PubDB urls
        for url in unique_urls :
            # check that PubDBurl occurrance is the same as the number of collections 
            if ( allurls.count(url)==countColl ) :
                SelectedPubDBURLs.append(url)
        common.logger.debug(5,'PubDBs '+str(SelectedPubDBURLs))
       
        #print 'Required Collections',CollIDs,'are all present in PubDBURLs : ',SelectedPubDBURLs,'\n'
        ####  check based on CE black list: select only PubDB not in the CE black list   
        tmp=self.checkBlackList(SelectedPubDBURLs)
        common.logger.debug(5,'PubDBs after black list '+str(tmp))

        ### check based on CE white list: select only PubDB defined by user
        GoodPubDBURLs=self.checkWhiteList(tmp)
        if len(GoodPubDBURLs)>0 :
         common.logger.debug(5,'PubDBs after white list '+str(GoodPubDBURLs))
         common.logger.debug(3,'Selected sites via PubDB URLs are '+str(GoodPubDBURLs))
        return GoodPubDBURLs

#######################################################################
    def uniquelist(self, old):
        """
        remove duplicates from a list
        """
        nd={}
        for e in old:
            nd[e]=0
        return nd.keys()
 
#######################################################################
    def checkWhiteList(self, pubDBUrls):
        """
        select PubDB URLs that are at site defined by the user (via CE white list)
        """
        if len(self.reCEWhiteList)==0: return pubDBUrls
        goodurls = []
        for url in pubDBUrls:
            #print 'connecting to the URL ',url
            good=0
            for re in self.reCEWhiteList:
                if re.search(url):
                    common.logger.debug(5,'CE in white list, adding PubDB URL '+url)
                    good=1
                if not good: continue
                goodurls.append(url)
        if len(goodurls) == 0:
            common.logger.message("No sites found via PubDB \n")
        else:
            common.logger.debug(5,"Selected sites via PubDB URLs are "+str(goodurls)+"\n")
        return goodurls

#######################################################################
    def checkBlackList(self, pubDBUrls):
        """
        select PubDB URLs that are at site not exluded by the user (via CE black list) 
        """
        if len(self.reCEBlackList)==0: return pubDBUrls
        goodurls = []
        for url in pubDBUrls:
            common.logger.debug(10,'connecting to the URL '+url)
            good=1
            for re in self.reCEBlackList:
                if re.search(url):
                    common.logger.message('CE in black list, skipping PubDB URL '+url)
                    good=0
                pass
            if good: goodurls.append(url)
        if len(goodurls) == 0:
            common.logger.debug(3,"No sites found via PubDB")
        return goodurls

########################################################################
    def checkPubDBNewVersion(self, baseurl):
        """
        Check PubDB version to find out if it's new-style or old-style
        """
### check based on the existance of pubdb-get-version.php
        urlversion=baseurl+'pubdb-get-version.php'
        newversion=1;
        try:
         v = urllib2.urlopen(urlversion)
        except urllib2.URLError, msg:
          #print "WARNING: no URL to get PubDB version "
          newversion=0;
      
        if (newversion) :
         schemaversion = v.read()
         #print schemaversion;
   
        return newversion 

########################################################################
    def getPubDBData(self, CollIDs, url , newversion):
        """
         Contact a PubDB to collect all the relevant information
        """
        result = []
        
### get the base PubDb url 
        end=string.rfind(url,'/')
        lastEq=string.rfind(url,'=')

        if (newversion) :
### from PubDB V4 : get info for all the collections in one shot and unserialize the content
           Collections=string.join(CollIDs,'-')
           ## add the PU among the required Collections if the Digi are requested
           # ( for the time being asking it directly to the PubDB so the RefDB
           # level data discovery is bypassed..... in future when every site
           # will have the new style it will be possible to ask for PU , at RefDB level, in method findAllCollections ) 
           if ( self.NeededdataTiers.count('Digi') ):
             PUCollID=self.getDatatierCollID(url[:end+1],Collections,"PU")
             if (PUCollID) :
               if CollIDs.count(PUCollID)<=0:
                CollIDs.append(PUCollID)
           ##
           Collections=string.join(CollIDs,'-')
           ### download from PubDB all the info about the given collections
           pubdb_analysis=PubDBInfo(url[:end+1],Collections)
           #print pubdb_analysis.GetPubDBInfo()
           ok=0
           try:
             catInfos=pubdb_analysis.GetPubDBInfo()
             ok=1
           except :
             #print "WARNING: can't get PubDB content out of "+url[:end+1]+"\n"
             print '\nERROR extracting info for collections '+Collections+' from PubDB '+url[:end+1]+'.'
             print '>>>> Ask for help reporting that the failing PubDB script is: \n>>>> '+url[:end+1]+'pubdb-get-analysisinfo.php?collid='+Collections
             #raise PubDBGetAnalysisError(url[:end+1],Collections)   
           if (ok): result=catInfos;

        else:

### before PubDB V4 : get info for each collection and read the key-value pair text
              
          for CollID in CollIDs:
            urlphp=url[:end+1]+self.PubDBAnalysisPhp_+'?CollID='+CollID
            # print 'PHP URL: '+urlphp+' \n'

            reOld=re.compile( r'V24' )
            #print urlphp,'Old PubDB ',reOld.search(urlphp)
            if reOld.search(urlphp):
                raise NoPHPError(urlphp)
            else:
                try:
                    f = urllib2.urlopen(urlphp) 
                except urllib2.URLError, msg:
                    print "WARNING: ", msg 
                    raise PubDBError(urlphp)
                except urllib2.HTTPError, msg:
                    print "WARNING: ", msg
                    raise NoPHPError(urlphp)
                content = f.read()
                result.append(pubDBResult(content))
                #print "Coll",CollID," content ",content
                pass
            pass
        
        #print '.....'
        #for r in result:
        #     r.dump()
        #print '.....'
        return result

########################################################################
    def getDatatierCollID(self,urlbase,CollIDString,datatier):
        """
        Contact a script of PubDB to retrieve the collid a DataTier
        """
        try:
          f = urllib.urlopen(urlbase+'pubdb-get-collidbydatatier.php?collid='+CollIDString+"&datatier="+datatier)
        except IOError:
          raise PubDBGetAnalysisError(url[:end+1]+'pubdb-get-collidbydatatier.php',CollIDString)
        data = f.read()
        colldata=re.compile(r'collid=(\S*)').search(data);
        if colldata:
           datatier_CollID=colldata.group(1)
#           print " --> asking to PubDB "+urlbase+" for an additional collection : ID= "+datatier_CollID+" DataTier= "+datatier
           common.logger.message(" --> asking to PubDB "+urlbase+" for an additional collection : ID= "+datatier_CollID+" DataTier= "+datatier)

           return datatier_CollID       
 
########################################################################
    def getAllPubDBData(self):
        """
         Contact a list of PubDB to collect all the relevant information
        """
        newPubDBResult=[]
        oldPubDBResult=[]
        Result={}

### find the user-required collection IDs 
        CollIDs = self.findAllCollections()
### find the PubDB URLs publishing the needed data 
        urllist = self.findPubDBs(CollIDs)
### collect information sparatelly from new-style PubDBs and old-style PubDBs
        for pubdburl in urllist: 
            end=string.rfind(pubdburl,'/')
            newversion=self.checkPubDBNewVersion(pubdburl[:end+1])
            if (newversion):
              res=self.getPubDBData(CollIDs,pubdburl,newversion)
              if len(res)>0:
               newPubDBResult.append(res)
            else:
              resold=self.getPubDBData(CollIDs,pubdburl,newversion)
              if len(resold)>0:
               oldPubDBResult.append(resold)
### fill a dictionary with all the PubBDs results both old-style and new-style
        Result['newPubDB']=newPubDBResult
        Result['oldPubDB']=oldPubDBResult

        ## print for debugging purpose
        #
        #for PubDBversion in Result.keys():
            #print ("key %s, val %s" %(PubDBversion,Result[PubDBversion]))
        #    if len(Result[PubDBversion])>0 :
               #print (" key %s"%(PubDBversion)) 
        #       for result in Result[PubDBversion]:
        #          for r in result:
                      #r.dump()
        #              common.log.write('----------------- \n')
              #print '.....................................'

        return Result

####################################################################
