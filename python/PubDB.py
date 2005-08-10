#!/usr/bin/env python2
import sys, os, string, re
import urllib, urllister
import urllib2
import common
from RefDBInfo import RefDBInfo

# ####################################
class PubDBError:
    def __init__(self, url):
        print '\nERROR accessing PubDB at '+url+'\n'
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
            tmpBad = string.split(self.cfg_params['USER.ce_black_list'],',')
            #tmpBad = ['fnal']
            for tmp in tmpBad:
                tmp=string.strip(tmp)
                CEBlackList.append(tmp)
        except KeyError:
            pass
        print 'CEBlackList: ',CEBlackList
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
                refdbdataTiers.append(coll[2])
                common.logger.message("\n --> primary collection for owner "+self.owner+" is: ID="+coll[0]+" DataTier="+coll[2])
                first=0
            else:
                ## select only the parents collections corresponding to data-tiers requested by the user 
                if  self.dataTiers.count(coll[2]):
                    NeededCollID.append(coll[0])
                    common.logger.message(" --> further collection required: ID="+coll[0]+" DataTier="+coll[2])
                refdbdataTiers.append(coll[2])
           
        ## check that the user asks for Data Tier really existing in RefDB, otherwise give a warning message
        for dt in self.dataTiers:
            if refdbdataTiers.count(dt)<=0:
                msg = "ERROR: Data Tier ( =>",dt,"<= ) not existing for dataset/owner "+ self.dataset+"/"+self.owner+"!"
                msg = msg + "Check the data_tier variable in crab.cfg"
                msg = msg + 'Owner Dataset not published with asked dataTiers! '+\
                       self.owner+' '+ self.dataset+' '+self.dataTiers
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
       
        #print 'Required Collections',CollIDs,'are all present in PubDBURLs : ',SelectedPubDBURLs,'\n'
        #return SelectedPubDBURLs
  ####  check based on CE black list: select only PubDB not in the CE black list   
        GoodPubDBURLs=self.checkBlackList(SelectedPubDBURLs)
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
    def checkBlackList(self, pubDBUrls):
        """
        select PubDB URLs that are at site not excluded by the user (via CE black list) 
        """
        goodurls = []
        for url in pubDBUrls:
            print 'connecting to the URL ',url
            good=1
            for re in self.reCEBlackList:
                if re.search(url):
                    common.logger.message('CE in black list, skipping PubDB URL '+url)
                    good=0
                pass
            if good: goodurls.append(url)
        if len(goodurls) == 0:
            common.logger.debug(3,"No selected PubDB URLs")
        return goodurls
  
########################################################################
    def getPubDBData(self, CollIDs, url):
        """
         Contact a PubDB to collect all the relevant information
        """
        result = []
        for CollID in CollIDs:
            end=string.rfind(url,'/')
            lastEq=string.rfind(url,'=')
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
    def getAllPubDBData(self, CollIDs, urllist):
        """
         Contact a list of PubDB to collect all the relevant information
        """
        completeResult=[]
        for pubdburl in urllist: 
            completeResult.append(self.getPubDBData(CollIDs,pubdburl))
        
        ## print for debugging purpose
        #for result in completeResult:
        #   print '..... PubDB Site URL :',pubdburl
        #   for r in result:
        #      r.dump()
        #   print '.....................................'
         
        return completeResult
####################################################################
