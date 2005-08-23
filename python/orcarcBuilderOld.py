import sys, os, re, string
import PubDB
import common

###########################################################################
## ORCARC builder to be used with PubDB v3: will be obsoleted asap
###########################################################################
class orcarc:
  def __init__(self,
               CE,
               SE,
               initFile,
               orcarcFile,
               Nevents):
    """ constructor from full infos """
    self.CE= CE
    self.SE= SE
    self.initFile= initFile
    self.orcarcFile=orcarcFile
    self.Nevents=Nevents
    return

  def __str__(self):
    txt = ''
    txt += "SE "+self.SE+'\n'
    txt += "CE "+self.CE+'\n'
    txt += "initFile "+self.initFile+'\n'
    txt += "orcarcFile "+self.orcarcFile+'\n'
    txt += "Nevents "+`self.Nevents`+'\n'
    return txt

  def dump(self):
    print self

  def content(self):
    """ return contents as a list """
    result = []
    result.append(self.CE)
    result.append(self.SE)
    result.append(self.initFile)
    result.append(self.orcarcFile)
    result.append(self.Nevents)
    return result

  def fileList(self):
    """ return list of files created """
    result = []
    result.append(self.initFile)
    result.append(self.orcarcFile)
    return result

def constructFromFile(content):
  """ constructor from list """
  return orcarc(content[0], 
                content[1],
                content[2],
                content[3],
                content[4])

###########################################################################
class catalogEntry:
  def __init__(self,
               FileType,
               ValidationStatus,
               ContactString,
               ContactProtocol,
               CatalogueType,
               SE,
               CE,
               Nevents,
               RunRange):
    self.FileType=FileType
    self.ValidationStatus=ValidationStatus
    self.ContactString=ContactString
    self.ContactProtocol=ContactProtocol
    self.CatalogueType=CatalogueType
    self.SE=SE
    self.CE=CE
    self.Nevents=Nevents
    self.RunRange=RunRange
  
  def __init__(self, list):
    reGen = re.compile(r'\w*=(.*)')

    self.FileType=string.split(list[0], '=')[1]
    self.ValidationStatus=reGen.match(list[1]).group(1)
    self.ContactString=reGen.match(list[2]).group(1)
    self.ContactProtocol=string.lower(reGen.match(list[3]).group(1))
    self.CatalogueType=string.lower(reGen.match(list[4]).group(1))
    self.SE=reGen.match(list[5]).group(1)
    #print "SE=", self.SE
    self.CE=reGen.match(list[6]).group(1)
    #print "CE=", self.CE
    self.Nevents=reGen.match(list[7]).group(1)
    self.RunRange=reGen.match(list[8]).group(1)
   
    # self.FileType=reGen.match(list[0]).group(1)
    # self.ValidationStatus=reGen.match(list[1]).group(1)
    # self.ContactString=reGen.match(list[2]).group(1)
    # self.ContactProtocol=string.lower(reGen.match(list[3]).group(1))
    # self.CatalogueType=string.lower(reGen.match(list[4]).group(1))
    # self.SE=reGen.match(list[5]).group(1)
    # self.CE=reGen.match(list[6]).group(1)
    # self.Nevents=reGen.match(list[7]).group(1)
    # self.RunRange=reGen.match(list[8]).group(1)
  
  def __str__(self):
    txt = ''
    txt += "FileType "+self.FileType+'\n'
    txt += "ValidationStatus "+self.ValidationStatus+'\n'
    txt += "ContactString "+self.ContactString+'\n'
    txt += "ContactProtocol "+self.ContactProtocol+'\n'
    txt += "CatalogueType "+self.CatalogueType+'\n'
    txt += "SE "+self.SE+'\n'
    txt += "CE "+self.CE+'\n'
    txt += "Nevents "+self.Nevents+'\n'
    txt += "RunRange "+self.RunRange+'\n'
    return txt

  def dump(self):
    print self
    return

###########################################################################
###########################################################################
class orcarcBuilderOld:
  def __init__(self):
    self.protocolPrio_ = self.defineProtocolPrio_()
    self.fileList = []
    self.CE = []
    self.SE = []
    pass
    
###########################################################################
  def parseResult_(self, pubDBResult):
    """
    parse the output from PubDB and create a list of CatalogEntry objects for further usage
    """

    result = []
    filename = pubDBResult.contents
    f = file( 'tmp', 'w' )
    f.write(pubDBResult.contents)
    f.close()

    reComment = re.compile( r'^#.*$' )
    reEmptyLine = re.compile( r'^$' )
    reLineStart = re.compile( r'FileType=(\w*)$' )
    reValid= re.compile( r'ValidationStatus=(\w*)$' )
    reLineEnd = re.compile( r'RunRange=.*$' )
    f = file( 'tmp', 'r' )
    Catalog_list = [] 
    out = []
    valid=0
    for line in f:
      line = line.strip()
      # print line
      if reComment.match( line ):
        pass
      elif reLineStart.match(line):
        out.append(line)
      elif reValid.match(line):
        valString = reValid.match(line).group(1)
        # print " valid ", line, " " , pippo
        # print (pippo == "VALIDATED") 
        if valString == "VALIDATED": 
          # print " VALID ", line, " " , pippo
          valid=1
        #### TMP SL 08-Feb-2005 ok even if not valid!
        elif (valString == "NOT_VALIDATED"): 
          common.logger.write('Dataset '+valString+'\n')
          valid=1
 




        out.append(line)
      elif reLineEnd.match( line ):
        out.append(line)
        if valid: Catalog_list.append(out)
        valid=0
        # print out 
        # print "\n"
        # print Catalog_list
        # print "\n"
        out = []
      elif reEmptyLine.match( line ):
        pass
      else:
        out.append(line)
    os.remove('tmp')

    for cc in Catalog_list:
      e = catalogEntry(cc)
      result.append(e)
      pass

    return result

  ###########################################################################
  def defineProtocolPrio_(self):
    """
    Just define the priority for the catalog acces protocol
    """
    return ['http', 'rfio', 'gridftp']

  ###########################################################################
  def selectCompleteSetOfCatalogs(self, CatalogList, Meta=1):
    """
    Select from the many catalogs returned from the PubDB a complete and unique
    set of them, which contains all needed, according to the pre-defined priority
    """
    result = []

    metaCat = []
    reComplete = re.compile( r'Complete' )
    if Meta==1:
    # priority 1: first look for Complete
      for cat in CatalogList:
         if cat.FileType=='Complete':
           metaCat.append(cat)
           # cat.dump()
           # print "-==-==-==-==-==-"
         elif reComplete.match(cat.FileType):
         ## SL 09-Feb-2005 Hack: pattern match rather than strick check (case
         ## Complete+Complete)...
           metaCat.append(cat)
           # cat.dump()
           # print "-==-==-==-==-==-"
           
    # if one or more "Complete" found, return them
      if (len(metaCat)>0):
        result.append(self.selectBestCatalog_(metaCat))
        return result

    # priority 2: assume attacchedMeta + Events
      metaCat = []
      evdCat = []
      for cat in CatalogList:
        if cat.FileType=='AttachedMETA':
          metaCat.append(cat)
          # cat.dump()
          # print "-==-==-==-==-==-"
        elif cat.FileType=='Events':
          evdCat.append(cat)
          # cat.dump()
          # print "-==-==-==-==-==-"

      if (len(metaCat) & len(evdCat)):
        result.append(self.selectBestCatalog_(metaCat))
        result.append(self.selectBestCatalog_(evdCat))
        return result

    elif Meta==0:
      evdCat = []
      for cat in CatalogList:
        if cat.FileType=='Events':
          evdCat.append(cat)
          # cat.dump()
          # print "-==-==-==-==-==-"
      if (len(evdCat)):
        result.append(self.selectBestCatalog_(evdCat))
        return result

      for cat in CatalogList:
         if cat.FileType=='Complete':
           evdCat.append(cat)
           # cat.dump()
           # print "-==-==-==-==-==-"
         elif reComplete.match(cat.FileType):
           metaCat.append(cat)
           # cat.dump()
           # print "-==-==-==-==-==-"

      if (len(evdCat)):
        result.append(self.selectBestCatalog_(evdCat))
        return result

    return []

  ###########################################################################
  def selectBestCatalog_(self, CatalogList):
    """
    From a set of catalogs with different access protocol,
    select the 'best' one according to access protocols
    """

    # if just one catalog, just return it!
    if (len(CatalogList)==1): return CatalogList[0]

    sortedProtocols = self.protocolPrio_
    # priority 1: first look for Complete
    for cat in CatalogList:
      for prot in sortedProtocols:
        if cat.ContactProtocol==prot:
          # cat.dump()
          # print "-==-==-==-==-==-"
          return cat
        pass
      pass

    return ''
         
  ###########################################################################
  def createStageInScript(self, CatalogList):
    """
    Check if some catalog need to be copied locally before usage and create a
    script to be issued by the user before excution of orca application.
    If no copy is needed, the init script will do nothing
    """

    # sanity check
    if len(CatalogList) == 0: 
       print 'Error ***: empty catalog list'
       return

    # print CatalogList
    site = CatalogList[0].CE
    initScriptFileName = 'init_'+site+'.sh'
    initScript = open(initScriptFileName,'w')
    initScript.write('#!/bin/sh\n')
    initScript.write('#\n')
    initScript.write('# Script automatic genetrated by ...\n')
    initScript.write('# Execute it just before starting your orca executable\n')
    initScript.write('# Use the .orcarc fragment generated as input for your orca executable\n')
    initScript.write('#\n')
    
    for cat in CatalogList:
       tail = cat.ContactString.split("/")
       if len(tail):
         catalogName=tail[len(tail)-1]
         catalogName="./"+catalogName
       else:
         print "ERROR ***: could not parse catalog name ",cat.ContactString
         return
  # HTTP need wget?
       if cat.ContactProtocol == "http":
         initScript.write('wget '+cat.ContactString+'\n')
         initScript.write('exitStatus=$?\n')
         initScript.write('if [ $exitStatus != 0 ] ;then\n  exit $exitStatus\nfi\n')
         # remove http:
         tmp = string.split(cat.ContactString,'/')
         cat.ContactString = './'+tmp[-1]
         cat.ContactProtocol='file'
  # RFIO need rfcp!
       elif cat.ContactProtocol == "rfio":
         initScript.write('rfcp '+cat.ContactString+' .'+'\n')
         initScript.write('exitStatus=$?\n')
         initScript.write('if [ $exitStatus != 0 ] ;then\n  exit $exitStatus\nfi\n')
         # just file name
         cat.ContactString=catalogName
         cat.ContactProtocol='file'
  # gridftp need globus-url-copy!
       elif cat.ContactProtocol == "gridftp":
         initScript.write('globus-url-copy '+cat.ContactString+' file://`pwd`/cat.ContactString'+'\n')
         initScript.write('exitStatus=$?\n')
         initScript.write('if [ $exitStatus != 0 ] ;then\n  exit $exitStatus\nfi\n')
         # just file name
         cat.ContactString=catalogName
         cat.ContactProtocol='file'
  # don't kwow what to do
       else:
         print 'ERROR ***: Unkwown protocol: ',cat.ContactProtocol

    os.chmod(initScriptFileName,0744)
    initScript.close()
    return initScriptFileName

  ###########################################################################
  def createOrcarc(self, CatalogList):
    """
    Create the .orcarc fragment to be used by ORCA application
    """
    if len(CatalogList)==0: 
       print 'Error ***: empty catalog list'
       return

    site = CatalogList[0].CE
    orcarcFileName = 'orcarc_'+site
    orcarc = open(orcarcFileName,'w')
    orcarc.write('# Start of automatic generated .orcarc fragment\n')
    
    
    orcarc.write('InputFileCatalogURL = @{\n')
    for cat in CatalogList:
      orcarc.write('  '+cat.CatalogueType+'catalog_'+cat.ContactProtocol+':'+cat.ContactString+'\n')

    orcarc.write('}@\n')
      
    orcarc.write('# End of automatic generated .orcarc fragment\n')

    orcarc.close()

    return orcarcFileName

  def getCE(self, CatalogList):
    """
    Get the CE from a CatalogList, checking that the CE are the same!
    """

    ce = ''
    ce_sve = ''
    for cat in CatalogList:
      #cat.dump()
      ce = cat.CE
      if (ce_sve!='') & (ce!=ce_sve):
        assert(ce != ce_sve)
      ce_sve=ce
      #print "-==-==-==-==-==-"
      # print Catalog_list

    return ce

  def getSE(self, CatalogList):
    """
    Get the SE from a CatalogList, checking that the SE are the same!
    """

    se = ''
    se_sve = ''
    for cat in CatalogList:
      #cat.dump()
      se = cat.SE
      if (se_sve!='') & (se!=se_sve):
        assert(se != se_sve)
      se_sve=se
      #print "-==-==-==-==-==-"
      # print Catalog_list

    return se

  def getMaxEvents(self, CatalogList):
    """
    Get the Max events from a CatalogList, checking that the Max events are the same!
    """

    num = ''
    num_sve = ''
    for cat in CatalogList:
      #cat.dump()
      num = cat.Nevents
      if (num_sve!='') & (num!=num_sve):
        assert(num != num_sve)
      num_sve=num
      #print "-==-==-==-==-==-"
      # print Catalog_list

    return num
# ###################################################
  def createOrcarcAndInit(self, pubDBResultsSet, maxEvents=-1):
    """
    Create orcarc and init.sh script for all pubDBResult passed for input
    return a list of all file created
    """
    
    # print 'pubDBResults=',pubDBResultsSet
    # print 'pubDBResults[0]=',pubDBResults[0]
    # print 'pubDBResults[1]=',pubDBResults[1]

    fun = __name__+"::createOrcarcAndInit"

    result = []

    for pubDBResults in pubDBResultsSet:
      #pubDBResults[0].dump()
      CatalogList = self.parseResult_(pubDBResults[0])
      
      ce = self.getCE(CatalogList)
      se = self.getSE(CatalogList)
      Nev = self.getMaxEvents(CatalogList)
      # print 'ALL CATALOGS'
      # for cat in CatalogList:
      #   cat.dump()
      #   print "-==-==-==-==-==-"
      # print 'FINE ALL CATALOGS'
      # print CatalogList

      completeCatalogsSet = self.selectCompleteSetOfCatalogs(CatalogList,1)
      if len(completeCatalogsSet)==0:
        # print 'Sorry, no complete list of catalogs available!'
        continue
      #   return result
      # print 'primaryCollId'
      # for cat in completeCatalogsSet:
      #   cat.dump()
      #   print "-==-==-==-==-==-"
      # print 'FINE primaryCollId'

      for res in pubDBResults[1:]:
        CatalogList = self.parseResult_(res)
        
        tmpce = self.getCE(CatalogList)
        tmpse = self.getSE(CatalogList)
        if (tmpce!=ce) | (tmpse !=se):
          break
        # print 'SECONDARIES'
        # for cat in CatalogList:
        #   cat.dump()
        #   print "-==-==-==-==-==-"
        # print 'FINE   SECONDARIES'

        tmpcompleteCatalogsSet = self.selectCompleteSetOfCatalogs(CatalogList, 0)
        if len(tmpcompleteCatalogsSet)==0:
          # print 'Sorry, no complete list of catalogs available!'
          continue
        # for cat in tmpcompleteCatalogsSet:
        #   cat.dump()
        #   print "-==-==-==-==-==-"
       
        for cat in tmpcompleteCatalogsSet:
          completeCatalogsSet.append(cat)

      initFile = self.createStageInScript(completeCatalogsSet)
      # for cat in completeCatalogsSet:
      #   cat.dump()
      #   print "-==-==-==-==-==-"

      orcarcFile = self.createOrcarc(completeCatalogsSet)

      result.append(orcarc(ce, se, initFile, orcarcFile, Nev))

    # print 'result ',result
    return result
