import sys, os, re, string
import PubDB
import common

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

  def dump(self):
    print "SE ",self.SE
    print "CE ",self.CE
    print "initFile ",self.initFile
    print "orcarcFile ",self.orcarcFile
    print "Nevents ",self.Nevents

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
class catalogEntryNew:
  def __init__(self,
               CollType,
               PrimaryCollFlag,
               FileType,
               ValidationStatus,
               ContactString,
               ContactProtocol,
               CatalogueType,
               SE,
               CElist,
               Nevents,
               RunRange,
               RedirectionVariables):
    self.CollType=CollType
    self.PrimaryCollFlag=PrimaryCollFlag
    self.FileType=FileType
    self.ValidationStatus=ValidationStatus
    self.ContactString=ContactString
    self.ContactProtocol=string.lower(ContactProtocol)
    self.CatalogueType=string.lower(CatalogueType)
    self.SE=SE
    self.CElist=CElist
    self.Nevents=Nevents
    self.RunRange=RunRange
    self.RedirectionVariables=RedirectionVariables
  
  def dump(self):
    print "CollType ",self.CollType
    print "PrimaryCollFlag ",self.PrimaryCollFlag
    print "FileType ",self.FileType
    print "ValidationStatus ",self.ValidationStatus
    print "ContactString ",self.ContactString
    print "ContactProtocol ",self.ContactProtocol
    print "CatalogueType ",self.CatalogueType
    print "SE ",self.SE
    for aCE in self.CElist:
     print "CE ",aCE
    print "Nevents ",self.Nevents
    print "RunRange ",self.RunRange
    print "RedirectionVariables ",self.RedirectionVariables

###########################################################################
###########################################################################
class orcarcBuilder:
  def __init__(self,cfg_params):
    self.cfg_params=cfg_params
    self.fileList = []
    self.CE = []
    self.SE = []
    pass

###########################################################################    
  def selectCompleteSetOfCatalogs(self, CatalogList):
    """
    Select from the many catalogs returned from the PubDB a complete and unique
    set of them, which contains all needed, according to the pre-defined priority
    """
    result = []
    metaCat= []
    reComplete = re.compile( r'Complete' )
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
        for m in metaCat:
         result.append(m)
        return result

    # priority 2: assume attacchedMeta + Events
    metaCat = []
    evdCat = []
    for cat in CatalogList:
        if cat.FileType=='AttachedMETA':
          metaCat.append(cat)
          #cat.dump()
          #print "-==-==-==-==-==-"
        elif cat.FileType=='Events':
          evdCat.append(cat)
          #cat.dump()
          #print "-==-==-==-==-==-"

    # remove duplicated META catalogues
    contactstrings=[]
    uniq_metaCat=[]
    for mc in metaCat:
       if mc.ContactString not in contactstrings:
        contactstrings.append(mc.ContactString)
        uniq_metaCat.append(mc)
    metaCat=uniq_metaCat 
    #for mcat in uniq_metaCat:
    #  print mcat.ContactString

    ## order Events catalogues by datatier
    sort_evdCat=self.sortbyDataTier(evdCat)
    evdCat=sort_evdCat
    if (len(metaCat)>0) & (len(evdCat)>0):
        for m in metaCat:
            result.append(m)
        for e in evdCat:
            result.append(e)
        return result

    return []

  ###########################################################################
  def sortbyDataTier(self, CatalogList):
    """
     sort by datatier
    """
    ## use the user defined ordering if present , otherwise use the default 
    try:
        sortedDataTier= string.split(self.cfg_params['USER.order_catalogs'],',')
    except KeyError: 
        sortedDataTier=['Hit','InitHit','PU','InitDigi','Digi','DST','DSTStreams']
    #print sortedDataTier


    sortCatalogList=[]
    # sanity check
    if len(CatalogList) == 0:
      print 'Error ***: empty catalog list'
      return
   
    ## sorting algorithm
    for datatier in sortedDataTier:
      for cat in CatalogList:
       if ( cat.CollType == datatier ) :
        sortCatalogList.append(cat)   

    ## additional check: to append at the end catalogues with data tier 
    ## for which ordering has not been defined
    try:
        user_DataTier= string.split(self.cfg_params['USER.data_tier'],',')
    except KeyError:
        user_DataTier=[]

    for dt in user_DataTier:
     if ( dt == "Digi") : user_DataTier.append('PU')

    #print user_DataTier
    for dt in user_DataTier:
     if sortedDataTier.count(dt)<=0:
       for cat in CatalogList:
         if ( cat.CollType == dt ):
           sortCatalogList.append(cat)


    return sortCatalogList

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
    
    site = self.getCE(CatalogList)

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
       elif cat.ContactProtocol == "mysql":
         # do nothing since the MySQl catalogue contact string is already enough
         continue
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

    site = self.getCE(CatalogList)

    orcarcFileName = 'orcarc_'+site
    orcarc = open(orcarcFileName,'w')
    orcarc.write('# Start of automatic generated .orcarc fragment\n')
    

    COBRAredirectionList=[]    
    orcarc.write('InputFileCatalogURL = @{\n')
    for cat in CatalogList:
      ## write input POOL catalogues
      #print " contact string is "+cat.ContactString
      orcarc.write('  '+cat.CatalogueType+'catalog_'+cat.ContactProtocol+':'+cat.ContactString+'\n')
      ## look for COBRA redirection variables 
      if (cat.RedirectionVariables!='') & (cat.RedirectionVariables not in COBRAredirectionList): 
        COBRAredirectionList.append(cat.RedirectionVariables)

    orcarc.write('}@\n')

    if len(COBRAredirectionList)>0 :
     orcarc.write('TFileAdaptor = true \n')
     for COBRAvariable in COBRAredirectionList:
       variableName=COBRAvariable.split("=")[0]
       variableValue=COBRAvariable.split("=")[1]
       #print ' add to orcarc the COBRA redirection variable '+variableName+' = @{'+variableValue+'}@\n'
       orcarc.write(variableName+' = @{'+variableValue+'}@\n')
      
    orcarc.write('# End of automatic generated .orcarc fragment\n')

    orcarc.close()

    return orcarcFileName

  #############################################################################
  def getCE(self, CatalogList):
    """
    Get the CE from a CatalogList, picking up the CE from where all the needed data are!
    """
    ### List all the possible CEs
    CEs=[]
    for cat in CatalogList:    
      for ce in cat.CElist:           
       #print " for catalogue "+cat.ContactString+" CE is "+ce
       if ce not in CEs :
          CEs.append(ce)   

    ### List good CEs: only the CEs that are associated to _all_ the needed catalogues
    goodCElist=[]
    for aCE in CEs:
      for cat in CatalogList:
        skip=1
        if aCE not in cat.CElist:
          #print "the CE "+aCE+" is not among the CEs in this catalogue => skip it"
          skip=0
          break
      if (skip) : goodCElist.append(aCE)      
         
    ### Return the first CE since otherwise is too complex afterword
    ### to deal with the rest of CRAB ( JDL creation and so on in cms_orca.py)
    goodce=''
    if len(goodCElist) <= 0 :
      ## if the CE are different implement a similar behaviour 
      ## as in orcarcBuilder.py : assert statement and not raising exceptions
      assert( len(goodCElist) <= 0 )
      goodce=CEs[0]
    else:
      goodce=goodCElist[0]

    return goodce

  #############################################################################
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

  #############################################################################
  def getMaxEvents(self, CatalogList):
    """
    Get the Max events from a CatalogList, checking that the Max events are the same!
    """
    ### pick up the Nevents of the primary collection catalogue, with a preference
    ### for Events catalogues 
    stop=1
    for cat in CatalogList:
      if ( cat.PrimaryCollFlag ):
       if(stop):
         if ( cat.FileType == 'Events' ):
           num_primary=cat.Nevents
           stop=0
         else :
           num_primary=cat.Nevents
    return num_primary

# ###################################################
  def createOrcarcAndInit(self, pubDBResultsSet, maxEvents=-1):
    """
    Create orcarc and init.sh script for all pubDBResult passed for input
    return a list of all file created
    """
    
    result = []

    for CatalogList in pubDBResultsSet:
      
      ce = self.getCE(CatalogList)
      se = self.getSE(CatalogList)
      Nev = self.getMaxEvents(CatalogList)
      #print 'ALL CATALOGS'
      #for cat in CatalogList:
      #   cat.dump()
      #   print "-==-==-==-==-==-"
      #   print 'FINE ALL CATALOGS'
      # print CatalogList

      completeCatalogsSet = self.selectCompleteSetOfCatalogs(CatalogList)
      if len(completeCatalogsSet)==0:
        #print 'Sorry, no complete list of catalogs available!'
        continue
      #   return result
      # print 'primaryCollId'
      #for cat in completeCatalogsSet:
      #   cat.dump()
      #   print "-==-==-==-==-==-"
      #print 'FINE primaryCollId'
     
      #print "skip search for secondaries.... in new-style PubDB the info are in one shot"

      initFile = self.createStageInScript(completeCatalogsSet)
      #for cat in completeCatalogsSet:
      #   cat.dump()
      #   print "-==-==-==-==-==-"

      orcarcFile = self.createOrcarc(completeCatalogsSet)

      result.append(orcarc(ce, se, initFile, orcarcFile, Nev))

    # print 'result ',result
    return result
