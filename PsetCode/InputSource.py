#!/usr/bin/env python
"""
_InputSource_

Object to assist with manipulating the input source provided in a PSet

"""
import CfgInterface
from Utilities import isQuoted
import string

class InputSource:
    """
    _InputSource_

    Util for manipulating the InputSource within a cmsconfig object

    """
    def __init__(self, sourceDictRef):
        self.data = sourceDictRef
        self.sourceType = sourceDictRef['@classname'][2]

    def maxevents(self):
        """get value of MaxEvents, None if not set"""
        tpl = self.data.get("maxEvents", None)
        if tpl != None:
            return int(tpl[2])
        return None
   
    ########## Daniele clean settin max event if there
    def cleanMaxEvent(self):
        """ remove Old max event set  """
        tpl = self.data.get("maxEvents", None)
        if tpl != None:
            del self.data['maxEvents'] 
        return None 

    def setMaxEvents(self, maxEv):
        """setMaxEvents value"""
        self.data['maxEvents'] = ('int32', 'untracked', maxEv)

    def skipevents(self):
        """get value of SkipEvents, None if not set"""
        tpl = self.data.get("skipEvents", None)
        if tpl != None:
            return int(tpl[2])
        return None
   
    ########## Daniele clean settin skip event if there
    def cleanSkipEvent(self):
        """ remove Old skip event set  """
        tpl = self.data.get("skipEvents", None)
        if tpl != None:
            del self.data['skipEvents'] 
        return None 

    def setSkipEvents(self, skipEv):
        """setSkipEvents value"""
        self.data['skipEvents'] = ('uint32', 'untracked', skipEv)

    def firstRun(self):
        """get firstRun value of None if not set"""
        tpl = self.data.get("firstRun", None)
        if tpl != None:
            return int(tpl[2])
        return None
        
    def setFirstRun(self, firstRun):
        """set first run number"""
        self.data['firstRun'] = ('uint32', 'untracked', firstRun)
        
    def fileNames(self):
        """ return value of fileNames, None if not provided """
        tpl = self.data.get("fileNames", None)
        if tpl != None:
            return tpl[2]
        return None

    ########## Daniele clean  string input file if there
    def cleanStringFileNames(self):
        """ remove Old String fileNames  """
        tpl_1 = self.data.get("fileName", None)
        if tpl_1 != None:
            del self.data['fileName'] 
        return None 
   

    def setFileNames(self, *fileNames):
        """set fileNames vector"""
        fnames = []
        for fname in fileNames:
            if not isQuoted(fname):
                fname = "\'%s\'" % fname
            fnames.append(fname)
        self.data['fileNames'] = ('vstring', 'untracked', fnames)
        
            
        
    def setVtxSeed(self,cfg, vtxSeed):
        """set pythia  vertex seed"""

        modName = 'moduleSeeds'
        _SvcName = "RandomNumberGeneratorService" 
        seedSvc = cfg.cmsConfig.service(_SvcName)
        check = 0
        if  modName in seedSvc.keys():
            for i in range(len(seedSvc[modName])): 
                if string.find(str(seedSvc[modName][i]), 'VtxSmeared') != -1: 
                    seedSvc[modName][i]["VtxSmeared"] =  ('uint32', 'untracked', vtxSeed )
                    check = 1
            if not check: print 'no VtxSmeared in ',modName
        else:
            print 'no ',modName,' in Pset '
            pass        

    def setG4Seed(self,cfg, g4Seed):
        """set G4 seed"""

        modName = 'moduleSeeds'
        _SvcName = "RandomNumberGeneratorService" 
        seedSvc = cfg.cmsConfig.service(_SvcName)
        check = 0
        if  modName in seedSvc.keys():
            for i in range(len(seedSvc[modName])): 
                if string.find(str(seedSvc[modName][i]), 'g4SimHits') != -1: 
                    seedSvc[modName][i]["g4SimHits"] =  ('uint32', 'untracked', vtxSeed )
                    check = 1
            if not check: print 'no g4SimHits in ',modName
        else:
            print 'no ',modName,' in Pset '
            pass        

    def setMixSeed(self,cfg, g4Seed):
        """set Mixing seed"""

        modName = 'moduleSeeds'
        _SvcName = "RandomNumberGeneratorService" 
        seedSvc = cfg.cmsConfig.service(_SvcName)
        check = 0
        if  modName in seedSvc.keys():
            for i in range(len(seedSvc[modName])): 
                if string.find(str(seedSvc[modName][i]), 'mix') != -1: 
                    seedSvc[modName][i]["mix"] =  ('uint32', 'untracked', vtxSeed )
                    check = 1
            if not check: print 'no mix in ',modName
        else:
            print 'no ',modName,' in Pset '
            pass        

    def setPythiaSeed(self,cfg, seed):
        """set pythia seed"""

        _SvcName = "RandomNumberGeneratorService" 
        seedSvc = cfg.cmsConfig.service(_SvcName)
        #  //
        # // insert Source Seed
        seedSvc["sourceSeed"] = ('uint32', 'untracked', seed )

