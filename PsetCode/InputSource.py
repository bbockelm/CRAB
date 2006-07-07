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
        
            
        
    def setPythiaVtxSeed(self,cfg, vtxSeed):
        """set pythia  vertex seed"""

        _SvcName = "RandomNumberGeneratorService" 
        seedSvc = cfg.cmsConfig.service(_SvcName)
        check = 0
        if  'moduleSeed' in seedSvc.keys():
            for i in range(len(seedSvc["moduleSeed"])): 
                if string.find(str(seedSvc["moduleSeed"][i]), 'VtxSmeared') != -1: 
                    seedSvc["moduleSeed"][i]["VtxSmeared"] =  ('uint32', 'untracked', vtxSeed )
                    check = 1
            if not check: print 'no VtxSmeared in  moduleSeed '
        else:
            print 'no  moduleseed in Pset '
            pass        

    def setPythiaSeed(self,cfg, seed):
        """set pythia seed"""

        _SvcName = "RandomNumberGeneratorService" 
        seedSvc = cfg.cmsConfig.service(_SvcName)
        #  //
        # // insert Source Seed
        seedSvc["sourceSeed"] = ('uint32', 'untracked', seed )

