#!/usr/bin/env python

import sys, getopt, string
import imp
import os
import random
from random import SystemRandom
_inst  = SystemRandom()

from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface
from FWCore.ParameterSet.DictTypes import SortedKeysDict
from FWCore.ParameterSet.Modules   import Service
from FWCore.ParameterSet.Types     import *
from FWCore.ParameterSet.Config    import include

import FWCore.ParameterSet.Types   as CfgTypes
import FWCore.ParameterSet.Modules as CfgModules

class ConfigException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self._msg = msg
        return

    def __str__(self):
        return self._msg

def main(argv) :
  """
  writeCfg

  - Read in existing, user supplied cfg or pycfg file
  - Modify job specific parameters based on environment variables
  - Write out modified cfg or pycfg file

  required parameters: none

  optional parameters:
  --help             :       help
  --debug            :       debug statements

  """

  # defaults
  inputFileNames  = None
  parentFileNames = None
  firstRun        = 0
  sourceSeed      = 0
  vtxSeed         = 0
  g4Seed          = 0
  mixSeed         = 0
  debug           = False
  _MAXINT         = 900000000
  maxSeeds        = 4         # Kludge, maximum # of seeds that any engine takes

  try:
    opts, args = getopt.getopt(argv, "", ["debug", "help"])
  except getopt.GetoptError:
    print main.__doc__
    sys.exit(2)

  try:
    CMSSW  = os.environ['CMSSW_VERSION']
    parts = CMSSW.split('_')
    CMSSW_major = int(parts[1])
    CMSSW_minor = int(parts[2])
    CMSSW_patch = int(parts[3])
  except KeyError, ValueError:
    msg = "Your environment doesn't specify the CMSSW version or specifies it incorrectly"
    raise ConfigException(msg)

  # Parse command line options
  for opt, arg in opts :
    if opt  == "--help" :
      print main.__doc__
      sys.exit()
    elif opt == "--debug" :
      debug = True
  # Parse remaining parameters

  try:
    fileName    = args[0];
    outFileName = args[1];
  except IndexError:
    print main.__doc__
    sys.exit()

# Optional Parameters

  maxEvents  = int(os.environ.get('MaxEvents', '0'))
  skipEvents = int(os.environ.get('SkipEvents','0'))
  firstRun   = int(os.environ.get('FirstRun',  '0'))
  nJob       = int(os.environ.get('NJob',      '0'))

  inputFiles     = os.environ.get('InputFiles','')
  parentFiles     = os.environ.get('ParentFiles','')
  preserveSeeds  = os.environ.get('PreserveSeeds','')
  incrementSeeds = os.environ.get('IncrementSeeds','')

# Read Input cfg or python cfg file, FUTURE: Get rid cfg mode

  if fileName.endswith('py'):
    handle = open(fileName, 'r')
    try:   # Nested form for Python < 2.5
      try:
        cfo = imp.load_source("pycfg", fileName, handle)
        cmsProcess = cfo.process
      except Exception, ex:
        msg = "Your pycfg file is not valid python: %s" % str(ex)
        raise "Error: ",msg
    finally:
        handle.close()
  else:
    try:
      print "Importing .cfg file"
      cfo = include(fileName)
      cmsProcess = cfo
    except Exception, ex:
      msg =  "The cfg file is not valid, %s\n" % str(ex)
      raise "Error: ",msg
  print "Getting interface"
  cfg = CfgInterface(cmsProcess)

  # Set parameters for job
  print "Setting parameters"
  inModule = cfg.inputSource
  if maxEvents:
    cfg.maxEvents.setMaxEventsInput(maxEvents)

  if skipEvents:
    inModule.setSkipEvents(skipEvents)

  if inputFiles:
    inputFiles = inputFiles.replace('\\','')
    inputFiles = inputFiles.replace('"','')
    inputFileNames = inputFiles.split(',')
    inModule.setFileNames(*inputFileNames)

  # handle parent files if needed
  if parentFiles:
    parentFiles = parentFiles.replace('\\','')
    parentFiles = parentFiles.replace('"','')
    parentFileNames = parentFiles.split(',')
    inModule.setSecondaryFileNames(*parentFileNames)
    
  # Pythia parameters
  if (firstRun):
    inModule.setFirstRun(firstRun)

  incrementSeedList = []
  preserveSeedList  = []

  if incrementSeeds:
    incrementSeedList = incrementSeeds.split(',')
  if preserveSeeds:
    preserveSeedList  = preserveSeeds.split(',')

  # FUTURE: This function tests the CMSSW version and presence of old-style seed specification. Reduce when we drop support for old versions
  if cfg.data.services.has_key('RandomNumberGeneratorService'): # There are random #'s to deal with
    print "RandomNumberGeneratorService found, will attempt to change seeds"
    ranGenerator = cfg.data.services['RandomNumberGeneratorService']
    ranModules   = getattr(ranGenerator,"moduleSeeds",None)
    if ranModules != None:              # Old format present, no matter the CMSSW version
      print "Old-style random number seeds found, will be changed."
      sourceSeed = int(ranGenerator.sourceSeed.value())
      if ('sourceSeed' in preserveSeedList) or ('theSource' in preserveSeedList):
        pass
      elif ('sourceSeed' in incrementSeedList) or ('theSource' in incrementSeedList):
        ranGenerator.sourceSeed = CfgTypes.untracked(CfgTypes.uint32(sourceSeed+nJob))
      else:
        ranGenerator.sourceSeed = CfgTypes.untracked(CfgTypes.uint32(_inst.randint(1,_MAXINT)))

      for seed in incrementSeedList:
        curSeed = getattr(ranGenerator.moduleSeeds,seed,None)
        if curSeed:
          curValue = int(curSeed.value())
          setattr(ranGenerator.moduleSeeds,seed,CfgTypes.untracked(CfgTypes.uint32(curValue+nJob)))
          preserveSeedList.append(seed)

      for seed in ranGenerator.moduleSeeds.parameterNames_():
        if seed not in preserveSeedList:
          curSeed = getattr(ranGenerator.moduleSeeds,seed,None)
          if curSeed:
            curValue = int(curSeed.value())
            setattr(ranGenerator.moduleSeeds,seed,CfgTypes.untracked(CfgTypes.uint32(_inst.randint(1,_MAXINT))))
    elif CMSSW_major > 2 or (CMSSW_major == 2 and CMSSW_minor >= 1): # Treatment for  seeds, CMSSW 2_1_x and later
      print "New-style random number seeds found, will be changed."
      from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper
      randSvc = RandomNumberServiceHelper(ranGenerator)

      # Increment requested seed sets
      for seedName in incrementSeedList:
        curSeeds = randSvc.getNamedSeed(seedName)
        newSeeds = [x+nJob for x in curSeeds]
        randSvc.setNamedSeed(seedName,*newSeeds)
        preserveSeedList.append(seedName)

      # Randomize remaining seeds
      randSvc.populate(*preserveSeedList)
    else:
      print "Neither old nor new seed format found!"

    # End version specific code

  # Write out new config file in one format or the other, FUTURE: Get rid of cfg mode
  outFile = open(outFileName,"w")
  if outFileName.endswith('py'):
    outFile.write("import FWCore.ParameterSet.Config as cms\n")
    outFile.write(cmsProcess.dumpPython())
    if (debug):
      print "writeCfg output:"
      print "import FWCore.ParameterSet.Config as cms"
      print cmsProcess.dumpPython()
  else:
    outFile.write(str(cfg))
    if (debug):
      print "writeCfg output:"
      print str(cfg)
  outFile.close()


if __name__ == '__main__' :
    exit_status = main(sys.argv[1:])
    sys.exit(exit_status)
