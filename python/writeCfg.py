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
  inputFileNames = None
  firstRun       = 0
  sourceSeed     = 0
  vtxSeed        = 0
  g4Seed         = 0
  mixSeed        = 0
  debug          = False

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

  maxEvents  = int(os.environ.get('MaxEvents','0'))
  skipEvents = int(os.environ.get('SkipEvents','0'))
  inputFiles = os.environ.get('InputFiles','')
  firstRun   = int(os.environ.get('FirstRun','0'))
  nJob       = int(os.environ.get('NJob','0'))
  preserveSeeds = os.environ.get('PreserveSeeds','')
  incrementSeeds = os.environ.get('IncrementSeeds','')

# Read Input cfg or python cfg file

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
      cfo = include(fileName)
      cmsProcess = cfo
    except Exception, ex:
      msg =  "The cfg file is not valid, %s\n" % str(ex)
      raise "Error: ",msg
  cfg = CfgInterface(cmsProcess)

  # Set parameters for job
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

  # Pythia parameters
  if (firstRun):
    inModule.setFirstRun(firstRun)

  incrementSeedList = []
  preserveSeedList  = []

  if incrementSeeds:
    incrementSeedList = incrementSeeds.split(',')
  if preserveSeeds:
    preserveSeedList  = preserveSeeds.split(',')

  # FUTURE: This function tests the CMSSW version. Can be simplified as we drop support for old versions
  if CMSSW_major < 3: # True for now, should be < 2 when really ready
  # Treatment for seeds, CMSSW < 2_0_x
    if cfg.data.services.has_key('RandomNumberGeneratorService'):
      ranGenerator = cfg.data.services['RandomNumberGeneratorService']
      ranModules   = ranGenerator.moduleSeeds

      _MAXINT = 900000000

      sourceSeed = int(ranGenerator.sourceSeed.value())
      if 'sourceSeed' in preserveSeedList:
        pass
      elif 'sourceSeed' in incrementSeedList:
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
  else:
    # Treatment for  seeds, CMSSW => 2_0_x
    #from RandomService import RandomSeedService


    # This code not currently working because randSvc is not part of the actual configuration file

    # Translate old format to new format first
    randSvc = RandomSeedService()
    try:
      ranGenerator = cfg.data.services['RandomNumberGeneratorService']
      ranModules   = ranGenerator.moduleSeeds
      for seed in ranGenerator.moduleSeeds.parameters().keys():
        curSeed = getattr(ranGenerator.moduleSeeds,seed,None)
        curValue = int(curSeed.value())
        setattr(randSvc,seed,CfgTypes.PSet())
        curPSet = getattr(randSvc,seed,None)
        curPSet.initialSeed = CfgTypes.untracked(CfgTypes.uint32(curValue))
      del ranGenerator.moduleSeeds # Get rid of seeds in old format
# Doesn't work, filter is false      randSvc.populate()

    except:
      print "Problems converting old seeds to new format"

  # Write out new config file in one format or the other
  outFile = open(outFileName,"w")
  if (outFileName.endswith('py') or outFileName.endswith('pycfg') ):
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

