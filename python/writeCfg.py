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
from FWCore.ParameterSet.Config import include

import FWCore.ParameterSet.Types   as CfgTypes
import FWCore.ParameterSet.Modules as CfgModules


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
  maxEvents      = 0
  skipEvents     = 0
  inputFileNames = None
  firstRun       = 0
  sourceSeed     = 0
  vtxSeed        = 0
  g4Seed         = 0
  mixSeed        = 0
  debug          = False

  try:
    opts, args = getopt.getopt(argv, "", ["debug", "help","inputFiles=","maxEvents=","skipEvents="])
  except getopt.GetoptError:
    print main.__doc__
    sys.exit(2)

  # Parse command line parameters
  for opt, arg in opts :
    if opt  == "--help" :
      print main.__doc__
      sys.exit()
    elif opt == "--debug" :
      debug = True
    elif opt == "--maxEvents":
      maxEvents = int(arg)
    elif opt == "--skipEvents":
      skipEvents = int(arg)
    elif opt == "--inputFiles":
      inputFiles = arg
      inputFiles = inputFiles.replace('\\','')
      inputFiles = inputFiles.replace('"','')
      inputFileNames = inputFiles.split(',')
    elif opt == "--firstRun":
      firstRun = int(arg)
    elif opt == "--sourceSeed":
      sourceSeed = int(arg)
    elif opt == "--vtxSeed":
      vtxSeed = int(arg)
    elif opt == "--g4Seed":
      g4Seed = int(arg)
    elif opt == "--mixSeed":
      mixSeed = int(arg)

  # Parse remaining parameters

  fileName    = args[0];
  outFileName = args[1];

  # Input cfg or python cfg file

  if (fileName.endswith('py') or fileName.endswith('pycfg') ):
    handle = open(fileName, 'r')
    try:   # Nested form for Python < 2.5
      try:
        cfo = imp.load_source("pycfg", fileName, handle)
      except Exception, ex:
        msg = "Your pycfg file is not valid python: %s" % str(ex)
        raise "Error: ",msg
    finally:
        handle.close()
    cfg = CfgInterface(cfo.process)
  else:
    try:
      cfo = include(fileName)
      cfg = CfgInterface(cfo)
    except Exception, ex:
      msg =  "The cfg file is not valid, %s\n" % str(ex)
      raise "Error: ",msg

  # Set parameters for job

  inModule = cfg.inputSource

  cfg.maxEvents.setMaxEventsInput(maxEvents)

  inModule.setSkipEvents(skipEvents)
  if (inputFileNames):
    inModule.setFileNames(*inputFileNames)

  # Pythia parameters
  if (firstRun):
    inModule.setFirstRun(firstRun)
  if (sourceSeed) :
    ranGenerator = cfg.data.services['RandomNumberGeneratorService']
    ranGenerator.sourceSeed = CfgTypes.untracked(CfgTypes.uint32(sourceSeed))
    if (vtxSeed) :
      ranModules   = ranGenerator.moduleSeeds
      ranModules.VtxSmeared = CfgTypes.untracked(CfgTypes.uint32(vtxSeed))
    if (g4Seed) :
      ranModules   = ranGenerator.moduleSeeds
      ranModules.g4SimHits = CfgTypes.untracked(CfgTypes.uint32(g4Seed))
    if (mixSeed) :
      ranModules   = ranGenerator.moduleSeeds
      ranModules.mix = CfgTypes.untracked(CfgTypes.uint32(mixSeed))

  # Randomize all seeds

  print "There are ",cfg.seedCount()," seeds"
  seedList = [random.randint(100,200) for i in range(cfg.seedCount)]
  seedTuple = seedList.tuple()

  cfg.insertSeeds(*seedTuple)

  # Write out new config file

  outFile = open(outFileName,"w")
  outFile.write("import FWCore.ParameterSet.Config as cms\n")
  outFile.write(cfo.dumpPython())
  outFile.close()

  if (debug):
    print "writeCfg output:"
    print "import FWCore.ParameterSet.Config as cms"
    print cfo.dumpPython()


if __name__ == '__main__' :
    exit_status = main(sys.argv[1:])
    sys.exit(exit_status)

