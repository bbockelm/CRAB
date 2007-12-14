#!/usr/bin/env python

import sys, getopt, string
import imp
import os

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
  maxEvents = 0
  skipEvents = 0
  inputFileNames = None
  debug = False

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

