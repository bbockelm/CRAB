#!/usr/bin/env python

"""
Re-write config file and optionally convert to python
"""

__revision__ = "$Id: writeCfg.py,v 1.18 2009/05/04 21:45:43 ewv Exp $"
__version__ = "$Revision: 1.18 $"

import getopt
import imp
import os
import pickle
import sys
import xml.dom.minidom

from random import SystemRandom

from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface
from FWCore.ParameterSet.Config                       import include
import FWCore.ParameterSet.Types as CfgTypes

MyRandom  = SystemRandom()

class ConfigException(Exception):
    """
    Exceptions raised by writeCfg
    """

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
    debug           = False
    _MAXINT         = 900000000

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
    except (KeyError, ValueError):
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
        fileName    = args[0]
        outFileName = args[1]
    except IndexError:
        print main.__doc__
        sys.exit()

  # Read in Environment, XML and get optional Parameters

    nJob       = int(os.environ.get('NJob',      '0'))
    preserveSeeds  = os.environ.get('PreserveSeeds','')
    incrementSeeds = os.environ.get('IncrementSeeds','')

  # Defaults

    maxEvents  = 0
    skipEvents = 0
    firstEvent = -1
    compHEPFirstEvent = 0
    firstRun   = 0

    dom = xml.dom.minidom.parse(os.environ['RUNTIME_AREA']+'/arguments.xml')

    for elem in dom.getElementsByTagName("Job"):
        if nJob == int(elem.getAttribute("JobID")):
            if elem.getAttribute("MaxEvents"):
                maxEvents = int(elem.getAttribute("MaxEvents"))
            if elem.getAttribute("SkipEvents"):
                skipEvents = int(elem.getAttribute("SkipEvents"))
            if elem.getAttribute("FirstEvent"):
                firstEvent = int(elem.getAttribute("FirstEvent"))
            if elem.getAttribute("CompHEPFirstEvent"):
                compHEPFirstEvent = int(elem.getAttribute("CompHEPFirstEvent"))
            if elem.getAttribute("FirstRun"):
                firstRun = int(elem.getAttribute("FirstRun"))

            inputFiles     = str(elem.getAttribute('InputFiles'))
            parentFiles    = str(elem.getAttribute('ParentFiles'))

  # Read Input cfg or python cfg file, FUTURE: Get rid cfg mode

    if fileName.endswith('py'):
        handle = open(fileName, 'r')
        try:   # Nested form for Python < 2.5
            try:
                print "Importing .py file"
                cfo = imp.load_source("pycfg", fileName, handle)
                cmsProcess = cfo.process
            except Exception, ex:
                msg = "Your pycfg file is not valid python: %s" % str(ex)
                raise ConfigException(msg)
        finally:
            handle.close()
    else:
        try:
            print "Importing .cfg file"
            cfo = include(fileName)
            cmsProcess = cfo
        except Exception, ex:
            msg =  "The cfg file is not valid, %s\n" % str(ex)
            raise ConfigException(msg)

    cfg = CfgInterface(cmsProcess)

    # Set parameters for job
    print "Setting parameters"
    inModule = cfg.inputSource
    if maxEvents:
        cfg.maxEvents.setMaxEventsInput(maxEvents)

    if skipEvents:
        inModule.setSkipEvents(skipEvents)
    if firstEvent != -1:
        cmsProcess.source.firstEvent = CfgTypes.untracked(CfgTypes.uint32(firstEvent))
    if compHEPFirstEvent:
        cmsProcess.source.CompHEPFirstEvent = CfgTypes.int32(compHEPFirstEvent)
    if inputFiles:
        inputFileNames = inputFiles.split(',')
        inModule.setFileNames(*inputFileNames)

    # handle parent files if needed
    if parentFiles:
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

    # FUTURE: This function tests the CMSSW version and presence of old-style seed specification.
    # Reduce when we drop support for old versions
    if cfg.data.services.has_key('RandomNumberGeneratorService'): # There are random #'s to deal with
        print "RandomNumberGeneratorService found, will attempt to change seeds"
        ranGenerator = cfg.data.services['RandomNumberGeneratorService']
        ranModules   = getattr(ranGenerator, "moduleSeeds", None)
        oldSource    = getattr(ranGenerator, "sourceSeed",  None)
        if ranModules != None or oldSource != None:     # Old format present, no matter the CMSSW version
            print "Old-style random number seeds found, will be changed."
            if oldSource != None:
                sourceSeed = int(ranGenerator.sourceSeed.value())
                if ('sourceSeed' in preserveSeedList) or ('theSource' in preserveSeedList):
                    pass
                elif ('sourceSeed' in incrementSeedList) or ('theSource' in incrementSeedList):
                    ranGenerator.sourceSeed = CfgTypes.untracked(CfgTypes.uint32(sourceSeed+nJob))
                else:
                    ranGenerator.sourceSeed = CfgTypes.untracked(CfgTypes.uint32(MyRandom.randint(1, _MAXINT)))

            for seed in incrementSeedList:
                curSeed = getattr(ranGenerator.moduleSeeds, seed, None)
                if curSeed:
                    curValue = int(curSeed.value())
                    setattr(ranGenerator.moduleSeeds, seed, CfgTypes.untracked(CfgTypes.uint32(curValue+nJob)))
                    preserveSeedList.append(seed)

            if ranModules != None:
                for seed in ranGenerator.moduleSeeds.parameterNames_():
                    if seed not in preserveSeedList:
                        curSeed = getattr(ranGenerator.moduleSeeds, seed, None)
                        if curSeed:
                            curValue = int(curSeed.value())
                            setattr(ranGenerator.moduleSeeds, seed,
                                    CfgTypes.untracked(CfgTypes.uint32(MyRandom.randint(1,_MAXINT))))
        elif CMSSW_major > 2 or (CMSSW_major == 2 and CMSSW_minor >= 1): # Treatment for  seeds, CMSSW 2_1_x and later
            print "New-style random number seeds found, will be changed."
            from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper
            randSvc = RandomNumberServiceHelper(ranGenerator)

            # Increment requested seed sets
            for seedName in incrementSeedList:
                curSeeds = randSvc.getNamedSeed(seedName)
                newSeeds = [x+nJob for x in curSeeds]
                randSvc.setNamedSeed(seedName, *newSeeds)
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
        outFile.write("import pickle\n")
        outFile.write("pickledCfg=\"\"\"%s\"\"\"\n" % pickle.dumps(cmsProcess))
        outFile.write("process = pickle.loads(pickledCfg)\n")
        if (debug):
            print "writeCfg output (May not be exact):"
            print "import FWCore.ParameterSet.Config as cms"
            print cmsProcess.dumpPython()
    else:
        outFile.write(cfg.data.dumpConfig())
        if (debug):
            print "writeCfg output:"
            print str(cfg.data.dumpConfig())
    outFile.close()


if __name__ == '__main__' :
    exit_status = main(sys.argv[1:])
    sys.exit(exit_status)
