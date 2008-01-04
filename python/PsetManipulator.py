#!/usr/bin/env python

import os
import common
from crab_util import *
from crab_exceptions import *
from crab_logger import Logger

from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface
from FWCore.ParameterSet.DictTypes import SortedKeysDict
from FWCore.ParameterSet.Modules   import Service
from FWCore.ParameterSet.Types     import *

import FWCore.ParameterSet.Types   as CfgTypes
import FWCore.ParameterSet.Modules as CfgModules

class PsetManipulator:
    def __init__(self, pset):
        """
        Read in Pset object and initialize
        """

        self.pset = pset
        #convert Pset
        from FWCore.ParameterSet.Config import include
        common.logger.debug(3,"PsetManipulator::__init__: PSet file = "+self.pset)
        if (self.pset.endswith('py') or self.pset.endswith('pycfg') ):
            handle = open(self.pset, 'r')
            try:   # Nested form for Python < 2.5
                try:
                    self.cfo = imp.load_source("pycfg", self.pset, handle)
                except Exception, ex:
                    msg = "Your pycfg file is not valid python: %s" % str(ex)
                    raise CrabException(msg)
            finally:
                handle.close()
            self.cfg = CfgInterface(self.cfo.process)
        else:
            try:
                self.cfo = include(self.pset)
                self.cfg = CfgInterface(self.cfo)
            except Exception, ex:
                msg =  "Your cfg file is not valid, %s\n" % str(ex)
                msg += "  https://twiki.cern.ch/twiki/bin/view/CMS/CrabFaq#Problem_with_ParameterSet_parsin\n"
                msg += "  may help you understand the problem."
                raise CrabException(msg)
        pass

    def inputModule(self, source):
        """
        Set  vString Filenames key
        """
        # set input module
        inModule = self.cfg.inputSource
        inModule.setFileNames(source)
        return

    def pythiaSeed(self,seed):
        """
        Set pythia seed key
        """
        ranGenerator = self.cfg.data.services['RandomNumberGeneratorService']
        ranGenerator.sourceSeed = CfgTypes.untracked(CfgTypes.uint32(seed))
        return

    def vtxSeed(self,vtxSeed):
        """
        Set vtx seed key
        """
        ranGenerator = self.cfg.data.services['RandomNumberGeneratorService']
        ranModules   = ranGenerator.moduleSeeds
        # set seed
        ranModules.VtxSmeared = CfgTypes.untracked(CfgTypes.uint32(vtxSeed))
        return

    def g4Seed(self,g4Seed):
        """
        Set g4 seed key
        """
        ranGenerator = self.cfg.data.services['RandomNumberGeneratorService']
        ranModules   = ranGenerator.moduleSeeds
        # set seed
        ranModules.g4SimHits = CfgTypes.untracked(CfgTypes.uint32(g4Seed))
        return

    def mixSeed(self,mixSeed):
        """
        Set mix seed key
        """
        ranGenerator = self.cfg.data.services['RandomNumberGeneratorService']
        ranModules   = ranGenerator.moduleSeeds
        ranModules.mix = CfgTypes.untracked(CfgTypes.uint32(mixSeed))
        return

    def pythiaFirstRun(self, firstrun):
        """
        Set firstRun
        """
        inModule = self.cfg.inputSource
        inModule.setFirstRun(firstrun)   ## Add Daniele
        return

    def maxEvent(self, maxEv):
        """
        Set max event in the standalone untracked module
        """
        self.cfg.maxEvents.setMaxEventsInput(maxEv)
        return

    def skipEvent(self, skipEv):
        """
        Set skipEvents
        """
        inModule = self.cfg.inputSource
        inModule.setSkipEvents(skipEv)   ## Add Daniele
        return

    def outputModule(self, output):

        #set output module
        outModule = self.cfg.outputModules['out']
        outModule.setFileName('file:'+str(output))

        return

    def psetWriter(self, name):
        """
        Write out modified CMSSW.cfg
        """

        file1 = open(common.work_space.jobDir()+name,"w")
        file1.write(str(self.cfg))
        file1.close()

        return

    def addCrabFJR(self,name):
        """
        _addCrabFJR_
        add CRAB specific FrameworkJobReport (FJR)
        if a FJR already exists in input CMSSW parameter-set, add a second one.
        This code is not needed for CMSSW >= 1.5.x and is non-functional in CMSSW >= 1.7.x.
        It should be removed at some point in the future.
        """

        # Check if MessageLogger service already exists in configuration. If not, add it
        svcs = self.cfg.data.services
        if not svcs.has_key('MessageLogger'):
            self.cfg.data.add_(CfgModules.Service("MessageLogger"))

        messageLogger = self.cfg.data.services['MessageLogger']

        # Add fwkJobReports to Message logger if it doesn't exist
        if "fwkJobReports" not in messageLogger.parameterNames_():
            messageLogger.fwkJobReports = CfgTypes.untracked(CfgTypes.vstring())

        # should figure out how to remove "name" if it is there.

        if name not in messageLogger.fwkJobReports:
            messageLogger.fwkJobReports.append(name)

        return
