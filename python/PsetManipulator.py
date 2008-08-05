#!/usr/bin/env python

import os
import common
import imp

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
        if self.pset.endswith('py'):
            handle = open(self.pset, 'r')
            try:   # Nested form for Python < 2.5
                try:
                    self.cfo = imp.load_source("pycfg", self.pset, handle)
                    self.cmsProcess = self.cfo.process
                except Exception, ex:
                    msg = "Your config file is not valid python: %s" % str(ex)
                    raise CrabException(msg)
            finally:
                handle.close()
        else:
            try:
                self.cfo = include(self.pset)
                self.cmsProcess = self.cfo
            except Exception, ex:
                msg =  "Your cfg file is not valid, %s\n" % str(ex)
                msg += "  https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrabFaq#Problem_with_ParameterSet_parsin\n"
                msg += "  may help you understand the problem."
                raise CrabException(msg)
        self.cfg = CfgInterface(self.cmsProcess)

    def maxEvent(self, maxEv):
        """
        Set max event in the standalone untracked module
        """
        self.cfg.maxEvents.setMaxEventsInput(maxEv)
        return

    def psetWriter(self, name):
        """
        Write out modified CMSSW.cfg
        """

        # FUTURE: Can drop cfg mode for CMSSW < 2_1_x
        outFile = open(common.work_space.jobDir()+name,"w")
        if name.endswith('py'):
            outFile.write("import FWCore.ParameterSet.Config as cms\n")
            outFile.write(self.cmsProcess.dumpPython())
        else:
            outFile.write(self.cfg.data.dumpConfig())
        outFile.close()

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

    def getTFileService(self):
        """ Get Output filename from TFileService and return it. If not existing, return None """
        if not self.cfg.data.services.has_key('TFileService'):
            return None
        tFileService = self.cfg.data.services['TFileService']
        if "fileName" in tFileService.parameterNames_():
            fileName = getattr(tFileService,'fileName',None).value()
            return fileName
        return None

    def getPoolOutputModule(self):
        """ Get Output filename from PoolOutputModule and return it. If not existing, return None """
        if not self.cfg.data.outputModules:
            return None
        poolOutputModule = self.cfg.data.outputModules
        for out in poolOutputModule:
            return poolOutputModule[out].fileName.value()

