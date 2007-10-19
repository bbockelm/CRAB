from PsetManipulator import *
from crab_logger import Logger
from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface

class PsetManipulator(PsetManipulator) :
    def __init__(self, pset):
        """
        Convert Pset in Python format  and initialize
        To be used with CMSSW version >150
        """

        self.pset = pset

        from FWCore.ParameterSet.Config import include
        common.logger.debug(3,"PsetManipulator::__init__: PSet file = "+self.pset)
        self.cfo = include(self.pset)
        self.cfg = CfgInterface(self.cfo)

    def maxEvent(self, maxEv):
        """ Set max event in the standalone untracked module """
        self.cfg.maxEvents.setMaxEventsInput(maxEv)
        return
