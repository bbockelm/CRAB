#!/usr/bin/env python
                                                                                                                                                             
import os, sys, commands
import common
from crab_util import *
from crab_exceptions import *

from cmsconfig import cmsconfig
from CfgInterface import CfgInterface

class PsetManipulator:
    def __init__(self, pset):
        """ 
        Convert Pset in Python format  
        and initialize   
        """

        self.pset = pset
        #convert Pset
        self.pyPset = os.path.basename(pset)  
        cmd = 'EdmConfigToPython > '+common.work_space.shareDir()+self.pyPset+'py < '+ self.pset
        cmd_out = runCommand(cmd)  
        if cmd_out != '':
            msg = 'Could not convert Pset.cfg into python Dictionary \n'
            msg1= '      Did you do eval `scramv1 runtime ...` from your CMSSW working area ?'
            raise CrabException(msg+msg1)
            pass
        
        self.par = file(common.work_space.shareDir()+self.pyPset+'py').read()

        # get PSet
        self.cfg = CfgInterface(self.par,True)

    def inputModule(self, source):
        """ Clean  String FileName if there
            and add  vString Filenames key
        """
        # set input module
        inModule = self.cfg.inputSource
        inModule.cleanStringFileNames() ## Add Daniele
        inModule.setFileNames(source)
        return
  
    def pythiaSeed(self,seed):
        """ 
        Set pythia seed key
        """
        # set seed
        inModule = self.cfg.inputSource
        inModule.setPythiaSeed(self.cfg,seed)
        return 

    def pythiaSeedVtx(self,vtxSeed):
        """ 
        Set vtx seed key
        """
        # set seed
        inModule = self.cfg.inputSource
        inModule.setPythiaVtxSeed(self.cfg,vtxSeed)
        return 

    def pythiaFirstRun(self, firstrun):
        """ """ 
        # set input module
        inModule = self.cfg.inputSource
        inModule.setFirstRun(firstrun)   ## Add Daniele 
        return

    def maxEvent(self, maxEv):
        """ """ 
        # set input module
        inModule = self.cfg.inputSource
        inModule.cleanMaxEvent()   
        inModule.setMaxEvents(maxEv)   ## Add Daniele 
        return

    def skipEvent(self, skipEv):
        """ """ 
        # set input module
        inModule = self.cfg.inputSource
        inModule.cleanSkipEvent()
        inModule.setSkipEvents(skipEv)   ## Add Daniele 
        return

    def outputModule(self, output):

        #set output module
        outModule = self.cfg.outputModules['out']
        outModule.setFileName('file:'+str(output))

        return

    def psetWriter(self, name):

        configObject = cmsconfig(str(self.cfg))

        file1 = open(common.work_space.jobDir()+name,"w")
        file1.write(str(configObject.asConfigurationString()))
        file1.close()

        return

    def addCrabFJR(self,name):
        """

        _addCrabFJR_

        add CRAB specific FrameworkJobReport (FJR)

        if already a FJR exist in input CMSSW parameter-set, add a second one

        """

        # check if MessageLogger service already exist in configuration, if not, add it
        if not "MessageLogger" in self.cfg.cmsConfig.serviceNames() :
            self.cfg.cmsConfig.psdata['services']['MessageLogger'] = {
                '@classname': ('string', 'tracked', 'MessageLogger'),
                }
            
        # get MessageLogger service
        loggerSvc = self.cfg.cmsConfig.service("MessageLogger")

        # check if FJR is in MessageLogger service configuration, if not, add it
        if not loggerSvc.has_key("fwkJobReports"):
            loggerSvc['fwkJobReports'] = ("vstring", "untracked", [])

        # check if crab FJR configuration is in MessageLogger configuration, if not, add it
        if not '\"'+name+'\"' in loggerSvc['fwkJobReports'][2] :
            loggerSvc['fwkJobReports'][2].append('\"'+name+'\"')

        # check that default is taken for CRAB FJR configuration and any user specific is removed
        if loggerSvc.has_key(name):
            del loggerSvc[name]
