#!/usr/bin/env python
"""
_CfgInterface_

Wrapper class for a cmsconfig object with interfaces to manipulate
the InputSource and output modules

"""

import copy

from cmsconfig import cmsconfig
from InputSource import InputSource
from OutputModule import OutputModule


class CfgInterface:
    """
    _CfgInterface_

    Wrapper object for a cmsconfig instance.
    Generates an InputSource object and OutputModules from the
    cfg file.

    Provides a clone interface that returns a new copy of itself to
    assist in generating jobs from a Template cfg file

    Ctor argument can be either the file containing the cfg file, or
    the content of it as a string. In the latter case, the second ctor
    arg must be provided as True.

    """
    def __init__(self, pyFormatCfgFile, isString = False):
        if not isString:
            cfgContent = file(pyFormatCfgFile).read()
        else:
            cfgContent = pyFormatCfgFile
            
        self.cmsConfig = cmsconfig(cfgContent)
        self.inputSource = InputSource(self.cmsConfig.mainInputSource())
        self.outputModules = {}
        for omodName in self.cmsConfig.outputModuleNames():
            self.outputModules[omodName] = OutputModule(
                omodName, self.cmsConfig.module(omodName)
                )

    def clone(self):
        """
        _clone_

        return a new instance of this object by copying it

        """
        return copy.deepcopy(self)

    
    def __str__(self):
        """string rep of self: give python format PSet"""
        return self.cmsConfig.asPythonString()
