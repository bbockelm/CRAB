#!/usr/bin/env python
"""
_OutputModule_

Util for manipulating OutputModules within a PSet

"""

from Utilities import isQuoted

class OutputModule:
    """
    _OutputModule_

    Util for manipulating an OutputModule's settings within
    a cmsconfig object

    Instantiate with a Reference to the output module dictionary
    so that it can be manipulated.
    """
    def __init__(self, moduleName, moduleDictRef):
        self.name = moduleName
        self.data = moduleDictRef


    def catalog(self):
        """
        _catalog_

        Extract the output catalog, None if not present

        """
        tpl = self.data.get('catalog', None)
        if tpl != None:
            return tpl[2]
        return None

    def setCatalog(self, newCatalog):
        """
        _setCatalog_

        Set the catalog name

        """
        if not isQuoted(newCatalog):
            newCatalog = "\'%s\'" % newCatalog
        self.data['catalog'] = ('string', 'untracked', newCatalog)
        return

    
    def fileName(self):
        """
        _fileName_

        get the fileName parameter, None if not present

        """
        tpl = self.data.get('fileName', None)
        if tpl != None:
            return tpl[2]
        return None

    def setFileName(self, fname):
        """
        _setFileName_

        """
        if not isQuoted(fname):
            fname = "\'%s\'" % fname
        self.data['fileName'] = ('string', 'untracked', fname)

    def logicalFileName(self):
        """
        _logicalFileName_

        """
        tpl = self.data.get('logicalFileName', None)
        if tpl != None:
            return tpl[2]
        return None

    def setLogicalFileName(self, lfn):
        """
        _setLogicalFileName_

        """
        if not isQuoted(lfn):
            lfn = "\'%s\'" % lfn
        self.data['logicalFileName'] = ('string', 'untracked', lfn)

    def __str__(self):
        return str(self.data)
    
    
