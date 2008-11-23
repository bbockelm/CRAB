import imp, sys
import traceback
from crab_exceptions import *
from crab_logger import Logger


class ValidateCfg:
    def __init__(self, args):
               
        self.pset = args['CMSSW.pset']
   
    def run(self):
          
        if self.pset.endswith('py'):
            self.DumpPset()   
        else:   
            self.IncludePset()

        return        
  
    def ImportFile(self):
        """
        Read in Pset object
        """
        common.logger.message( "Importing .py file %s"%fileName)
        handle = open(self.pset, 'r')
        try:     
            cfo = imp.load_source("pycfg", self.pset, handle)
            cmsProcess = cfo.process
        except Exception, ex:
            goodcfg = False
            msg = "%s file is not valid python: %s" % (self.pset,str(ex))
            raise CrabException( msg )
        handle.close()
        return cmsProcess   

    def DumpPset(self, cmsProcess ):        
        """ 
        """ 
        common.logger.message( 'Starting the dump.....\n' )
        try:      
            cmsProcess.dumpPython()
        except Exception, ex:      
            msg = "....dumpPython failed : \n\t%s" % str(traceback.format_exc())
            raise CrabException( msg )
        msg = "....dumpPython succeded"
        common.logger.message( msg )

    def IncludePset(self):    
        """ 
        """ 
        from FWCore.ParameterSet.Config import include
        common.logger.message( 'Starting include.....\n' )
        try:
            cfo = include(self.pset)
        except Exception, ex:
            msg = '....include failed with error: \n\t %s'%str(traceback.format_exc())   
            raise CrabException(msg)
        msg = "....include succeded"
        common.logger.message( msg )


if __name__ == '__main__' :

    pset = sys.argv[1]
    check = CheckPset(pset)
    check.Dump()
