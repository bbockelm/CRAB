#!/usr/bin/env python
import sys, os, commands,string, re
import exceptions
from crab_exceptions import *
from crab_util import *
import common

class DLSError:
  def __init__(self, fileblocks):
    print '\nERROR accessing DLS for fileblock '+fileblocks+'\n'
    pass


class DLSNoReplicas(exceptions.Exception):
  def __init__(self, FileBlock):
    args ="No replicas exists for fileblock: "+FileBlock+"\n"
    exceptions.Exception.__init__(self, args)
    pass

  def getClassName(self):
    """ Return class name. """
    return "%s" % (self.__class__.__name__)

  def getErrorMessage(self):
    """ Return exception error. """
    return "%s" % (self.args)

##############################################################################
# Class to extract info from DLS 
##############################################################################

class DLSInfo:
     def __init__(self, fileblocks):
          self.fileblocks = fileblocks
          self.DLSclient_ = 'dls-get-se '
          self.DLSServer_ = 'lxgate10.cern.ch'
          self.DLSServerPort_ = '18081'
          #self.DLSServerPort_ = '18080'
           
          out=commands.getstatusoutput('which '+self.DLSclient_)
          if out[0]>0:
             msg="ERROR no DLS CLI available in $PATH : %s"%self.DLSclient_
             raise CrabException(msg)

# ####################################
     def getReplicas(self):
         """
          query DLS to get replicas
         """
         ##
         cmd = self.DLSclient_+" --port "+self.DLSServerPort_+" --host "+self.DLSServer_+" --datablock "+self.fileblocks 
         #print cmd
         sites = runCommand(cmd)
         sites=string.strip(sites)
         if len(sites)<=0:
           raise DLSNoReplicas(self.fileblocks)

         ListSites=string.split(string.strip(sites),'\n')
         return ListSites         
