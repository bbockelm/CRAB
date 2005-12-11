#!/usr/bin/env python
import sys, os, commands,string, re
from crab_util import *
import common

class DLSError:
  def __init__(self, fileblocks):
    print '\nERROR accessing DLS for fileblock '+fileblocks+'\n'
    pass

##############################################################################
# Class to extract info from DLS 
##############################################################################

class DLSInfo:
     def __init__(self, fileblocks):
          self.fileblocks = fileblocks
          self.DLSclient_ = 'DLSAPI/dls-get-se '
          self.DLSServer_ = 'lxgate10.cern.ch'
          self.DLSServerPort_ = '18081'
          #self.DLSServerPort_ = '18080'

# ####################################
     def getReplicas(self):
         """
          query DLS to get replicas
         """
         ##
         cmd = self.DLSclient_+" --port "+self.DLSServerPort_+" --host "+self.DLSServer_+" --datablock "+self.fileblocks 
         sites = runCommand(cmd)
         ListSites=string.split(string.strip(sites),'\n')
         return ListSites         
