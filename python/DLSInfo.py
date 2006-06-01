#!/usr/bin/env python
import sys, os, commands,string, re
import exceptions
from crab_exceptions import *
from crab_util import *
import common

try:
    import dlsApi
    import dlsClient
    from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
except:
    try:
        Crabpydir=commands.getoutput('which crab')
        Topdir=string.replace(Crabpydir,'/python/crab','')
        sys.path.append(Topdir+'/DLSAPI')
        import dlsApi
        import dlsClient
        from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry
    except:
        msg="ERROR no DLS API available"
        raise CrabException(msg)
                                                                                            
## for python 2.2 add the pyexpat.so to PYTHONPATH
pythonV=sys.version.split(' ')[0]
if pythonV.find('2.2') >= 0 :
 Crabpydir=commands.getoutput('which crab')
 Topdir=string.replace(Crabpydir,'/python/crab','')
 extradir=Topdir+'/DLSAPI/extra'
 if sys.path.count(extradir) <= 0:
   if os.path.exists(extradir):
    sys.path.insert(0, extradir)


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
    def __init__(self, type, jobtype):
        if type=="DLS_TYPE_DLI":
           if jobtype.count('orca')>0:
             endpoint="lfc-cms-test.cern.ch/grid/cms/DLS/LFCProto"
           else:  
             endpoint="lfc-cms-test.cern.ch/grid/cms/DLS/LFC"
           try:
             import xml.dom.ext.reader
           except:
             crabdir=os.getenv('CRABDIR')
## Let the user set up PyXML by hand
             msg="There is no setup of PyXML python module required by DLS (DLI). Do the following:\n"
             msg+=" - check that in  %s/configure  the function configureDLSAPI is not commented \n"%crabdir
             msg+=" - uncomment it and re-run the configuration :"
             msg+="\n    cd %s\n"%crabdir
             msg+="     ./configure\n"
             msg+="     source crab.(c)sh\n"
             raise CrabException(msg)

        elif type=="DLS_TYPE_MYSQL":
           endpoint="lxgate10.cern.ch:18081"
        else:
           msg = "DLS type %s not among the supported DLS ( DLS_TYPE_DLI and DLS_TYPE_MYSQL ) "%type
           raise CrabException(msg)

        common.logger.debug(5,"DLS interface: %s Server %s"%(type,endpoint))       
        try:
          self.api = dlsClient.getDlsApi(dls_type=type,dls_endpoint=endpoint)
        except dlsApi.DlsApiError, inst:
          msg = "Error when binding the DLS interface: %s  Server %s"%(str(inst),self.DLSServer_)
          #print msg
          raise CrabException(msg)

# ####################################
    def getReplicas(self,fileblocks):
        """
        query DLS to get replicas
        """
        ##
        try:
          entryList=self.api.getLocations([fileblocks])
        except dlsApi.DlsApiError, inst:
          msg = "Error in the DLS query: %s." % str(inst)
          #print msg
          raise DLSNoReplicas(fileblocks)

        ListSites=[] 
        for entry in entryList:
         for loc in entry.locations:
           ListSites.append(str(loc.host))
        if len(ListSites)<=0:
          raise DLSNoReplicas(fileblocks)

        return ListSites         
