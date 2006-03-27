#!/usr/bin/env python
"""
_GridCatService_

GridCat interface class that wraps the XMLRPC UI

"""

import xmlrpclib
import sys

class GridCatService:
    """
    _GridCatService_

    GridCat interface class generic interface class for no specific site

    """
    def __init__(self, gridCatURL):
        self._URL = gridCatURL
        try:
            self._Server = xmlrpclib.Server(self._URL)
        except StandardError, ex:
            #print ""
            msg = "Unable to connect to GridCat Server:\n"
            msg += "%s\n" % self._URL
            msg += "Error:\n%s\n" % str(ex)
            raise RuntimeError, msg

        
        
    def hostnames(self):
        """
        _hostnames_

        list hostnames
        """
        hostList = self._Server.hostnames().split()
        return hostList
    

    def sitenames(self):
        """
        _sitenames_

        return a list of sitenames
        """
        siteList = self._Server.sitenames().split()
        return siteList
    




class GridCatHostService(GridCatService):
    """
    _GridCatHostService_

    Specialisation of GridCatService that deals only with a single
    host, provided as a FQDN on instantiation
    
    """
    def __init__(self, serverURL, hostname):
        GridCatService.__init__(self, serverURL)
        self.hostname = hostname
        if hostname not in self.hostnames():
            msg = "GridCatService @ %s\n" % serverURL
            msg += "Does not know about host: %s\n" % hostname
            raise RuntimeError, msg
        sitename= self._Server.getresult("sites","name","WHERE cs_gatekeeper_hostname='"+hostname+"'").strip()
        self.sitename = sitename
        if sitename not in self.sitenames():
            msg = "GridCatService @ %s\n" % serverURL
            msg += "Does not know about site: %s\n" % sitename
            raise RuntimeError, msg

    def batchSystem(self):
        """
        _batchJobmanager_

        Default batch execution job manager 
        """
        #contact = self.gatekeeper()
        #contact += "/jobmanager-"
        #sitename= self._Server.getresult("sites","name","WHERE cs_gatekeeper_hostname='"+self.hostname+"'").strip()
        #    "site_info", "jobcon", self.sitename).strip()
        #return contact 
        #print "sitename ",sitename
        
        batchsystem=self._Server.getsiteresult("site_info", "jobcon", self.sitename).strip()
        return batchsystem

class GridCatSiteService(GridCatService):
    """
    _GridCatSiteService_

    Specialisation of GridCatService that deals only with a single
    site, provided as a sitename on instantiation
    
    """
    def __init__(self, serverURL, sitename):
        GridCatService.__init__(self, serverURL)
        self.sitename = sitename
        if sitename not in self.sitenames():
            msg = "GridCatService @ %s\n" % serverURL
            msg += "Does not know about site: %s\n" % sitename
            raise RuntimeError, msg
        
        
    def jobmanagers(self):
        """
        _jobmanagers_

        return a list of jobmanagers for this site
        """
        jobmgrs = self._Server.jobmanagers(self.sitename)
        jobmgrs = jobmgrs.split(",")
        result = []
        for item in jobmgrs:
            result.append(item.strip())
        return result
    
    def gatekeeper(self):
        """
        _gatekeeper_

        return the gatekeeper hostname for the site
        """
        return self._Server.getsiteresult(
            'sites', 'cs_gatekeeper_hostname' , self.sitename).strip()
    

    #def batchSystem(self):
    #    """
    #    _batchJobmanager_

    #    Default batch execution job manager 
    #    """
        #contact = self.gatekeeper()
        #contact += "/jobmanager-"
        #contact += self._Server.getsiteresult(
        #    "site_info", "jobcon", self.sitename).strip()
        #return contact 
    #    self.sitename=
    #    return

    def batchJobmanager(self):
        """
        _batchJobmanager_

        Default batch execution job manager 
        """
        contact = self.gatekeeper()
        contact += "/jobmanager-"
        contact += self._Server.getsiteresult(
            "site_info", "jobcon", self.sitename).strip()
        return contact 

    def utilJobmanager(self):
        """
        _utilJobmanager_

        Default util jobmanager
        
        """
        contact = self.gatekeeper()
        contact += "/jobmanager-"
        contact += self._Server.getsiteresult(
            "site_info", "utilcon", self.sitename).strip()
        return contact 
    
    def sitestatus(self):
        """
        _sitestatus_

        return the site status
        """
        return self._Server.sitestatus(self.sitename)

    def authstatus(self):
        """
        _authstatus_
        """
        return self._Server.teststatus("auth", self.sitename)

    def directories(self):
        """
        _directories_

        return a dictionary of directory paths for the site
        """
        result = {
            "$app" : None,
            "$data" : None,
            "$tmp": None,
            }
        result["$app"] = self._Server.getsiteresult('site_info', 'appdir',
                                                    self.sitename).strip()
        result["$data"] = self._Server.getsiteresult('site_info', 'datadir',
                                                     self.sitename).strip()
        result["$tmp"] = self._Server.getsiteresult('site_info', 'tmpdir',
                                                    self.sitename).strip()

        return result
    

if __name__ == '__main__':

    print GridCatHostService(sys.argv[1],sys.argv[2]).batchSystem()
