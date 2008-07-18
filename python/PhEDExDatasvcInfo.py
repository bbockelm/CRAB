#!/usr/bin/env python

import urllib
from xml.dom.minidom import parse
from crab_exceptions import *

class PhEDExDatasvcInfo:
  """
  provides information from PhEDEx Data Service 
  """
  ## PhEDEx Data Service URL
  #datasvc_url="https://cmsweb.cern.ch/phedex/test/datasvc/xml/prod"
  datasvc_url="https://cmsweb.cern.ch/phedex/datasvc/xml/prod"
   
  def lfn2pfn(self,node,lfn,protocol):
      """
      PhEDEx Data Service lfn2pfn call

      input:   LFN,node name,protocol
      returns: DOM object with the content of the PhEDEx Data Service call
      """  
      params = {'node' : node , 'lfn': lfn , 'protocol': protocol}
      params = urllib.urlencode(params)
      #print params
      datasvc_lfn2pfn="%s/lfn2pfn"%self.datasvc_url
      urlresults = urllib.urlopen(datasvc_lfn2pfn, params)
      try:
          urlresults = parse(urlresults)
      except:
          urlresults = None
      return urlresults

  def parse_error(self,urlresults):
      """
      look for errors in the DOM object returned by PhEDEx Data Service call
      """
      errormsg = None 
      errors=urlresults.getElementsByTagName('error')
      for error in errors:
          errormsg=error.childNodes[0].data
          if len(error.childNodes)>1:
             errormsg+=error.childNodes[1].data
      return errormsg

  def parse_lfn2pfn(self,urlresults):
      """
      Parse the content of the result of lfn2pfn PhEDEx Data Service  call

      input:    DOM object with the content of the lfn2pfn call
      returns:  PFN  
      """
      result = urlresults.getElementsByTagName('phedex')
      if not result:
            return []
      result = result[0]
      pfn = None
      mapping = result.getElementsByTagName('mapping')
      for m in mapping:
          pfn=m.getAttribute("pfn")
          if pfn:
            return pfn

  def getStageoutPFN(self,node,lfn,protocol):
      """
      input:   LFN,node name,protocol
      returns: PFN 
      """
      fullurl="%s/lfn2pfn?node=%s&lfn=%s&protocol=%s"%(self.datasvc_url,node,lfn,protocol) 
      domlfn2pfn = self.lfn2pfn(node,lfn,protocol)
      if not domlfn2pfn :
          msg="Unable to get info from %s"%fullurl
          raise CrabException(msg)

      errormsg = self.parse_error(domlfn2pfn)
      if errormsg: 
          msg="Error extracting info from %s due to: %s"%(fullurl,errormsg)
          raise CrabException(msg)

      stageoutpfn = self.parse_lfn2pfn(domlfn2pfn)
      if not stageoutpfn:
          msg="Unable to get stageout path (PFN) from %s"%fullurl
          raise CrabException(msg)
      return stageoutpfn 


if __name__ == '__main__' :
    """
    """
    from crab_logger import Logger
    from WorkSpace import *
    continue_dir="/home/fanfani/CRAB"
    cfg_params={'USER.logdir' : continue_dir }
    common.work_space = WorkSpace(continue_dir, cfg_params)
    log = Logger()
    common.logger = log

    from LFNBaseName import *
    # test values
    lfn = LFNBase("datasetstring") 
    node='T2_IT_Bari'
    protocol="srmv2"

    #create an instance of the PhEDExDatasvcInfo object
    dsvc = PhEDExDatasvcInfo()
    #extract the PFN for the given node,LFN,protocol
    print "Stageout to %s"%dsvc.getStageoutPFN(node,lfn,protocol)
 
