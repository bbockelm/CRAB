from Actor import *
import urllib
from xml.dom.minidom import parse
from crab_exceptions import *
from crab_logger import Logger
from WorkSpace import *
from urlparse import urlparse 
from LFNBaseName import *

class PhEDExDatasvcInfo:
    def __init__( self , cfg_params ):
 
        ## PhEDEx Data Service URL
        url="https://cmsweb.cern.ch/phedex/datasvc/xml/prod"
        self.datasvc_url = cfg_params.get("USER.datasvc_url",url)

        self.FacOps_savannah = 'https://savannah.cern.ch/support/?func=additem&group=cmscompinfrasup'


        self.srm_version = cfg_params.get("USER.srm_version",'srmv2')
        self.node = cfg_params.get('USER.storage_element',None)
        
        self.publish_data = cfg_params.get("USER.publish_data",0)
        self.usenamespace = cfg_params.get("USER.usenamespace",0)
        self.user_remote_dir = cfg_params.get("USER.user_remote_dir",'')
        if self.user_remote_dir:
            if ( self.user_remote_dir[-1] != '/' ) : self.user_remote_dir = self.user_remote_dir + '/'
            
        self.datasetpath = cfg_params.get("CMSSW.datasetpath")
        self.publish_data_name = cfg_params.get('USER.publish_data_name','')

        self.user_lfn = cfg_params.get("USER.lfn",'')
        if self.user_lfn:
            if ( self.user_lfn[-1] != '/' ) : self.user_lfn = self.user_lfn + '/'
            
        self.user_port = cfg_params.get("USER.storage_port",'8443')
        self.user_se_path = cfg_params.get("USER.storage_path",'')
        if self.user_se_path:
            if ( self.user_se_path[-1] != '/' ) : self.user_se_path = self.user_se_path + '/'
                                                    
       
        #check if using "private" Storage
        self.usePhedex = True 
        stage_out_faq='https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrabFaq#How_to_store_output_with_CRAB_2'
        if not self.node :
            msg = 'Please specify the storage_element name in your crab.cfg section [USER].\n'
            msg +='      For further information please visit : %s'%stage_out_faq 
            raise CrabException(msg)
        if (self.node.find('T1_') + self.node.find('T2_')+self.node.find('T3_')) == -3: self.usePhedex = False 
        if not self.usePhedex and ( self.user_lfn == '' or self.user_se_path == '' ):
            msg = 'You are asking to stage out without using CMS Storage Name convention. In this case you \n' 
            msg += '      must specify both lfn and storage_path in the crab.cfg section [USER].\n '
            msg += '      For further information please visit : %s'%stage_out_faq
            raise CrabException(msg)
        self.sched = common.scheduler.name().upper() 
        self.protocol = self.srm_version
        if self.sched in ['CAF','LSF']:self.protocol = 'direct'

        return
 
    def getEndpoint(self):   
        '''
        Return full SE endpoint and related infos
        '''
        self.lfn = self.getLFN()
 
        #extract the PFN for the given node,LFN,protocol
        endpoint = self.getStageoutPFN()
   
        #extract SE name an SE_PATH (needed for publication)
        SE, SE_PATH, User = self.splitEndpoint(endpoint)

        return endpoint, self.lfn , SE, SE_PATH, User         
       
    def splitEndpoint(self, endpoint):
        '''
        Return relevant infos from endpoint  
        '''
        SE = ''
        SE_PATH = ''
        USER = ''
        if self.usePhedex: 
            if self.protocol == 'direct':
                query=endpoint
                SE_PATH = endpoint
                ### FEDE added SE ###
                SE = self.sched
            else: 
                url = 'http://'+endpoint.split('://')[1]
                # python > 2.4
                # SE = urlparse(url).hostname 
                scheme, host, path, params, query, fragment = urlparse(url)
                SE = host.split(':')[0]
                SE_PATH = endpoint.split(host)[1]
            USER = (query.split('user')[1]).split('/')[1]
        else:
            SE = self.node
            SE_PATH = self.user_se_path + self.user_lfn
            try:
                USER = (self.lfn.split('user')[1]).split('/')[1]
            except:
                pass

        return SE, SE_PATH, USER 
   

    def getLFN(self):
        """
        define the LFN composing the needed pieces
        """
        lfn = ''
        l_User = False
        if not self.usePhedex and (int(self.publish_data) == 0 and int(self.usenamespace) == 0) :
            ### add here check if user is trying to force a wrong LFN using a T2  TODO
            ## check if storage_name is a T2 (siteDB query)
            ## if yes :match self.user_lfn with LFNBaseName...
            ##     if NOT : raise (you are using a T2. It's not allowed stage out into self.user_path+self.user_lfn)   
            lfn = self.user_lfn
            return lfn
	if self.publish_data_name == '' and int(self.publish_data) == 1:
            msg = "Eeror. The [USER] section does not have 'publish_data_name'"
            raise CrabException(msg)
        if self.publish_data_name == '' and int(self.usenamespace) == 1:
           self.publish_data_name = "DefaultDataset"
        if int(self.publish_data) == 1 or int(self.usenamespace) == 1:
            if self.sched in ['CAF']: l_User=True 
            primaryDataset = self.computePrimaryDataset()
            lfn = LFNBase(primaryDataset,self.publish_data_name,LocalUser=l_User)  + '/${PSETHASH}/'    
        else:
            if self.sched in ['CAF','LSF']: l_User=True 
            lfn = LFNBase(self.user_remote_dir,LocalUser=l_User)
        return lfn
 
    def computePrimaryDataset(self):
        """
        compute the last part for the LFN in case of publication     
        """
        if (self.datasetpath.upper() != 'NONE'):
            primarydataset = self.datasetpath.split("/")[1]
        else:
            primarydataset = self.publish_data_name
        return primarydataset
    
    def lfn2pfn(self):
        """
        PhEDEx Data Service lfn2pfn call
 
        input:   LFN,node name,protocol
        returns: DOM object with the content of the PhEDEx Data Service call
        """  
        params = {'node' : self.node , 'lfn': self.lfn , 'protocol': self.protocol}
        params = urllib.urlencode(params)
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
 
    def getStageoutPFN( self ):
        """
        input:   LFN,node name,protocol
        returns: PFN 
        """
        if self.usePhedex:
            fullurl="%s/lfn2pfn?node=%s&lfn=%s&protocol=%s"%(self.datasvc_url,self.node,self.lfn,self.protocol) 
            domlfn2pfn = self.lfn2pfn()
            if not domlfn2pfn :
                msg="Unable to get info from %s"%fullurl
                raise CrabException(msg)
  
            errormsg = self.parse_error(domlfn2pfn)
            if errormsg: 
                msg="Error extracting info from %s due to: %s"%(fullurl,errormsg)
                raise CrabException(msg)
  
            stageoutpfn = self.parse_lfn2pfn(domlfn2pfn)
            if not stageoutpfn:
                msg ='Unable to get stageout path from TFC at Site %s \n'%self.node
                msg+='      Please alert the CompInfraSup group through their savannah %s \n'%self.FacOps_savannah
                msg+='      reporting: \n'
                msg+='       Summary: Unable to get user stageout from TFC at Site %s \n'%self.node
                msg+='       OriginalSubmission: stageout path is not retrieved from %s \n'%fullurl
                raise CrabException(msg)
        else:
            if self.sched in ['CAF','LSF'] :
                stageoutpfn = self.user_port+self.user_se_path+self.lfn 
            else: 
                stageoutpfn = 'srm://'+self.node+':'+self.user_port+self.user_se_path+self.lfn 

        return stageoutpfn 



if __name__ == '__main__':
  """
  Sort of unit testing to check Phedex API for whatever site and/or lfn.
  Usage:
     python PhEDExDatasvcInfo.py --node T2_IT_Bari --lfn /store/maremma

  """
  import getopt,sys
  from crab_util import *
  import common
  klass_name = 'SchedulerGlite'
  klass = importName(klass_name, klass_name)
  common.scheduler = klass()

  lfn="/store/user/"
  node='T2_IT_Bari'
  valid = ['node=','lfn=']
  try:
       opts, args = getopt.getopt(sys.argv[1:], "", valid)
  except getopt.GetoptError, ex:
       print str(ex)
       sys.exit(1)
  for o, a in opts:
        if o == "--node":
            node = a
        if o == "--lfn":
            lfn = a
  
  mycfg_params = { 'USER.storage_element': node }
  dsvc = PhEDExDatasvcInfo(mycfg_params)
  dsvc.lfn = lfn
  print dsvc.getStageoutPFN()

