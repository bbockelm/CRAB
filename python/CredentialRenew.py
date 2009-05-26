from Actor import *
from crab_util import *
import common
import traceback
from ProdCommon.Credential.CredentialAPI import CredentialAPI
from SubmitterServer import SubmitterServer


class CredentialRenew(Actor):

    def __init__(self, cfg_params):
        self.cfg_params=cfg_params
        self.credentialType = 'Proxy'
        if common.scheduler.name().upper() in ['LSF', 'CAF']:
            self.credentialType = 'Token'
         
        # init client server params...
        CliServerParams(self)       

    def run(self):
        """
        """
        common.logger.debug("CredentialRenew::run() called")
        ## TEMPORARY FIXME  
        if self.credentialType == 'Proxy':
            subServer = SubmitterServer(self.cfg_params, None, "all")
            subServer.moveProxy()
        else: 
            self.renewer()    
        common.logger.info("Credential successfully delegated to the server.\n")
        return

    def renewer(self):
        """
        """
        myproxyserver = self.cfg_params.get('EDG.proxy_server', 'myproxy.cern.ch')
        configAPI = {'credential' : self.credentialType, \
                     'myProxySvr' : myproxyserver,\
                     'serverDN'   : self.server_dn,\
                     'shareDir'   : common.work_space.shareDir() ,\
                     'userName'   : getUserName(),\
                     'serverName' : self.server_name \
                     }
        try:
            CredAPI =  CredentialAPI( configAPI )            
        except Exception, err : 
            common.logger.debug( "Configuring Credential API: " +str(traceback.format_exc()))
            raise CrabException("ERROR: Unable to configure Credential Client API  %s\n"%str(err))
        if not CredAPI.checkCredential(Time=100) :
           common.logger.info("Please renew your %s :\n"%self.credentialType)
           try:
               CredAPI.ManualRenewCredential()
           except Exception, ex:
               raise CrabException(str(ex))
        try:
            dict = CredAPI.registerCredential() 
        except Exception, err:
            common.logger.debug( "Registering Credentials : " +str(traceback.format_exc()))
            raise CrabException("ERROR: Unable to register %s delegating server: %s\n"%(self.credentialType,self.server_name ))
