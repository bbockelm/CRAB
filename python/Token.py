import os,sys
import commands
import traceback
import time

from ProdCommon.BossLite.Common.System import executeCommand

class Token:
    """
    basic class to handle user Token  
    """
    def __init__( self, **args ):
        self.timeout = args.get( "timeout", None )
        self.myproxyServer = args.get( "myProxySvr", '')
        self.serverDN = args.get( "serverDN", '')
        self.shareDir = args.get( "shareDir", '')
        self.userName = args.get( "userName", '')

    def ExecuteCommand( self, command ):
        """
        _ExecuteCommand_

        Util it execute the command provided in a popen object with a timeout
        """

        return executeCommand( command, self.timeout )

        
    def registerCredential(self,serverName):
        """
        """
        token = self.getUserToken()
        self.delegate(serverName,token)
        return 
   
    def getUserToken(self):
        """
        """
        userToken = os.path.join(self.shareDir,self.userName) 

        cmd = '/afs/usr/local/etc/GetToken > ' + userToken

        out, ret =  self.ExecuteCommand(cmd)  
        if ret != 0 :
            msg = ('Error %s in getToken while executing : %s ' % (out, cmd)) 
            raise Exception(msg)
 
        return userToken

    def delegate(self,serverName,token):
        """
        """
        cmd = 'rfcp '+token+' '+serverName+':/data/proxyCache'         

        out, ret = self.ExecuteCommand(cmd)  
        if ret != 0 :
            msg = ('Error %s in getToken while executing : %s ' % (out, cmd)) 
            raise Exception(msg)
        return 
