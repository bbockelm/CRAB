import sys
import commands
import traceback
import time

class CredentialAPI:
    def __init__( self, args ):

        self.credential = args.get( "credential", '')
        self.pInfos = {}
 
        try:
            module =  __import__(
                self.credential, globals(), locals(), [self.credential]
                )
            credClass = vars(module)[self.credential]
            self.credObj = credClass( **args )
        except KeyError, e:
            msg = 'Credential interface' + self.credential + 'not found'
            raise msg, str(e)
        except Exception, e:
            raise e.__class__.__name__, str(e)


    def registerCredential( self, serverName ):
        """
        """

        self.credObj.registerCredential(serverName)
 
        return
 
