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


    def getCredential( self ):
        """
        """

        #self.credObj
 
        return
      
    def checkCredential( self, credential=None ):
        """
        """
        try: 
            self.credObj.checkCredential(credential)
        except Exception, ex:
            print str(ex)
        return

    def registerCredential( self, command=None ):
        """
        """

        self.credObj.registerCredential(command)
 
        return

    def getSubject(self, credential=None):
        """   
        """   
        sub = ''   
        try: 
            sub = self.credObj.getSubject(credential)
        except Exception, ex:
            print str(ex)
        return sub

    def getUserName(self, credential=None):
        """   
        """   
        uName = ''   
        try: 
            uName = self.credObj.getSubject(credential)
        except Exception, ex:
            print str(ex)
        return uName


