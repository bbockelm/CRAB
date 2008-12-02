import os,sys
import commands
import traceback
import time

from ProdCommon.BossLite.Common.System import executeCommand

class Proxy:
    """
    basic class to handle user Token  
    """
    def __init__( self, **args ):
        self.timeout = args.get( "timeout", None )
        self.myproxyServer = args.get( "myProxySvr", '')
        self.serverDN = args.get( "serverDN", '')
        self.shareDir = args.get( "shareDir", '')
        self.userName = args.get( "userName", '')
        self.debug = args.get("debug",False)
        self.args = args

    def ExecuteCommand( self, command ):
        """
        _ExecuteCommand_

        Util it execute the command provided in a popen object with a timeout
        """

        return executeCommand( command, self.timeout )


    def getUserProxy(self):
        """
        """
        try:
            proxy = os.path.expandvars('$X509_USER_PROXY')
        except Exception,ex:
            msg = ('Error %s in getUserProxy search\n' %str(ex))
            if self.debug : msg += traceback.format_exc()
            raise Exception(msg)

        return proxy.strip() 

    def getSubject(self, proxy = None):
        """
        """
        subject = None    
        if proxy == None: proxy=self.getUserProxy()

        cmd = 'openssl x509 -in '+proxy+' -subject -noout'

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 :
            msg = "Error while checking proxy subject for %s"%proxy
            raise Exception(msg)
        lines = out.split('\n')[0]
   
        return subject.strip()    
    
    def getUserName(self, proxy = None ):
        """
        """
        uName = None
        if proxy == None: proxy=self.getUserProxy()

        cmd = "voms-proxy-info -file "+proxy+" -subject"

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 :
            msg = "Error while extracting User Name from proxy %s"%proxy
            raise Exception(msg)

        emelments = out.split('/')
        uName = elements[-1:][0].split('CN=')[1]   

        return uName.strip()

    def checkCredential(self, proxy=None, Time=10):
        """
        Function to check the Globus proxy.
        """
        valid = True
        if proxy == None: proxy=self.getUserProxy()
        minTimeLeft=int(Time)*3600 # in seconds

        cmd = 'voms-proxy-info -file '+proxy+' -timeleft '
 
        out, ret

        timeLeftLocal = 

        ## if no valid proxy
        if timeLeftLocal == None or int(timeLeftLocal)<minTimeLeft :
            valid = False
      
        return valid 

    def renewCredential( self, proxy=None ): 
        """
        """
        if proxy == None: proxy=self.getUserProxy()
        # check 
        if not self.checkCredential():
            # ask for proxy delegation 
            # using myproxy
            pass
        return 

    def checkAttribute( self, proxy=None ): 
        """
        """
        if proxy == None: proxy=self.getUserProxy()

        ## check first attribute
      #  cmd = 'voms-proxy-info -fqan | head -1'

      #  reg="/%s/"%self.VO
      #  if self.group:
      #      reg+=self.group
      #  if self.role:
      #      reg+="/Role=%s"%self.role

        return 

    def ManualRenewCredential( self, VO='cms', group=None, role=None ):
        """
        """
   #     ## you always have at least  /cms/Role=NULL/Capability=NULL
   #     if not re.compile(r"^"+reg).search(att):
   #         if not mustRenew:
   #             common.logger.message( "Valid proxy found, but with wrong VO group/role.\n")
   #         mustRenew = 1
        ######

        if not self.checkCredential:
            cmd = 'voms-proxy-init -voms '+VO
            if group:
                cmd += ':/'+VO+'/'+group
            if role:
                cmd += '/role='+role
            cmd += ' -valid 192:00'
            try:
                out = os.system(cmd)
                if (out>0): raise Exception("Unable to create a valid proxy!\n")
            except:
                msg = "Unable to create a valid proxy!\n"
                raise Exception(msg)

    def checkMyProxy( self, proxyServer ):
        """
        """
        ## check the myproxy server
        valid = True
        cmd = 'myproxy-info -d -s %s'%proxyServer

        if not out:
            print 'No credential delegated to myproxy server %s will do now'%proxyServer
            valid = False
        else:
            ## minimum time: 5 days
            minTime = 4 * 24 * 3600
            ## regex to extract the right information
            myproxyRE = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)")
            for row in out.split("\n"):
                g = myproxyRE.search(row)
                if g:
                    hours = g.group("hours")
                    minutes = g.group("minutes")
                    seconds = g.group("seconds")
                    timeleft = int(hours)*3600 + int(minutes)*60 + int(seconds)
                    if timeleft < minTime:
                        print 'Your proxy will expire in:\n\t%s hours %s minutes %s seconds\n'%(hours,minutes,seconds)
                        valid = False
        return valid    

    def ManualRenewMyProxy( self ): 
        """
        """
        if not self.checkMyProxy:
            cmd = 'myproxy-init -d -n -s '+self.proxyServer
            out = os.system(cmd)
            if (out>0):
                raise CrabException("Unable to delegate the proxy to myproxyserver "+self.proxyServer+" !\n")
            pass
        return
  
    def logonProxy( self ):
        """
        To be implemented
        """
        #
        return
