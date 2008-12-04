import os,sys
import commands
import traceback
import time
import re
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

        cmd = 'voms-proxy-info -file '+proxy+' -timeleft 2>/dev/null'
 
        timeLeftLocal,  ret = self.ExecuteCommand(cmd)
       
        if ret != 0 and ret != 1:
            msg = "Error while checking proxy timeleft for %s"%proxy
            raise Exception(msg)
        
        ## if no valid proxy
        if not timeLeftLocal :
            valid = False
        elif int(timeLeftLocal)<minTimeLeft :
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

    def checkAttribute( self, proxy=None, vo='cms', group=None, role=None): 
        """
        """
        valid = True
        if proxy == None: proxy=self.getUserProxy()

        ## check first attribute
        cmd = 'export X509_USER_PROXY=%s; voms-proxy-info -fqan 2>/dev/null | head -1'%proxy

        reg="/%s/"%vo
        if group:
            reg+=group
        if role:
            reg+="/Role=%s"%role

        att, ret = self.ExecuteCommand(cmd)

        if ret != 0 :
            msg = "Error while checking proxy timeleft for %s"%proxy
            raise Exception(msg)
 
       ## you always have at least  /cms/Role=NULL/Capability=NULL
        if not re.compile(r"^"+reg).search(att):
            if self.debug: print "\tWrong VO group/role.\n"
            valid = False
        return valid 

    def ManualRenewCredential( self, proxy=None, vo='cms', group=None, role=None ):
        """
        """

        cmd = 'voms-proxy-init -voms %s'%vo

        if group:
            cmd += ':/'+vo+'/'+group
        if role:
            cmd += '/role='+role
        cmd += ' -valid 192:00'
        print cmd
        try:
            out = os.system(cmd)
            if (out>0): raise Exception("Unable to create a valid proxy!\n")
        except:
            msg = "Unable to create a valid proxy!\n"
            raise Exception(msg)

    def checkMyProxy( self , proxy=None, Time=4 ):
        """
        """
        if proxy == None: proxy=self.getUserProxy()
        ## check the myproxy server
        valid = True

        #cmd = 'export X509_USER_PROXY=%s; myproxy-info -d -s %s 2>/dev/null'%(proxy,self.myproxyServer)
        cmd = 'myproxy-info -d -s %s 2>/dev/null'%(self.myproxyServer)

        out, ret = self.ExecuteCommand(cmd)
        if ret != 0 and ret != 1 :
            msg = "Error while checking myproxy timeleft for %s"%proxy
            raise Exception(msg)

        if not out:
            if self.debug: print '\tNo credential delegated to myproxy server %s will do now'%self.myproxyServer
            valid = False
        else:
            ## minimum time: 5 days
            minTime = int(Time) * 24 * 3600
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
                        if self.debug: print '\tYour proxy will expire in:\n\t%s hours %s minutes %s seconds\n'%(hours,minutes,seconds)
                        valid = False
        return valid    

    def ManualRenewMyProxy( self ): 
        """
        """
        cmd = 'myproxy-init -d -n -s %s'%self.myproxyServer
        out = os.system(cmd)
        if (out>0):
            raise Exception("Unable to delegate the proxy to myproxyserver %s"%self.myproxyServer+" !\n")
        return
  
    def logonProxy( self ):
        """
        To be implemented
        """
        #
        return
