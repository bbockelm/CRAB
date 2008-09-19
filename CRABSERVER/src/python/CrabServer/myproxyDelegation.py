import sys
import commands
import traceback
import time
import logging

class myProxyDelegationClientside:
    def __init__(self, myproxyServer, serverDN, vomsesPath):
        self.myproxyServer = myproxyServer
        self.serverDN = serverDN
        self.vomesPath = vomsesPath
        
        self.pInfos = {}
        pass

    def _getInfosFromProxy(self):
        cmd = 'voms-proxy-info -all -acissuer'
        retCode, output = commands.getstatusoutput(cmd)
        if not retCode == 0:
            raise Exception('Error while initializing MyProxy credential:\n %s\n %s'%(cmd, str(output)) )
            return retCode

        output = output.split('\n')
        self.pInfos.clear()
        self.pInfos['vomsPath'] = self.vomesPath        
        self.pInfos['acissuer'] = output[-1]

        for f in output:
            if ' : ' not in f:
                continue
            k, v = f.split(' : ')
            self.pInfos[k.strip()] = v.strip()
        return 0
    
    def _credentialExists(self):
        cmd = 'myproxy-info -s %s -l \'%s\' '
        cmd = cmd%(self.myproxyServer, self.pInfos['identity'])
        retCode, output = commands.getstatusoutput(cmd)
        if not retCode == 0:
            return False

        # consider the residual time allowed before myproxy renewal
        output = [ f for f in output.split('\n') if 'timeleft' in f ]
        residualHours = output[0].split(':')[1].strip()
        if int(residualHours) < 2:
            return False
        return True
    
    def delegate(self, delegT=192, credName='CrabServerProxy'):
        retCode =  self._getInfosFromProxy()
        if not retCode == 0:
            raise Exception('Unable to get information from proxy. Please try voms-proxy-init')
            return retCode

        # non chiaro, da fissare
        if self._credentialExists() == False:
            raise Exception('Too short proxy lifetime. Please renew it with myproxy-init')
            return -1

        # delegate proxy to myproxy            
        cmd = 'myproxy-init -s %s -c %d -x -n '%(self.myproxyServer, delegT)
        cmd += '-Z \'%s\' -l \'%s\' '%(self.serverDN, self.pInfos['identity'])
        cmd += '-k \'%s\' '%credName
        retCode, output = commands.getstatusoutput(cmd)

        if not retCode == 0:
            raise Exception('Error while initializing MyProxy credential:\n %s\n %s'%(cmd, str(output)) )
            return retCode
        return 0


class myProxyDelegationServerside:
    def __init__(self, userKeyPath, userKeyCert, myproxyServer):
        self.uKeyPath = userKeyPath
        self.uCertPath = userKeyCert
        self.myproxyServer = myproxyServer
        pass
    
    def getDelegatedProxy(self, destProxyPath, credName = 'CrabServerProxy', proxyArgs = {}):
        tmpProxyName = 'proxy_%d'%int( time.time() )

        if len(proxyArgs) == 0:
           raise Exception('No additional information specified. Proxy won\'t be retrieved')
           return -1

        acHostname = str(proxyArgs['acissuer']).split('/CN=')[1]

        # get delegated proxy
        cmd  = 'export X509_USER_KEY=%s && '%self.uKeyPath
        cmd += 'export X509_USER_CERT=%s && '%self.uCertPath
        cmd += 'myproxy-logon -s %s -n '%self.myproxyServer
        cmd += '-l \'%s\' -k %s -o %s'%(proxyArgs['identity'], credName, tmpProxyName)
        retCode, output = commands.getstatusoutput(cmd)
        if not retCode == 0:
            raise Exception('Error while getting MyProxy credential: %s'%cmd )
            return retCode

        # rebuild VOMS stuff, only if MyProxy does not support  
        if 'vomsCompliantMyProxy' not in proxyArgs:
            cmd  = 'voms-proxy-fake -cert %s -key %s '%(tmpProxyName, tmpProxyName)
            cmd += '-hostcert %s -hostkey %s '%(self.uCertPath, self.uKeyPath)
            cmd += '-voms %s -fqan \'%s\' '%(proxyArgs['VO'], proxyArgs['attribute'])
            cmd += '-uri %s:15002 -target %s '%(acHostname, acHostname)
            cmd += '-certdir %s -newformat -o %s'%(proxyArgs['vomsPath'], destProxyPath)
            retCode, output = commands.getstatusoutput(cmd)
            if not retCode == 0:
                raise Exception('Error while building VOMS extensions: %s\n %s'%(cmd, str(output)) )
                return retCode

        # clean up temp and renew the proxy
        cmd = 'rm -f %s'%tmpProxyName
        retCode, output = commands.getstatusoutput(cmd)
        self.renewDelegatedProxy(destProxyPath, proxyArgs['vomsPath'])
        return 0
    
    def renewDelegatedProxy(self, renewingProxyPath, vomsesDir):
        # NOTE1: omitted '-voms cms:/cms', preserve the proxy roles
        # NOTE2: as renewal is not a critical action (it can be performed later w/o problems)
        #       this method does not raises exception in case of failure
        cmd = 'voms-proxy-init -cert %s -key %s '%(renewingProxyPath, renewingProxyPath)
        cmd +='-vomses %s -hours 12:0 -vomslife 12:0 -out %s'%(vomsesDir, renewingProxyPath)
        retCode, output = commands.getstatusoutput(cmd)
        if not retCode == 0:
            print "WARNING: problem while renewing proxy: %d \n%s"%(retCode, output) 
            return retCode
        return 0

def main():
    mpSrv = 'myproxy-fts.cern.ch'
    cSrvDN = '/DC=ch/DC=cern/OU=computers/CN=crabdev1.cern.ch'
    vomsesPath = '/afs/cern.ch/project/gd/LCG-share/3.1.4-1/glite/etc/vomses'
    userKeyPath = '/home/crab/.globus/hostkey.pem'
    userKeyCert = '/home/crab/.globus/hostcert.pem'
    
    print "Delegation on client side"
    try:
        print 'Result Client = ', myProxyDelegationClientside(mpSrv, cSrvDN, vomsesPath).delegate()
        pass 
    except Exception, e:
        print str(e)
        print traceback.format_exc()

    print "Emulating server side activities"

    pinfos = {'strength': '512 bits', 'timeleft': '11:58:43', 'vomsPath': '/afs/cern.ch/project/gd/LCG-share/3.1.4-1/glite/etc/vomses', 'attribute': '/cms/Role=NULL/Capability=NULL', 'VO': 'cms', 'acissuer': '/DC=ch/DC=cern/OU=computers/CN=voms.cern.ch', 'path': '/tmp/x509up_u22115', 'subject': '/C=IT/O=INFN/OU=Personal Certificate/L=Fisica Milano Bicocca/CN=Fabio Farina', 'type': 'proxy', 'identity': '/C=IT/O=INFN/OU=Personal Certificate/L=Fisica Milano Bicocca/CN=Fabio Farina', 'issuer': '/DC=ch/DC=cern/OU=computers/CN=voms.cern.ch'}

    try:
        mpds = myProxyDelegationServerside(userKeyPath, userKeyCert, mpSrv)
        mpds.getDelegatedProxy('./testServerDeleg.proxy', proxyArgs=pinfos)
    except Exception, e:
        print str(e)
        print traceback.format_exc()
    return 0

if __name__ == "__main__":
     sys.exit(main())
