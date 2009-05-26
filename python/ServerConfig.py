from crab_exceptions import *
from crab_util import *
import common

import urllib
import os, time

class ServerConfig:
    def __init__(self, serverName):
        import string
        serverName = string.lower(serverName)
        common.logger.debug('Calling ServerConfig '+serverName)

        self.url ='https://cmsweb.cern.ch/crabconf/'
        # self.url ='http://www.pd.infn.it/~lacaprar/Computing/'
        if 'default' in  serverName:
            common.logger.debug('getting serverlist from web')
            # get a list of available servers 
            serverListFileName ='AvalableServerList'
            serverListFile = self.getConfig_(serverListFileName)
            # parse the localCfg file
            f = open(serverListFile, 'r')
            tmp = f.readlines()
            f.close()
            if not tmp:
                msg = 'List of avalable Server '+serverListFileName+' from '+self.url+' is empty\n'
                msg += 'Please report to CRAB feedback hypernews'
                raise CrabException(msg)
            # clean up empty lines and comments
            serverList=[]
            [serverList.append(string.split(string.strip(it))) for it in tmp if (it.strip() and not it.strip()[0]=="#")]
            common.logger.debug('All avaialble servers: '+str(serverList))

            # select servers from client version
            compatibleServerList=[]
            for s in serverList:
                vv=string.split(s[1],'-')
                if len(vv[0])==0: vv[0]='0.0.0'
                if len(vv[1])==0: vv[1]='99.99.99'
                for i in 0,1:
                    tmp=[]
                    [tmp.append(int(t)) for t in vv[i].split('.')]
                    vv[i]=tuple(tmp)
                #[vv.append(tuple(t.split('.'))) for t in string.split(s[1],'-')]

                common.prog_version
                
                #print vv[0],common.prog_version,vv[1]
                if vv[0]<=common.prog_version and common.prog_version<=vv[1]: compatibleServerList.append(s[0])
            common.logger.debug('All avaialble servers compatible with %s: '%common.prog_version_str +str(serverList))
            if len(compatibleServerList)==0: 
                msg = "No compatible server available with client version %s\n"%common.prog_version_str
                msg += "Exiting"
                raise CrabException(msg)
            # if more than one, pick up a random one, waiting for something smarter (SiteDB)
            import random
            serverName = random.choice(compatibleServerList)
            common.logger.debug('Avaialble servers: '+str(compatibleServerList)+' choosen: '+serverName)
        if 'server_' in serverName:
            configFileName = '%s.conf'%serverName
        else: 
            configFileName = 'server_%s.conf'%serverName

        localCfg = self.getConfig_(configFileName)

        # parse the localCfg file
        f = open(localCfg, 'r')
        l = ''.join( f.readlines() )
        f.close()
        
        if not l:
            l = str('{}') 
        self.theConfig = eval(l)
        self.theConfig['serverGenericName']=serverName
        pass
        
    def config(self):
        return self.theConfig

    def downloadFile(self, url, destination):
        try:
            f = urllib.urlopen(url)
            data = f.read()
            if '<!' in data[:2]:
                raise IOError

            ff = open(destination, 'w')
            ff.write(data)
            ff.close()
        except IOError:
            raise CrabException('Cannot download config file '+destination+' from '+self.url)

    def getConfig_(self, configFileName):
        url = self.url+configFileName
        if not os.path.exists(configFileName):
            common.logger.info('Downloading config files for '+url)
            self.downloadFile( url, configFileName)
        else:
            statinfo = os.stat(configFileName)
            ## if the file is older then 12 hours it is re-downloaded to update the configuration
            oldness = 12*3600
            if (time.time() - statinfo.st_ctime) > oldness:
                common.logger.info('Downloading config files for '+url)
                self.downloadFile( url, configFileName)
            pass
        return os.getcwd()+'/'+configFileName
