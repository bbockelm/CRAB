from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import urllib
import os, time

class ServerConfig:
    def __init__(self, serverName):
        import string
        serverName = string.lower(serverName)
        common.logger.debug(5,'Calling ServerConfig')
#        self.url = 'http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/useful_script/'

        self.url ='https://cmsweb.cern.ch/crabconf/files/'
        if 'default' in  serverName:
            common.logger.debug(5,'getting serverlist from web')
            # get a list of available servers 
            serverListFileName ='AvalableServerList'
            serverListFile = self.getConfig_(serverListFileName)
            #f = urllib.urlopen('http://www.pd.infn.it/~lacaprar/Computing/'+serverListFileName)
            # parse the localCfg file
            f = open(serverListFile, 'r')
            tmp = f.readlines()
            f.close()
            if not tmp:
                msg = 'List of avalable Server '+serverListFileName+' from '+self.url+' is empty\n'
                msg += 'Please report to CRAB feedback hypernews'
                raise CrabException(msg)
            # clean up empty lines and "\n"
            serverList=[]
            [serverList.append(string.strip(it)) for it in tmp if (it.strip() and not it.strip()[0]=="#")]

            # if more than one, pick up a random one, waiting for something smarter (SiteDB)
            import random
            serverName = random.choice(serverList)
            common.logger.debug(5,'Avaialble servers: '+str(serverList)+' choosen: '+serverName)
            common.logger.write('Avaialble servers: '+str(serverList)+' choosen: '+serverName)
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
            common.logger.message('Downloading config files for '+url)
            self.downloadFile( url, configFileName)
        else:
            statinfo = os.stat(configFileName)
            ## if the file is older then 12 hours it is re-downloaded to update the configuration
            oldness = 12*3600
            if (time.time() - statinfo.st_ctime) > oldness:
                common.logger.message('Downloading config files for '+url)
                self.downloadFile( url, configFileName)
            pass
        return os.getcwd()+'/'+configFileName
