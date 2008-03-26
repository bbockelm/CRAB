from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import urllib
import os, time

class ServerConfig:
    def __init__(self, serverName):
        common.logger.debug(5,'Calling ServerConfig')
        self.url = 'http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/useful_script/Server_conf/'
        self.configFileName = 'server_%s.conf'%string.lower(serverName)
        localCfg = self.getConfig_()

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
            ff = open(destination, 'w')
            ff.write(f.read())
            ff.close()
        except IOError:
            raise CrabException('Cannot download config file '+destination+' from '+self.url)

    def getConfig_(self):
        if not os.path.exists(self.configFileName):
            url = self.url+self.configFileName
            common.logger.message('Downloading config files for '+url)
            self.downloadFile( url, self.configFileName)
        else:
            statinfo = os.stat(self.configFileName)
            ## if the file is older then 12 hours it is re-downloaded to update the configuration
            oldness = 12*3600
            if (time.time() - statinfo.st_ctime) > oldness:
                url = self.url+self.configFileName
                common.logger.message('Downloading config files for '+url)
                self.downloadFile( url, self.configFileName)
            pass
        return os.getcwd()+'/'+self.configFileName



