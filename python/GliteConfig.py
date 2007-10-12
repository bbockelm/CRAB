from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import urllib
import os, time

class GliteConfig:
    def __init__(self, RB):
        common.logger.debug(5,'Calling GliteConfig')
        self.url = 'http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/useful_script/'
        self.configFileName = 'glite.conf.CMS_'+str(RB)
        self.theConfig = self.getConfig_()
        pass
        
    def config(self):
        return self.theConfig

    def downloadFile(self, url, destination):
        try:
            f = urllib.urlopen(url)
            ff = open(self.configFileName, 'w')
            ff.write(f.read())
            ff.close()
        except IOError:
            # print 'Cannot access URL: '+url
            raise CrabException('Cannot download config file '+self.configFileName+' from '+self.url)

    def getConfig_(self):
        if not os.path.exists(self.configFileName):
            url = self.url+self.configFileName
            common.logger.message('Downloading config files for WMS: '+url)
            self.downloadFile( url, self.configFileName)
        else:
            statinfo = os.stat(self.configFileName)
            ## if the file is older then 1 day it is re-downloaded to update the configuration
            oldness = 3600*24
            if (time.time() - statinfo.st_ctime) > oldness:
                url = self.url+self.configFileName
                common.logger.message('Downloading config files for WMS: '+url)
                self.downloadFile( url, self.configFileName)
            pass
        return os.getcwd()+'/'+self.configFileName
