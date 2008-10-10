from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import urllib2
import os, time

class GliteConfig:
    def __init__(self, RB):
        common.logger.debug(5,'Calling GliteConfig')
       # self.url = 'http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/useful_script/'
        self.url ='https://cmsweb.cern.ch/crabconf/files/'
        self.configFileName = 'glite_wms_'+str(RB)+'.conf'
        self.theConfig = self.getConfig_()
        pass
        
    def config(self):
        return self.theConfig

    def downloadFile(self, url, destination):
        try:
            f = urllib2.urlopen(url)
            ff = open(destination, 'w')
            ff.write(f.read())
            ff.close()
        except urllib2.HTTPError:
            # print 'Cannot access URL: '+url
            raise CrabException('Cannot download config file '+destination+' from '+self.url)

    def getConfig_(self):
        if not os.path.exists(self.configFileName):
            url = self.url+self.configFileName
            common.logger.message('Downloading config files for WMS: '+url)
            self.downloadFile( url, self.configFileName)
        else:
            statinfo = os.stat(self.configFileName)
            ## if the file is older then 12 hours it is re-downloaded to update the configuration
            oldness = 12*3600
            if (time.time() - statinfo.st_ctime) > oldness:
                url = self.url+self.configFileName
                common.logger.message('Downloading config files for WMS: '+url)
                try:
                   self.downloadFile( url, self.configFileName)
                except CrabException:
                   common.logger.message('Error downloading config files for WMS: %s . Keep using the local one.'%url) 
                   pass
            pass
        return os.getcwd()+'/'+self.configFileName
