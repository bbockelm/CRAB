from crab_exceptions import *
from crab_util import *
import common

import urllib
class MyUrlOpener(urllib.FancyURLopener):
    def http_error_default(*args, **kwargs):
        return urllib.URLopener.http_error_default(*args, **kwargs)

import os, time

class GliteConfig:
    def __init__(self, RB):
        common.logger.debug('Calling GliteConfig with WMS= '+str(RB))
        self.url ='http://cmsdoc.cern.ch/cms/LCG/crab/config/'
      #  self.url ='https://cmsweb.cern.ch/crabconf/'
        self.configFileName = 'glite_wms_'+str(RB)+'.conf'
        self.theConfig = self.getConfig_()
        pass
        
    def config(self):
        return self.theConfig

    def downloadFile(self, url, destination):
        try:
            urllib._urlopener = MyUrlOpener()
            f = urllib.urlopen(url)
            ff = open(destination, 'w')
            ff.write(f.read())
            ff.close()
        except IOError, msg:
            raise CrabException('Cannot download config file '+destination+' from '+self.url+'\n'+str(msg))

    def getConfig_(self):
        if not os.path.exists(self.configFileName):
            url = self.url+self.configFileName
            common.logger.info('Downloading config files for WMS: '+url)
            self.downloadFile( url, self.configFileName)
        else:
            statinfo = os.stat(self.configFileName)
            ## if the file is older then 12 hours it is re-downloaded to update the configuration
            oldness = 12*3600
            if (time.time() - statinfo.st_ctime) > oldness:
                url = self.url+self.configFileName
                common.logger.info('Downloading config files for WMS: '+url)
                try:
                   self.downloadFile( url, self.configFileName)
                except CrabException:
                   common.logger.info('Error downloading config files for WMS: %s . Keep using the local one.'%url) 
                   pass
            pass
        return os.getcwd()+'/'+self.configFileName
