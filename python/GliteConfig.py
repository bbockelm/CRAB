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
        print "dentro config ", self.theConfig
        pass
        
    def config(self):
        return self.theConfig

    def getConfig_(self):
        if not os.path.exists(self.configFileName):
            url = self.url+self.configFileName
            common.logger.message('Downloading config files for RB: '+url)
            try:
                f = urllib.urlopen(url)
                ff = open(common.work_space.shareDir() + '/' + self.configFileName, 'w')
                ff.write(f.read())
                ff.close()
            except IOError:
                # print 'Cannot access URL: '+url
                raise CrabException('Cannot download config file '+self.configFileName+' from '+self.url)
        return common.work_space.shareDir() + '/' + self.configFileName
