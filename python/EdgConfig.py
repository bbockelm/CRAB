from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import urllib
import os, time

class EdgConfig:
    def __init__(self, RB):
        common.logger.debug(5,'Calling EdgConfig')
        self.url = 'http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/useful_script/'
        self.configFileName = 'edg_wl_ui_cmd_var.conf.CMS_'+str.upper(RB)
        self.configVOFileName = 'edg_wl_ui.conf.CMS_'+str.upper(RB)
        self.theConfigVO = self.getConfigVO_()
        self.theConfig = self.getConfig_()
        pass
        
    def config(self):
        return self.theConfig

    def configVO(self):
        return self.theConfigVO

    def getConfig_(self):
        if not os.path.exists(self.configFileName):
            url = self.url+self.configFileName
            common.logger.message('Downloading config files for RB: '+url)
            try:
                f = urllib.urlopen(url)
                ff = open(self.configFileName, 'w')
                ff.write(f.read())
                ff.close()
            except IOError:
                # print 'Cannot access URL: '+url
                raise CrabException('Cannot download config file '+self.configFileName+' from '+self.url)
        return os.getcwd()+'/'+self.configFileName

    def getConfigVO_(self):
        if not os.path.exists(self.configVOFileName):
            url = self.url+self.configVOFileName
            common.logger.message('Downloading config files for RB: '+url)
            try:
                f = urllib.urlopen(url)
                ff = open(self.configVOFileName, 'w')
                ff.write(f.read())
                ff.close()
            except IOError:
                raise CrabException('Cannot download configVO file '+self.configVOFileName+' from '+self.url)
        return os.getcwd()+'/'+self.configVOFileName
