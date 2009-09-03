from Actor import *
import common
import string, os, time, sys, glob
from crab_util import *
import traceback
#from ServerCommunicator import ServerCommunicator

class CacheCleaner(Actor):
    def __init__(self):
       """
       A class to clean:
       - SiteDB cache
       - CrabServer cache
       - WMS cache
       """
       self.string_app = common.work_space.topDir()[0:len(common.work_space.topDir())-1]
       self.index = string.rfind(self.string_app, "/")
       self.submitting_dir = self.string_app[0:self.index] 
       self.username = gethnUserNameFromSiteDB()
       self.flag = 0
       return

    def run(self):
        common.logger.debug("CacheCleaner::run() called")
        try:
            #SiteDB cache
            if os.path.exists(self.submitting_dir+'/SiteDBusername.conf'):
               cmd = 'rm '+self.submitting_dir+'/SiteDBusername.conf'
               cmd_out = runCommand(cmd) 
               self.flag = 1 
            else:
               common.logger.debug(self.submitting_dir+'/SiteDBusername.conf'+' not found') 

            if os.path.exists(self.submitting_dir+'/.cms_sitedbcache'):
               cmd = 'rm -rf '+self.submitting_dir+'/.cms_sitedbcache'
               cmd_out = runCommand(cmd)
               self.flag = 1
            else:
               common.logger.debug(self.submitting_dir+'/.cms_sitedbcache'+' not found')

            if os.path.exists('/tmp/jsonparser_'+self.username):
               cmd = 'rm -rf /tmp/jsonparser_'+self.username 
               cmd_out = runCommand(cmd)
               self.flag = 1          
            else:
               common.logger.debug('/tmp/jsonparser_'+self.username+' not found')            

            #CrabServer cache and WMS cache
            if len(glob.glob(os.path.join(self.submitting_dir,'*.conf'))) > 0:
               cmd = 'rm -rf '+self.submitting_dir+'/*.conf'
               cmd_out = runCommand(cmd)
               self.flag = 1 
            else:
               common.logger.debug(self.submitting_dir,'*.conf'+' not found')  
            
            if self.flag == 1:
               common.logger.info("Cache cleaned!")
            else:
               common.logger.info("Cache already cleaned")

        except Exception, e:
            common.logger.debug("WARNING: Problem cleaning the cache.")
            common.logger.debug( str(e))
            common.logger.debug( traceback.format_exc() )
            return
