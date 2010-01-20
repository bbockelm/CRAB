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
       - Crab cache
       """
       return

    def run(self):
        common.logger.debug("CacheCleaner::run() called")
        try:
            sitedbCache= '%s/.cms_sitedbcache'%os.getenv('HOME')
            if os.path.isdir(sitedbCache):
               cmd = 'rm -f  %s/*'%sitedbCache
               cmd_out = runCommand(cmd)
               common.logger.info('%s Cleaned.'%sitedbCache)
            else:
               common.logger.info('%s not found'%sitedbCache)
        except Exception, e:
            common.logger.debug("WARNING: Problem cleaning the SiteDB cache.")
            common.logger.debug( str(e))
            common.logger.debug( traceback.format_exc() )

           # Crab  cache 
        try: 
            crabCache= '%s/.cms_crab'%os.getenv('HOME')
            if os.path.isdir(crabCache):
               cmd = 'rm -f  %s/*'%crabCache
               cmd_out = runCommand(cmd)
               common.logger.info('%s Cleaned.'%crabCache)
            else:
               common.logger.debug('%s not found'%crabCache)
        except Exception, e:
            common.logger.debug("WARNING: Problem cleaning the cache.")
            common.logger.debug( str(e))
            common.logger.debug( traceback.format_exc() )
        return
