#!/usr/bin/env python
"""
_StartComponent_

Start the component, reading its configuration from
the common configuration file, which is accessed by environment variable

"""

import os
import sys
import getopt
import time
import commands
from ProdAgentCore.Configuration import loadProdAgentConfiguration
import logging
from logging.handlers import RotatingFileHandler
import traceback

class CmdMgrComponent:
    def __init__(self, **args):
        self.args = {}
        self.args['Port'] = 20181
        self.args.update(args)

        # Logging system init
        if 'Logfile' not in self.args:
            self.args['Logfile'] = self.args['ComponentDir']+'/ComponentLog'

        logHandler = RotatingFileHandler(self.args['Logfile'], "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)

        self.cmd = ''
        self.WSlogFile = str(self.args['ComponentDir'])+'/FrontendLog'
        self.port = int(self.args['Port'])
       
        logging.info('Starting Crab Server Frontend component')
        pass

    def setEnvironment(self):
        ## Check if the environment is properly set
        if 'PYTHONHOME' not in os.environ:
            stat, out = commands.getstatusoutput('which python')
            out = out[:out.rfind('/')] # remove /python
            out = out[:out.rfind('/')] # remove /bin
            os.environ['PYTHONHOME'] = out
#            self.cmd += 'export PYTHONHOME=%s && '%out
            logging.info('PYTHONHOME set')

        ## add the source folder to the pypath
        cdir = os.path.expandvars(os.environ['CRABSERVER_ROOT']+'/lib/CommandManager')
        logging.debug('cdir=%s'%cdir)

        if cdir not in os.environ['PYTHONPATH']:
            os.environ['PYTHONPATH'] += cdir
#            self.cmd += 'export PYTHONPATH=$PYTHONPATH:%s && '%cdir
            logging.info('PYTHONPATH set')
        return
 

    def startComponent(self, testMode=False):
        self.setEnvironment()
        
        ## prepare the Frontend # old way
        # self.cmd += cdir + '/CRAB-CmdMgr-Frontend %s 2>&1 >> %s'%(self.port, self.stdoutFile)
        # logging.info(self.cmd)
        # stat, out = commands.getstatusoutput(self.cmd)
        # logging.info('Frontend outcome: %s %s'%(stat, out))
        ### 

        try:
            import FrontendLoader
        except Exception, e:
            logging.info('Error while importing Frontend module')
            logging.info( traceback.format_exc() )
 
        try_frontend_reboot = 8
        preTime = time.time() 

        while try_frontend_reboot:
            logging.info('Starting the Frontend WebService')
            outcode = 0
            try: 
                outcode = FrontendLoader.start(self.port, self.WSlogFile) 
            except Exception, e:
                logging.info('Frontend returned with code %d. Please inspect log file %s for further details'%(outcode, self.WSlogFile) )
                logging.info( traceback.format_exc() )
        #
        logging.info('Frontend failed too many times. Command Manager exits')  
        return 

    def __call__(self, messageType, payload):
        return


