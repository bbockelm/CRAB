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

class CmdMgrComponent:
    def __init__(self, **args):
        self.args = {}
        self.args['Port'] = 2181
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
        self.stdoutFile = str(self.args['ComponentDir'])+'/FrontendLog'
        self.port = self.args['Port']
       
        logging.info('-------------------------------------------------------') 
        logging.info('Starting the component driver.')
        pass

    def startComponent(self, testMode=False):
        ## Check if the environment is properly set
        if 'PYTHONHOME' not in os.environ:
            stat, out = commands.getstatusoutput('which python')
            out = out[:out.rfind('/')] # remove /python
            out = out[:out.rfind('/')] # remove /bin
            os.environ['PYTHONHOME'] = out
            self.cmd += 'export PYTHONHOME=%s && '%out
            logging.info('PYTHONHOME set')

        logging.debug('PYTHONHOME=%s'%os.environ['PYTHONHOME'])

        ## add the source folder to the pypath
        cdir = os.path.expandvars(os.environ['CRAB_SERVER_ROOT']+'/lib/CommandManager')
        logging.debug('cdir=%s'%cdir)

        if cdir not in os.environ['PYTHONPATH']:
            os.environ['PYTHONPATH'] += cdir
            self.cmd += 'export PYTHONPATH=$PYTHONPATH:%s && '%cdir
            logging.info('PYTHONPATH set')

        ## prepare the Frontend
        self.cmd += cdir + '/CRAB-AS-CmdMgr-Frontend %s 2>&1 >> %s'%(self.port, self.stdoutFile)
        logging.info(self.cmd)

        if testMode==True:
            print self.cmd
            return

        stat, out = commands.getstatusoutput(self.cmd)
        logging.info('Frontend outcome: %s %s'%(stat, out))
        #
        return 

    def __call__(self, messageType, payload):
        return



if __name__ == "__main__" :
    args = {'Port':2181, 'ComponentDir':os.getcwd()}  
    print "Test mode: %s"%str(args)
    c = CmdMgrComponent(**dict(args)) 
    c.startComponent(testMode=True)
