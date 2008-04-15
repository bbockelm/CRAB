from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator
from ServerConfig import *


class KillerServer(Actor):
    def __init__(self, cfg_params, range):
        self.cfg_params = cfg_params
        self.range = range

        self.server_name = None
        self.server_port = None
        self.srvCfg = {}
        try:
            self.srvCfg = ServerConfig(self.cfg_params['CRAB.server_name']).config()
            self.server_name = str(self.srvCfg['serverName'])
            self.server_port = int(self.srvCfg['serverPort'])
        except KeyError:
            msg = 'No server selected or port specified.'
            msg = msg + 'Please specify a server in the crab cfg file'
            raise CrabException(msg)

        return

    def run(self):
        """
        The main method of the class: kill a complete task
        """
        common.logger.debug(5, "Killer::run() called")

        ## get subject ##
        self.proxy = None # TODO From task object alreadyFrom task object already  ? common._db.queryTask('proxy')
        if 'X509_USER_PROXY' in os.environ:
            self.proxy = os.environ['X509_USER_PROXY']
        else:
            status, self.proxy = commands.getstatusoutput('ls /tmp/x509up_u`id -u`')
            self.proxy = self.proxy.strip()

        ## register proxy ##
        common.scheduler.checkProxy()
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params, self.proxy)

        taskuuid = str(common._db.queryTask('name'))
        ret = csCommunicator.killJobs( taskuuid, self.range)
        del csCommunicator

        if ret != 0:
            msg = "ClientServer ERROR: %d raised during the communication.\n"%ret
            raise CrabException(msg)

        # update runningjobs status
        updList = [{'statusScheduler':'Killed', 'status':'K'}] * len(self.range)
        common._db.updateRunJob_(self.range, updList)
        return
                
