from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator


class KillerServer(Actor):
    def __init__(self, cfg_params, range):
        self.cfg_params = cfg_params
        self.range = range

        # init client server params...
        CliServerParams(self)       

        return

    def run(self):
        """
        The main method of the class: kill a complete task
        """
        common.logger.debug(5, "Killer::run() called")

        ## register proxy ##
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)

        taskuuid = str(common._db.queryTask('name'))
        ret = csCommunicator.killJobs( taskuuid, self.range)
        del csCommunicator

        if ret != 0:
            msg = "ClientServer ERROR: %d raised during the communication.\n"%ret
            raise CrabException(msg)

        # update runningjobs status
        updList = [{'statusScheduler':'Killed', 'status':'K'}] * len(self.range)
        common._db.updateRunJob_(self.range, updList)

        # printout the command result
        common.logger.message("Kill request succesfully sent to the server") 

        return
                
