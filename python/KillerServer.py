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

        task = common._db.getTask(self.range)
        toBeKilled = []
        for job  in task.jobs:
           if job.runningJob['status'] in ['SS','R','SR','SW', 'SU']:
               toBeKilled.append(job['jobId'])
           else:
               common.logger.message("Not possible to kill Job #"+str(job['jobId'])+" : Status is "+str(job.runningJob['statusScheduler']))
           pass

        if len(toBeKilled)>0:
            ## register proxy ##
            csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
 
            taskuuid = str(common._db.queryTask('name'))
            ret = csCommunicator.killJobs( taskuuid, toBeKilled)
            del csCommunicator
 
            if ret != 0:
                msg = "ClientServer ERROR: %d raised during the communication.\n"%ret
                raise CrabException(msg)
 
            # update runningjobs status
            updList = [{'statusScheduler':'Killing', 'status':'KK'}] * len(toBeKilled)
            common._db.updateRunJob_(toBeKilled, updList)
 
            # printout the command result
            common.logger.message("Kill request for %d jobs succesfully sent to the server\n"%len(toBeKilled) ) 

        return
                
