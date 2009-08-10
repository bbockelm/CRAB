from Actor import *
from crab_exceptions import *
import common
import string
from ServerCommunicator import ServerCommunicator
from StatusServer import StatusServer

class CleanerServer(Cleaner):

    def __init__(self, cfg_params):
        """
        constructor
        """
        Cleaner.__init__(self, cfg_params)
        self.cfg_params = cfg_params

        # init client server params...
        CliServerParams(self)
        return

    def run(self):
        ############## Temporary trick (till the right version get tested) ####
        msg=''  
        msg+='functionality not yet available for the server. Work in progres \n' 
        msg+='only local worling directory will be removed'
        #msg+='planned for CRAB_2_5_0'
        common.logger.info(msg) 
        common.work_space.delete()
        print 'directory '+common.work_space.topDir()+' removed'
        return

        ############## CliSer version ####################
        # get updated status from server
        try:
            stat = StatusServer(self.cfg_params)
            stat.resynchClientSide()
        except:
            pass
        
        # check whether the action is allowable
        Cleaner.check()

        # notify the server to clean the task 
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
        taskuuid = str(common._db.queryTask('name'))

        try:
            csCommunicator.cleanTask(taskuuid)
        except Exception, e:
            msg = "Client Server comunication failed about cleanJobs: task " + taskuuid
            common.logger.debug( msg)
            pass

        # remove local structures
        common.work_space.delete()
        print 'directory '+common.work_space.topDir()+' removed'
        return

