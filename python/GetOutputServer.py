from GetOutput import GetOutput
from StatusServer import StatusServer
from crab_util import *
import common
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator
from ServerConfig import *


class GetOutputServer(StatusServer, GetOutput):

    def __init__(self, *args):
        self.cfg_params = args[0]
        self.jobs = args[1]
 
        self.server_name = None
        self.server_port = None

        self.storage_name = None
        self.storage_path = None
        self.storage_proto = None
        self.storage_port = None

        self.srvCfg = {}

        try:
            self.srvCfg = ServerConfig(self.cfg_params['CRAB.server_name']).config()
            self.server_name = str(self.srvCfg['serverName'])
            self.server_port = int(self.srvCfg['serverPort'])
            self.storage_name = str(self.srvCfg['storageName'])
            self.storage_path = str(self.srvCfg['storagePath'])
            self.storage_proto = str(self.srvCfg['storageProtocol'])
            self.storage_port = str(self.srvCfg['storagePort'])
        except KeyError:
            msg = 'No server selected or port specified.'
            msg = msg + 'Please specify a server in the crab cfg file'
            raise CrabException(msg)

        if self.storage_path[0]!='/':
            self.storage_path = '/'+self.storage_path

        self.outDir = self.cfg_params.get("USER.outputdir", common.work_space.resDir() )
        self.logDir = self.cfg_params.get("USER.logdir", common.work_space.resDir() )
        self.return_data = self.cfg_params.get('USER.return_data',0)

        self.possible_status = [
                         'Undefined',
                         'Submitted',
                         'Waiting',
                         'Ready',
                         'Scheduled',
                         'Running',
                         'Done',
                         'Cancelled',
                         'Aborted',
                         'Unknown',
                         'Done(failed)'
                         'Cleared'
                          ]
        return

    def run(self):
        common.logger.debug(5, "GetOutput server::run() called")
        start = time.time()
        common.scheduler.checkProxy()

        # get updated status from server #inherited from StatusServer
        self.resynchClientSide()

        # understand whether the required output are available
        self.checkBeforeGet()
        filesToRetrieve = self.list_id

        # create the list with the actual filenames 
        taskuuid = str(common._db.queryTask('name'))
        remotedir = os.path.join(self.storage_path, taskuuid)

        osbTemplate = self.storage_proto + '://'+ self.storage_name +\
            ':' + self.storage_port + remotedir + '/out_%s.tgz'  
        osbFiles = [ osbTemplate%str(jid) for jid in filesToRetrieve ]

        copyHere = common.work_space.resDir() # MATT
        if "USER.outputdir" in self.cfg_params.keys() and os.path.isdir(self.cfg_params["USER.outputdir"]):
            copyHere = self.cfg_params["USER.outputdir"] + "/"
        destTemplate = 'file://'+copyHere+'/out_%s.tgz'  
        destFiles = [ destTemplate%str(jid) for jid in filesToRetrieve ]

        # retrieve them from SE #TODO replace this with SE-API
        for i in xrange(len(filesToRetrieve)):
            try:
                cmd = 'lcg-cp --vo cms %s %s'%(osbFiles[i], destFiles[i])
                out = os.system(cmd +' >& /dev/null')
                common.logger.debug(5, cmd)
                if out != 0:
                    print "Unable to retrieve output file %s"%osbFiles[i]
                    del filesToRetrieve[i]
                    continue

                ##    
                ## TODO check if sizes are ok/the transfer was safe. Simpler with the API
                ## 
 
                ## clean-up SE
                cmd = 'lcg-del --vo cms %s'%osbFiles[i]
                out = os.system(cmd +' >& /dev/null')
                common.logger.debug(5, cmd)
                if out != 0:
                    print "Unable to clean up SE for output file %s. The deletion will be performed by the server"%osbFiles[i]
                    continue

            except Exception, e:
                print "Unable to retrieve output file %s"%osbFiles[i] 
                del filesToRetrieve[i]
                continue

        # notify to the server that output have been retrieved successfully. proxy from StatusServer
        if len(filesToRetrieve) > 0:
            common.logger.debug(5, "List of retrieved files notified to server: %s"%str(filesToRetrieve) ) 
            csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params, self.proxy)
            csCommunicator.outputRetrieved(taskuuid, filesToRetrieve)

        return


