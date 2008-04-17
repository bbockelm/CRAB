from GetOutput import GetOutput
from StatusServer import StatusServer
from crab_util import *
import common
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator
from ServerConfig import *

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

class GetOutputServer( GetOutput, StatusServer ):

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

        self.outDir = common.work_space.resDir()
        self.logDir = common.work_space.resDir()
        self.return_data = self.cfg_params.get('USER.return_data',0)

        self.possible_status = {
                         'UN': 'Unknown',
                         'SU': 'Submitted',
                         'SW': 'Waiting',
                         'SS': 'Scheduled',
                         'R': 'Running',
                         'SD': 'Done',
                         'SK': 'Killed',
                         'SA': 'Aborted',
                         'SE': 'Cleared',
                         'E': 'Cleared'
                         }
        return

    
    def getOutput(self): 

        # get updated status from server #inherited from StatusServer
        self.resynchClientSide()

        # understand whether the required output are available
        self.checkBeforeGet()

        # retrive files
        self.retrieveFiles(self.list_id) 

        self.organizeOutput()    

        return

    def retrieveFiles(self,filesToRetrieve):
        """
        Real get output from server storage
        """
        common.scheduler.checkProxy()
       
        self.proxyPath = getSubject(self)


        self.taskuuid = str(common._db.queryTask('name'))
        common.logger.debug(3, "Task name: " + self.taskuuid)

        # create the list with the actual filenames 
        remotedir = os.path.join(self.storage_path, self.taskuuid)

      
        # list of file to retrieve
        osbTemplate = remotedir + '/out_files_%s.tgz'  
        osbFiles = [ osbTemplate%str(jid) for jid in filesToRetrieve ]
        common.logger.debug(3, "List of OSB files: " +str(osbFiles) )
 
        #   
        copyHere = common.work_space.resDir()
        destTemplate = copyHere+'/out_files_%s.tgz'  
        destFiles = [ destTemplate%str(jid) for jid in filesToRetrieve ]

        common.logger.message("Starting retrieving output from server "+str(self.storage_name)+"...")

        try:  
            seEl = SElement(self.storage_name, self.storage_proto, self.storage_port)
        except Exception, ex:
            common.logger.debug(1, str(ex))
            msg = "ERROR : Unable to create SE source interface \n"
            raise CrabException(msg)
        try:  
            loc = SElement("localhost", "local")
        except Exception, ex:
            common.logger.debug(1, str(ex))
            msg = "ERROR : Unable to create destination  interface \n"
            raise CrabException(msg)

        ## copy ISB ##
        sbi = SBinterface( seEl, loc )

        # retrieve them from SE #TODO replace this with SE-API
        for i in xrange(len(filesToRetrieve)):
            source = osbFiles[i] 
            dest = destFiles[i]
            common.logger.debug(1, "retrieving "+ str(source) +" to "+ str(dest) )
            try:
                sbi.copy( source, dest, self.proxyPath)
            except Exception, ex:
                common.logger.debug(1, str(ex))
                msg = "WARNING: Unable to retrieve output file %s"%osbFiles[i] 
                common.logger.message(msg)
                continue

        # notify to the server that output have been retrieved successfully. proxy from StatusServer
        if len(filesToRetrieve) > 0:
            common.logger.debug(5, "List of retrieved files notified to server: %s"%str(filesToRetrieve) ) 
            csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params, self.proxyPath)
            csCommunicator.outputRetrieved(self.taskuuid, filesToRetrieve)

        return


