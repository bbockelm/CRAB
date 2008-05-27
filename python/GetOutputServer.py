from GetOutput import GetOutput
from StatusServer import StatusServer
from crab_util import *
import common
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

class GetOutputServer( GetOutput, StatusServer ):

    def __init__(self, *args):
 
        GetOutput.__init__(self,*args)

        # init client server params...
        CliServerParams(self)       

        if self.storage_path[0]!='/':
            self.storage_path = '/'+self.storage_path

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

        self.taskuuid = str(common._db.queryTask('name'))
        common.logger.debug(3, "Task name: " + self.taskuuid)

        # create the list with the actual filenames 
        remotedir = os.path.join(self.storage_path, self.taskuuid)

      
        # list of file to retrieve
        osbTemplate = remotedir + '/out_files_%s.tgz'  
        osbFiles = [ osbTemplate%str(jid) for jid in filesToRetrieve ]
        common.logger.debug(3, "List of OSB files: " +str(osbFiles) )
 
        #   
        copyHere = self.outDir 
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
        retrievedFilesJodId = []
        for i in xrange(len(filesToRetrieve)): 
            source = osbFiles[i] 
            dest = destFiles[i]
            common.logger.debug(1, "retrieving "+ str(source) +" to "+ str(dest) )
            try:
                sbi.copy( source, dest)
            except Exception, ex:
                msg = "WARNING: Unable to retrieve output file %s \n"%osbFiles[i] 
                msg += str(ex)
                common.logger.debug(1,msg)
                continue
            ## appending jobs id related to the index "i"
            retrievedFilesJodId.append(filesToRetrieve[i])

        # notify to the server that output have been retrieved successfully. proxy from StatusServer
        if len(retrievedFilesJodId) > 0:
            common.logger.debug(5, "List of retrieved files notified to server: %s"%str(retrievedFilesJodId) ) 
            csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
            try:
                csCommunicator.outputRetrieved(self.taskuuid, retrievedFilesJodId)
            except Exception, e:
                pass

            common._db.updateRunJob_(retrievedFilesJodId, [{'statusScheduler':'Cleared', 'status':'E'}] * len(retrievedFilesJodId) )
        return

