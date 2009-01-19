"""
Get output for server mode
"""

__revision__ = "$Id: GetOutputServer.py,v 1.36 2008/10/04 13:11:18 spiga Exp $"
__version__ = "$Revision: 1.36 $"

from GetOutput import GetOutput
from StatusServer import StatusServer
from crab_util import *
import common
import time

from ServerCommunicator import ServerCommunicator

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface

class GetOutputServer( GetOutput, StatusServer ):

    def __init__(self, *args):

        GetOutput.__init__(self, *args)

        # init client server params...
        CliServerParams(self)

        if self.storage_path[0] != '/':
            self.storage_path = '/'+self.storage_path

        return


    def getOutput(self):

        # get updated status from server #inherited from StatusServer
        self.resynchClientSide()

        # understand whether the required output are available
        self.checkBeforeGet()

        # retrive files
        filesAndJodId = { }

        filesAndJodId.update( self.retrieveFiles(self.list_id) )
        common.logger.debug(5, "Files to be organized and notified " + str(filesAndJodId))

        # load updated task       
        task = common._db.getTask()
     
        self.organizeOutput( task, self.list_id )

        self.notifyRetrievalToServer(filesAndJodId)
        return

    def retrieveFiles(self, filesToRetrieve):
        """
        Real get output from server storage
        """

        self.taskuuid = str(common._db.queryTask('name'))
        common.logger.debug(3, "Task name: " + self.taskuuid)

        # create the list with the actual filenames
        remotedir = os.path.join(self.storage_path, self.taskuuid)

        # list of file to retrieve
        osbTemplate = remotedir + '/out_files_%s.tgz'
        osbFiles = [osbTemplate % str(jid) for jid in filesToRetrieve]
        if self.cfg_params['CRAB.scheduler'].lower() in ["condor_g", "glidein"]:
            osbTemplate = remotedir + '/CMSSW_%s.stdout'
            osbFiles.extend([osbTemplate % str(jid) for jid in filesToRetrieve])
            osbTemplate = remotedir + '/CMSSW_%s.stderr'
            osbFiles.extend([osbTemplate % str(jid) for jid in filesToRetrieve])
        common.logger.debug(3, "List of OSB files: " +str(osbFiles) )

        copyHere = self.outDir
        destTemplate = copyHere+'/out_files_%s.tgz'
        destFiles = [ destTemplate % str(jid) for jid in filesToRetrieve ]
        if self.cfg_params['CRAB.scheduler'].lower() in ["condor_g","glidein"]:
            destTemplate = copyHere + '/CMSSW_%s.stdout'
            destFiles.extend([destTemplate % str(jid) for jid in filesToRetrieve])
            destTemplate = copyHere + '/CMSSW_%s.stderr'
            destFiles.extend([destTemplate % str(jid) for jid in filesToRetrieve])

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

        # retrieve them from SE
        filesAndJodId = {}
        sourceList = []
        destList = []
        for i in xrange(len(osbFiles)):
            source = osbFiles[i]
            dest = destFiles[i]
            common.logger.debug(3, "Adding file to transfer: "+ str(source) +" - "+ str(dest) )
            try:
                if self.storage_proto == "gridftp":
                    sourceList.append(source)
                    destList.append(dest)
                else:
                    sbi.copy( source, dest)
                if i < len(filesToRetrieve):
                    filesAndJodId[ filesToRetrieve[i] ] = dest
            except Exception, ex:
                msg = "WARNING: Unable to retrieve output file %s \n" % osbFiles[i]
                msg += str(ex)
                common.logger.debug(1,msg)
                continue

        if self.storage_proto == "gridftp":
            try:
                sbi.copy( sourceList, destList)
            except Exception, ex:
                msg = "WARNING: Unable to retrieve output file %s \n" % osbFiles[i]
                msg += str(ex)
                common.logger.message(msg)
                import traceback
                common.logger.debug( 1, str(traceback.format_exc()) )

        return filesAndJodId

    def notifyRetrievalToServer(self, fileAndJobList):
        retrievedFilesJodId = []

        for jid in fileAndJobList:
            if not os.path.exists(fileAndJobList[jid]):
                # it means the file has been untarred
                retrievedFilesJodId.append(jid)

        common.logger.debug(5, "List of retrieved files notified to server: %s"%str(retrievedFilesJodId) )

        # notify to the server that output have been retrieved successfully. proxy from StatusServer
        if len(retrievedFilesJodId) > 0:
            csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
            try:
                csCommunicator.outputRetrieved(self.taskuuid, retrievedFilesJodId)
            except Exception, e:
                pass
        return

