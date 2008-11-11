from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator
from Status import Status

import zlib
import base64

class StatusServer(Status):

    def __init__(self, *args):

        Status.__init__(self, *args) 
 
        # init client server params...
        CliServerParams(self)       

        return

    def query(self,display=True):

        self.resynchClientSide()
        
        upTask = common._db.getTask()  
        self.compute(upTask,display)

    def resynchClientSide(self):
        """
        get status from the server and
        aling back data on client
        """ 
        task = common._db.getTask()

        # communicator allocation
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
        handledXML = csCommunicator.getStatus( str(task['name']) )
        del csCommunicator

        # align back data and print
        try:
            handledXML += "="*( len(handledXML)%8 )  
            reportXML = zlib.decompress( base64.urlsafe_b64decode(handledXML) )
        except Exception, e:
            common.logger.debug(1,"WARNING: Problem while decompressing fresh status from the server.")
            common.logger.debug(1, str(e))
            common.logger.debug(1, traceback.format_exc() )
            return

        try:
            reportList = minidom.parseString(reportXML).getElementsByTagName('Job')
            common._db.deserXmlStatus(reportList)
        except Exception, e:
            common.logger.debug(1,"WARNING: Problem while retrieving fresh status from the server.")
            common.logger.debug(1, str(e))
            common.logger.debug(1, traceback.format_exc() ) 
            return

        return 

