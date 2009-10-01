from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator
from Status import Status

from xml.parsers.expat import *

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
        self.task_unique_name = str(task['name'])

        # communicator allocation
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params)
        handledXML = csCommunicator.getStatus( self.task_unique_name )
        del csCommunicator

        # align back data and print
        reportXML = None
        try:
            handledXML += "="*( len(handledXML)%8 )  
            reportXML = zlib.decompress( base64.urlsafe_b64decode(handledXML) )
        except Exception, e:
            common.logger.info("WARNING: The status cache is out of date. Please issue crab -status again")
            common.logger.debug("WARNING: Problem while decompressing fresh status from the server.")
            common.logger.debug( str(e))
            common.logger.debug( traceback.format_exc() )
            return

        try:
            reportList = minidom.parseString(reportXML).getElementsByTagName('Job')
            common._db.deserXmlStatus(reportList)
        except ExpatError, experr:
            common.logger.info("WARNING: The status cache is out of date. Please issue crab -status again")
            common.logger.debug("ERROR: %s"%str(experr))
            common.logger.debug( str(experr))
            common.logger.debug( traceback.format_exc() )
        #    raise CrabException(str(experr))
        except TypeError, e:
            common.logger.info("WARNING: The status cache is out of date. Please issue crab -status again")
            common.logger.debug("WARNING: Problem while retrieving fresh status from the server.")
            common.logger.debug( str(e))
            common.logger.debug( traceback.format_exc() ) 
            return

        return 

    def showWebMon(self):
        msg  = 'You can also check jobs status at: http://%s:8888/logginfo\n'%self.server_name
        msg += '\t( Your task name is: %s )\n'%common._db.queryTask('name') 
        common.logger.debug(msg)
        #common.logger.info("Web status at: http://%s:8888/visualog/?taskname=%s&logtype=Status\n"%(self.server_name,self.task_unique_name))
