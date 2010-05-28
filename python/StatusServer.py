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

class StatusServer(Status):

    def __init__(self, *args):

        Status.__init__(self, *args) 
 
        # init client server params...
        CliServerParams(self)       

        return

    def query(self,display=True):

        warning_msg = self.resynchClientSide()
        
        upTask = common._db.getTask()  
        self.compute(upTask,display)

        if warning_msg is not None:
            common.logger.info(warning_msg)

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
        useFallback = False
        warning_msg = None
 
        try:
            reportXML = zlib.decompress( handledXML.decode('base64') )
        except Exception, e:
            warning_msg = "WARNING: Problem while decompressing fresh status from the server. Use HTTP fallback"
            common.logger.debug(warning_msg)
            common.logger.debug( traceback.format_exc() )
            useFallback = True 

        try:
            if useFallback:
                import urllib
                xmlStatusURL  = 'http://%s:8888/visualog/'%self.server_name
                xmlStatusURL += '?taskname=%s&logtype=Xmlstatus'%common._db.queryTask('name')
                common.logger.debug("Accessing URL for status fallback: %s"%xmlStatusURL)
                reportXML = ''.join(urllib.urlopen(xmlStatusURL).readlines())
        except Exception, e:
            warning_msg = "WARNING: Unable to retrieve status from server. Please issue crab -status again"
            common.logger.debug(warning_msg)
            common.logger.debug( str(e) )
            common.logger.debug( traceback.format_exc() )
            return warning_msg

        try:
            xmlStatus = minidom.parseString(reportXML)
            reportList = xmlStatus.getElementsByTagName('Job')
            common._db.deserXmlStatus(reportList)
        except Exception, e:
            warning_msg = "WARNING: Unable to extract status from XML file. Please issue crab -status again"
            common.logger.debug(warning_msg)
            common.logger.debug("DUMP STATUS XML: %s"%s str(reportXML))
            common.logger.debug( str(e) )
            common.logger.debug( traceback.format_exc() )
            return warning_msg

        return warning_msg

    def showWebMon(self):
        msg  = 'You can also check jobs status at: http://%s:8888/logginfo\n'%self.server_name
        msg += '\t( Your task name is: %s )\n'%common._db.queryTask('name') 
        common.logger.debug(msg)
        #common.logger.info("Web status at: http://%s:8888/visualog/?taskname=%s&logtype=Status\n"%(self.server_name,self.task_unique_name))
