from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time

import traceback
from xml.dom import minidom
from ServerCommunicator import ServerCommunicator
from Status import Status
from ServerConfig import *

class StatusServer(Status):

    def __init__(self, *args):
        self.cfg_params = args[0]
        self.server_name = None
        self.server_port = None
        self.srvCfg = {}
        try:
            self.srvCfg = ServerConfig(self.cfg_params['CRAB.server_name']).config()

            self.server_name = str(self.srvCfg['serverName'])
            self.server_port = int(self.srvCfg['serverPort'])
        except KeyError:
            msg = 'No server selected or port specified.'
            msg = msg + 'Please specify a server in the crab cfg file'
            raise CrabException(msg)
 
        self.xml = self.cfg_params.get("USER.xml_report",'')
        return

    # all the behaviors are inherited from the direct status. Only some mimimal modifications
    # are needed in order to extract data from status XML and to align back DB information   
    # Fabio
  
    def query(self):
        common.scheduler.checkProxy()

        self.resynchClientSide()
        
        upTask = common._db.getTask()  
        self.compute(upTask)

    def resynchClientSide(self):
        """
        get status from the server and
        aling back data on client
        """ 
        task = common._db.getTask()
        # proxy management
        self.proxy = None # common._db.queryTask('proxy')
        if 'X509_USER_PROXY' in os.environ:
            self.proxy = os.environ['X509_USER_PROXY']
        else:
            status, self.proxy = commands.getstatusoutput('ls /tmp/x509up_u`id -u`')
            self.proxy = proxy.strip()

        # communicator allocation
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params, self.proxy)
        reportXML = csCommunicator.getStatus( str(task['name']) )
        del csCommunicator

        # align back data and print
        try:
            reportList = minidom.parseString(reportXML.strip()).getElementsByTagName('Job')
            common._db.deserXmlStatus(reportList)
        except Exception, e:
            print "WARNING: Problem while retrieving fresh status from the server."
            return
        return 

