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

    # all the behaviors are inherited from the direct status. Only some mimimal modifications
    # are needed in order to extract data from status XML and to align back DB information   
    # Fabio
  
    def compute(self):
        # proxy management
        proxy = None # common._db.queryTask('proxy')
        if 'X509_USER_PROXY' in os.environ:
            proxy = os.environ['X509_USER_PROXY']
        else:
            status, proxy = commands.getstatusoutput('ls /tmp/x509up_u`id -u`')
            proxy = proxy.strip()
        common.scheduler.checkProxy()

        task = common._db.getTask()

        # communicator allocation
        common.logger.message("Checking the status of all jobs: please wait")
        csCommunicator = ServerCommunicator(self.server_name, self.server_port, self.cfg_params, proxy)
        reportXML = csCommunicator.getStatus( str(task['name']) )
        del csCommunicator

        # align back data and print
        reportList = minidom.parseString(reportXML).getElementsByTagName('Job')
        toPrint=[]
        for job in task.jobs:
            id = str(job.runningJob['id'])
            # TODO linear search, probably it can be optized with binary search
            rForJ = None
            for r in reportList:
                if id == r.getAttribute('id'):
                    rForJ = r
                    break

            # Data alignment  
            job.runningJob['statusScheduler'] = str( rForJ.getAttribute('status') )
            jobStatus = str(job.runningJob['statusScheduler'])
            
            job.runningJob['destination'] = str( rForJ.getAttribute('site') )            
            dest = str(job.runningJob['destination']).split(':')[0]

            job.runningJob['applicationReturnCode'] = str( rForJ.getAttribute('exe_exit') )
            exe_exit_code = str(job.runningJob['applicationReturnCode'])

            job.runningJob['wrapperReturnCode'] = str( rForJ.getAttribute('job_exit') )
            job_exit_code = str(job.runningJob['wrapperReturnCode'])
              
            job['submissionNumber'] =  int(rForJ.getAttribute('resubmit'))
            # TODO cleared='0' field, how should it be handled/mapped in BL? #Fabio
            common.bossSession.updateDB( task )

            printline=''
            if dest == 'None' :  dest = ''
            if exe_exit_code == 'None' :  exe_exit_code = ''
            if job_exit_code == 'None' :  job_exit_code = ''
            printline+="%-8s %-18s %-40s %-13s %-15s" % (id,jobStatus,dest,exe_exit_code,job_exit_code)
            toPrint.append(printline)

        self.detailedReport(toPrint)
        pass




