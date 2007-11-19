from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
import time
from ProgressBar import ProgressBar
from TerminalController import TerminalController

import xml.dom.minidom

class GetOutputServer(Actor):
 
    def __init__(self, cfg_params,):
        self.cfg_params = cfg_params
        try:  
            self.server_name = self.cfg_params['CRAB.server_name'] # gsiftp://pcpg01.cern.ch/data/SEDir/
        except KeyError:
            msg = 'No server selected ...' 
            msg = msg + 'Please specify a server in the crab cfg file' 
            raise CrabException(msg) 
        return
    
    def run(self):
        """
        The main method of the class: retrieve the output from server
        """
        common.logger.debug(5, "GetOutput server::run() called")

        start = time.time()

        common.scheduler.checkProxy()

        common.taskDB.load()
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])
        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict("TasKUUID")     
        #Need to add a check on the treashold level 
        # and on the task readness  TODO  
        try:
            ### retrieving project from the server
            common.logger.message("Retrieving the project from the server...\n")

            copyHere = common.work_space.resDir() # MATT
            if "USER.outputdir" in self.cfg_params.keys() and os.path.isdir(self.cfg_params["USER.outputdir"]):
                  copyHere = self.cfg_params["USER.outputdir"] + "/" # MATT
             
            cmd = 'lcg-cp --vo cms --verbose gsiftp://' + str(self.server_name) + str(projectUniqName)+'/res/done.tar.gz file://'+copyHere+'done.tar.gz'# MATT
            common.logger.debug(5, cmd)
            copyOut = os.system(cmd +' >& /dev/null')
        except:
            msg = ("Output not yet available")
            raise CrabException(msg)

        zipOut = "done.tar.gz"
        if os.path.exists( copyHere + zipOut ): 
            cwd = os.getcwd()
            os.chdir( copyHere )
            common.logger.debug( 5, 'tar -zxvf ' + zipOut )
  	    cmd = 'tar -zxvf ' + zipOut 
            cmd += '; mv .tmpDone/* .; rm -drf .tmpDone/'
	    cmd_out = runCommand(cmd)
	    os.chdir(cwd)
            common.logger.debug( 5, 'rm -f '+copyHere+zipOut )
	    cmd = 'rm -f '+copyHere+zipOut
	    cmd_out = runCommand(cmd)

            try:
                # file = open(common.work_space.resDir()+"xmlReportFile.xml", "r")
                doc = xml.dom.minidom.parse(common.work_space.resDir()+ "xmlReportFile.xml" )

                task = doc.childNodes[0].childNodes[1].getAttribute("taskName")
                self.countToTjob = int(doc.childNodes[0].childNodes[1].getAttribute("totJob") )

                ended = doc.childNodes[0].childNodes[1].getAttribute("ended")

                addTree = 3
                if doc.childNodes[0].childNodes[3].getAttribute("id") != "all":
                    common.jobDB.load()
                    for job in range( self.countToTjob ):
                        idJob = doc.childNodes[0].childNodes[job+addTree].getAttribute("id")
                        status = doc.childNodes[0].childNodes[job+addTree].getAttribute("status")
                        cleared = doc.childNodes[0].childNodes[job+addTree].getAttribute("cleared")
                        if int(cleared) == 1 and (status == "Done" or status  == "Done (Failed)"):
                            common.jobDB.setStatus( str(int(idJob)-1), "Y" )
                        addTree += 1
                    common.jobDB.save()
            except Exception, ex:
                msg = ("Problems accessing the report file: " + str(ex))
                raise CrabException(msg)

            common.logger.message('Task Completed at '+str(ended)+' %\n')
	    msg='Data for project '+str(WorkDirName)+' successfully retrieved from server \n'      
	    msg+='and copied in '+copyHere+' \n'
            common.logger.message(msg)
        else:
            common.logger.message("Problems have been encoutered during project transfer. Please check with [status] option if jobs have finished .\n")

        return

