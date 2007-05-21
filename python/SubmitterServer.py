from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
import time
from ProgressBar import ProgressBar
from TerminalController import TerminalController


import commands
from TaskDB import TaskDB

class SubmitterServer(Actor):
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
        The main method of the class: submit jobs in range self.nj_list
        """
        common.logger.debug(5, "SubmitterServer::run() called")

        totalCreatedJobs= 0
        start = time.time()
        flagSubmit = 1
        common.jobDB.load()
        for nj in range(common.jobDB.nJobs()):
            if (common.jobDB.status(nj)=='C') or (common.jobDB.status(nj)=='RC'):
                totalCreatedJobs +=1
            else:
                flagSubmit = 0

        if not flagSubmit:
            if totalCreatedJobs > 0:
                common.logger.message("Not all jobs are created: before submit create all of them")
                return
            else:
                common.logger.message("Impossible to sumbit jobs that are already submitted")
                return
        elif (totalCreatedJobs==0):
            common.logger.message("No jobs to be submitted: first create them")
            return

        # submit pre DashBoard information
        params = {'jobId':'TaskMeta'}
               
        fl = open(common.work_space.shareDir() + '/' + self.cfg_params['apmon'].fName, 'r')
        for i in fl.readlines():
            val = i.split(':')
            params[val[0]] = string.strip(val[1])
            fl.close()

        common.logger.debug(5,'Submission DashBoard Pre-Submission report: '+str(params))
                        
        self.cfg_params['apmon'].sendToML(params)

        ### Here start the server submission 
        pSubj = os.popen3('openssl x509 -in $X509_USER_PROXY  -subject -noout')[1].readlines()[0]
       
        userSubj='userSubj'
        userSubjFile = open(common.work_space.shareDir()+'/'+userSubj,'w')
        userSubjFile.write(str(pSubj))   
        userSubjFile.close()   
    
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])

        try: 
            #cmd = 'asap-user-register --server crabdev1.cern.ch --verbose'
            common.logger.message("Registering a valid proxy to the server\n")
            cmd = 'asap-user-register --server '+str(self.server_name).split("/")[0] 
            ex = os.system(cmd)
            #ex = os.system(cmd+' >/dev/null')
            if (ex>0): raise CrabException("ASAP ERROR: Unable to ship a valid proxy to the server "+str(self.server_name).split("/")[0]+"\n")
        except:  
            msg = "ASAP ERROR: Unable to ship a valid proxy to the server \n"
            msg +="Project "+str(WorkDirName)+" not Submitted \n"      
            raise CrabException(msg)

        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict('TasKUUID')
        common.taskDB.load()
        common.taskDB.setDict('projectName',projectUniqName)
        common.taskDB.save()

        ### create a tar ball
        common.logger.debug( 5, 'tar -zcvf '+str(WorkDirName)+'.tgz '+str(WorkDirName) )
        cmd = 'tar -zcvf '+str(WorkDirName)+'.tgz '+str(WorkDirName)
        cmd_out = runCommand(cmd)
    
        try: 
            ### submit poject to the server   
            #projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict('TasKUUID')     
            common.logger.message("Sending the project to the server...\n")
            cmd = 'lcg-cp --vo cms file://'+os.getcwd()+'/'+str(WorkDirName)+'.tgz gsiftp://' + str(self.server_name) + str(projectUniqName)+'.tgz'
            shipProject = os.system(cmd +' >& /dev/null')
            common.logger.debug( 5, 'rm -f '+str(WorkDirName)+'.tgz' )
            cmd = 'rm -f '+str(WorkDirName)+'.tgz'
            cmd_out = runCommand(cmd)
            if (shipProject>0):
                raise CrabException("ERROR : Unable to ship the project to the server \n "+str(self.server_name).split("/")[0]+"\n")
            else:
                msg='Project '+str(WorkDirName)+' succesfuly submitted to the server \n'      
                common.logger.message(msg)
                for nj in range(common.jobDB.nJobs()):
                    common.jobDB.setStatus(nj, 'S')
                    common.jobDB.save()
        except Exception, ex:  
            print str(ex)
            cmd = 'rm -f '+str(WorkDirName)+'.tgz'
            cmd_out = runCommand(cmd)
            msg = "ERROR : Unable to ship the project to the server \n"
            msg +="Project "+str(WorkDirName)+" not Submitted \n"      
            raise CrabException(msg)

        return
                
                
                
                
                
                
                
             
                
                
                
                
                
                
                
                
                
                
