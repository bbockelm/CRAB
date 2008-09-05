from Actor import *
import common
import string, os, time
from crab_util import *

class GetOutput(Actor):
    def __init__(self, *args):
        self.cfg_params = args[0]
        self.jobs = args[1]
        
        self.log=0
        self.outDir = self.cfg_params.get('USER.outputdir' ,common.work_space.resDir())
        if ( self.outDir[-1] != '/' ) : self.outDir = self.outDir + '/'
        self.logDir = self.cfg_params.get('USER.logdir' ,common.work_space.resDir())
        if ( self.logDir[-1] != '/' ) : self.logDir = self.logDir + '/'
        if self.logDir != self.outDir:
            self.log=1
        self.return_data = self.cfg_params.get('USER.return_data',0)

        self.possible_status = {
                         'UN': 'Unknown', 
                         'SU': 'Submitted',
                         'SW': 'Waiting',
                         'SS': 'Scheduled',
                         'R': 'Running',
                         'SD': 'Done',
                         'K': 'Killed',
                         'SA': 'Aborted',
                         'SE': 'Cleared',
                         'E': 'Cleared'
                          }
        return

    def run(self):
        """
        The main method of the class: Check destination dirs and 
        perform the get output
        """
        common.logger.debug(5, "GetOutput::run() called")

        start = time.time()
        self.getOutput()
        stop = time.time()
        common.logger.debug(1, "GetOutput Time: "+str(stop - start))
        common.logger.write("GetOutput Time: "+str(stop - start))
        pass

    def checkBeforeGet(self):
        # should be in this way... but a core dump appear... waiting for solution  
        #self.up_task = common.scheduler.queryEverything(1) 
        self.up_task = common._db.getTask() 
        list_id_done=[]
        self.list_id=[]
        self.all_id =[]    
        for job in self.up_task.jobs:
            if job.runningJob['status'] in ['SD','DA']:
                list_id_done.append(job['jobId'])  
            self.all_id.append(job['jobId'])  
        check = -1 
        if self.jobs != 'all': check = len( set(self.jobs).intersection(set(list_id_done)) )  
        if len(list_id_done)==0 or ( check == 0 ) :
            msg=''  
            list_jobs=self.jobs 
            if self.jobs == 'all': list_jobs=self.all_id 
            msg += 'Jobs %s are not in Done status. It is not possible yet to retrieve the output.'% readableList(self,list_jobs)
            raise CrabException(msg)
        else:
            if self.jobs == 'all': 
                self.list_id= list_id_done
                if len(self.up_task.jobs)>len(self.list_id): 
                    msg = '\nOnly %d jobs will be retrieved '% (len(self.list_id))
                    msg += ' from %d requested.\n'%(len(self.up_task.jobs))
                    msg += ' (for details: crab -status)' 
                    common.logger.message(msg)
            else:
                for id in self.jobs:
                    if id in list_id_done: self.list_id.append(id)   
                if len(self.jobs) > len(self.list_id):
                    msg = '\nOnly %d jobs will be retrieved '% (len(self.list_id))
                    msg += ' from %d requested.\n'%(len(self.jobs))
                    msg += ' (for details: crab -status)' 
                    common.logger.message(msg)
        if not os.path.isdir(self.logDir) or not os.path.isdir(self.outDir):
            msg =  ' Output or Log dir not found!! check '+self.logDir+' and '+self.outDir
            raise CrabException(msg)
        #else:
        #    submission_id = common._db.queryRunJob('submission',self.list_id)
        #    submission_id.sort()
        #    submission_id.reverse()
        #    max_id=submission_id[0]
        #    if max_id > 1: self.moveOutput(max_id)

        return 

    def getOutput(self):
        """
        Get output for a finished job with id.
        """
        self.checkBeforeGet()
        common.scheduler.getOutput(1,self.list_id,self.outDir)
        self.organizeOutput()    
        return
  
    def organizeOutput(self): 
        """
        Untar Output  
        """
        listCode = []
        job_id = []

        #cwd = os.getcwd()
        #os.chdir( self.outDir )
        success_ret = 0
        for id in self.list_id:
            file = 'out_files_'+ str(id)+'.tgz'
            if os.path.exists(self.outDir + file):
                self.submission_id = common._db.queryRunJob('submission',id)
                self.max_id=max(self.submission_id)
                if self.max_id > 1:
                    for f in os.listdir(self.outDir):
                        if (f.find(str(id)) != -1 ) and (f != file) and f.find('Submission_'+str(id)) == -1: 
                            self.moveOutput(id, self.max_id, self.outDir, f)
                    if self.log==1:
                        for f in os.listdir(self.logDir):
                            if f.find(str(id)) != -1 and f.find('Submission_'+str(id)) == -1:
                                self.moveOutput(id, self.max_id, self.logDir, f)
                cmd = 'tar zxvf ' + self.outDir + file + ' ' + '-C ' + self.outDir  
                cmd_out = runCommand(cmd)
                cmd_2 ='rm ' + self.outDir + 'out_files_'+ str(id)+'.tgz'
                #cmd_2 ='rm out_files_'+ str(id)+'.tgz'
                cmd_out2 = runCommand(cmd_2)
                msg = 'Results of Jobs # '+str(id)+' are in '+self.outDir
                common.logger.message(msg) 
            else:  
                msg ="Output files for job "+ str(id) +" not available.\n"
                common.logger.debug(1,msg)
                continue   
            input = 'crab_fjr_' + str(id) + '.xml'
            if os.path.exists(self.outDir + input):
                codeValue = self.parseFinalReport(self.outDir + input)
                job_id.append(id)
                listCode.append(codeValue)
            else:
                msg = "Problems with "+str(input)+". File not available.\n"
                common.logger.message(msg) 
            success_ret +=1 
        #os.chdir( cwd )
        common._db.updateRunJob_(job_id , listCode)

        if self.logDir != self.outDir:
            for i_id in self.list_id:  
                try:
                    cmd = 'mv '+str(self.outDir)+'/*'+str(i_id)+'.std* '+str(self.outDir)+'/.BrokerInfo '+str(self.logDir)
                    cmd_out =os.system(cmd)
                except:
                    msg = 'Problem with copy of job results'
                    common.logger.message(msg)
            msg = 'Results of Jobs # '+str(self.list_id)+' are in '+self.outDir+' (log files are in '+self.logDir+')'
            common.logger.message(msg)
        return

    def parseFinalReport(self, input):
        """
        Parses the FJR produced by job in order to retrieve 
        the WrapperExitCode and ExeExitCode.
        Updates the BossDB with these values.

        """
        from ProdCommon.FwkJobRep.ReportParser import readJobReport
        
        #input = self.outDir + '/crab_fjr_' + str(jobid) + '.xml'  
        codeValue = {} 

        jreports = readJobReport(input)
        if len(jreports) <= 0 :
            codeValue["applicationReturnCode"] = str(50115)
            codeValue["wrapperReturnCode"] = str(50115)
            common.logger.debug(5,"Empty FWkobreport: error code assigned is 50115 ")
            return codeValue

        jobReport = jreports[0]

        exit_status = ''
    
        ##### temporary fix for FJR incomplete ####
        fjr = open (input)
        len_fjr = len(fjr.readlines())
        if (len_fjr <= 6):
            ### 50115 - cmsRun did not produce a valid/readable job report at runtime
            codeValue["applicationReturnCode"] = str(50115)
            codeValue["wrapperReturnCode"] = str(50115)
       
        if len(jobReport.errors) != 0 :
            for error in jobReport.errors:
                if error['Type'] == 'WrapperExitCode':
                    codeValue["wrapperReturnCode"] = error['ExitStatus']
                elif error['Type'] == 'ExeExitCode':     
                    codeValue["applicationReturnCode"] = error['ExitStatus']
                else:
                    continue

        if not codeValue.has_key('wrapperReturnCode'):
            codeValue["wrapperReturnCode"] = ''
        if not codeValue.has_key('applicationReturnCode'):
            codeValue["applicationReturnCode"] = ''
            
        return codeValue

    def moveOutput(self,id, max_id,path,file):
        """
        Move output of job already retrieved
        into the correct backup directory
        """
        Dir_Base=path +'Submission_'
        
        for i in range(1, max_id):
            if not os.path.isdir( Dir_Base + str(i) + '/'):
                cmd=('mkdir '+ Dir_Base + str(i) + '/  >& /dev/null')
                cmd_out = runCommand(cmd)   
                common.logger.write(str(cmd_out))
                common.logger.debug(3,str(cmd_out))
        cmd='mv '+ path + file + ' ' + Dir_Base + str(max_id -1) + '/  >& /dev/null' 
        
        try:
            cmd_out = runCommand(cmd) 
            common.logger.write(cmd_out)
            common.logger.debug(3,cmd_out)
        except:
            msg = 'no output to move for job '+str(id)
            common.logger.write(msg)
            common.logger.debug(3,msg)
            pass
        return
