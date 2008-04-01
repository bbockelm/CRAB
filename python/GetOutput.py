from Actor import *
import common
import string, os, time
from crab_util import *

class GetOutput(Actor):
    def __init__(self, *args):
        self.cfg_params = args[0]
        self.jobs = args[1]
        
        self.outDir = self.cfg_params.get("USER.outputdir", common.work_space.resDir() )
        self.logDir = self.cfg_params.get("USER.logdir", common.work_space.resDir() )
        self.return_data = self.cfg_params.get('USER.return_data',0)

        self.possible_status = [
                         'Undefined', 
                         'Submitted',
                         'Waiting',
                         'Ready', 
                         'Scheduled',
                         'Running',
                         'Done',
                         'Cancelled',
                         'Aborted',
                         'Unknown',
                         'Done(failed)'
                         'Cleared'
                          ]
        return

    def run(self):
        """
        The main method of the class: compute the status and print a report
        """
        common.logger.debug(5, "Status::run() called")

        start = time.time()
        self.getOutput()
        # self.parse.fjr 
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
            if job.runningJob['statusScheduler']=='Done':
                list_id_done.append(job['id'])  
            self.all_id.append(job['id'])  
        check = -1 
        if self.jobs != 'all': check = len( set(self.jobs).intersect(set(list_id_done)) )  
        if len(list_id_done)==0 or ( check == 0 ) :
            #common.logger.message(msg)
            msg='\n'    
            list_ID=[] 
            for st in self.possible_status:
                list_ID = common._db.queryAttrRunJob({'statusScheduler':st},'jobId')
                if len(list_ID)>0: msg += "       %i Jobs  %s \n" % (len(list_ID), str(st))
            msg += '\n*******No jobs in Done status. It is not possible yet to retrieve the output.\n'
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
                if len(self.jobs) > len(list_id):
                    msg = '\nOnly %d jobs will be retrieved '% (len(self.list_id))
                    msg += ' from %d requested.\n'%(len(self.jobs))
                    msg += ' (for details: crab -status)' 
                    common.logger.message(msg)
        if not os.path.isdir(self.logDir) or not os.path.isdir(self.outDir):
            msg =  ' Output or Log dir not found!! check '+self.logDir+' and '+self.outDir
            raise CrabException(msg)
        ##TODO
   #     else:
        ## here check the resubmission number and backup existing files calling moveOutput()    

        return 

    def getOutput(self):
        """
        Get output for a finished job with id.
        Returns the name of directory with results.

        """
       # self.checkBeforeGet()

       # common.scheduler.getOutput(1,self.list_id,self.outDir) ## NeW BL--DS
        ## TO DO 
        ### here the logic must be improved...
        ### 1) enable the getoutput check

        self.list_id=[3]
        cwd = os.getcwd()
        os.chdir( self.outDir )
        for id in self.list_id:
            cmd = 'tar zxvf out_files_'+ str(id)+'.tgz' 
            cmd_out = runCommand(cmd)
            cmd_2 ='rm out_files_'+ str(id)+'.tgz'
            cmd_out2 = runCommand(cmd_2)
        os.chdir( cwd )

        if self.logDir != self.outDir:
            for i_id in self.list_id:  
                try:
                    cmd = 'mv '+str(self.outDir)+'/*'+str(i_id)+'.std* '+str(self.outDir)+'/.BrokerInfo '+str(self.outDir)+'/*.log '+str(self.logDir)
                    cmd_out =os.system(cmd)
                except:
                    msg = 'Problem with copy of job results'
                    common.logger.message(msg)
            msg = 'Results of Jobs # '+str(self.list_id)+' are in '+self.outDir+' (log files are in '+self.logDir+')'
            common.logger.message(msg)
        else:
            msg = 'Results of Jobs # '+str(self.list_id)+' are in '+self.outDir
            common.logger.message(msg)
        return


  #  def moveOutput(self):
  #      """
  #      Move output of job already retrieved
  #      """
  #      
  #      listFile 
  #          if os.exists(self.logDir+'/'+i):
  #              shutil.move(self.logDir+'/'+i, resDirSave+'/'+i+'_'+self.current_time)
  #              common.logger.message('Output file '+i+' moved to '+resDirSave)
  #      return
