from Actor import Actor
from WorkSpace import WorkSpace
from JobList import JobList
from ScriptWriter import ScriptWriter
from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import os, string, math
import time

class Creator(Actor):
    def __init__(self, job_type_name, cfg_params, ncjobs):
        self.job_type_name = job_type_name
        self.job_type = None
        self.cfg_params = cfg_params
        self.total_njobs = 0
        self.total_number_of_events = 0
        self.job_number_of_events = 0
        self.first_event = 0
        self.jobParamsList=[]
     
        self.createJobTypeObject(ncjobs)
        common.logger.debug(5, __name__+": JobType "+self.job_type.name()+" created")

        self.job_type.prepareSteeringCards()
        common.logger.debug(5, __name__+": Steering cards prepared")

        self.total_njobs = self.job_type.numberOfJobs();
        common.logger.debug(5, __name__+": total # of jobs = "+`self.total_njobs`)

        self.ncjobs = ncjobs
        if ncjobs == 'all' : self.ncjobs = self.total_njobs
        if ncjobs > self.total_njobs : self.ncjobs = self.total_njobs
        

        # This is code for dashboard
        #First checkProxy
        common.scheduler.checkProxy()
        try:
            gridName = string.strip(common.scheduler.userName())
            common.logger.debug(5, "GRIDNAME: "+gridName)
            taskType = 'analysis'
            VO = cfg_params.get('EDG.virtual_organization','cms')

            params = {'tool': common.prog_name,\
                      'JSToolVersion': common.prog_version_str, \
                      'tool_ui': os.environ['HOSTNAME'], \
                      'scheduler': common.scheduler.name(), \
                      'GridName': gridName, \
                      'taskType': taskType, \
                      'vo': VO, \
                      'user': os.environ['USER']}
            jtParam = self.job_type.getParams()
            for i in jtParam.iterkeys():
                params[i] = string.strip(jtParam[i])
            fl = open(common.work_space.shareDir() + '/' + common.apmon.fName, 'w')
            for j, k in params.iteritems():
                fl.write(j + ':' + k + '\n')
            fl.close()
        except:
            exctype, value = sys.exc_info()[:2]
            common.logger.message("Creator::run Exception raised in collection params for dashboard: %s %s"%(exctype, value))
            pass

        # Set/Save Job Type name
        self.job_type_name = common._db.queryTask("jobType") ## New BL--DS

        common.logger.debug(5, "Creator constructor finished")
        return

    def writeJobsSpecsToDB(self):
        """
        Write firstEvent and maxEvents in the DB for future use
        """

        self.job_type.split(self.jobParamsList)
        return

    def nJobs(self):
        return self.total_njobs
    
    def createJobTypeObject(self,ncjobs):
        file_name = 'cms_'+ string.lower(self.job_type_name)
        klass_name = string.capitalize(self.job_type_name)

        try:
            klass = importName(file_name, klass_name)
        except KeyError:
            msg = 'No `class '+klass_name+'` found in file `'+file_name+'.py`'
            raise CrabException(msg)
        except ImportError, e:
            msg = 'Cannot create job type '+self.job_type_name
            msg += ' (file: '+file_name+', class '+klass_name+'):\n'
            msg += str(e)
            raise CrabException(msg)

        self.job_type = klass(self.cfg_params,ncjobs)
        return

    def jobType(self):
        return self.job_type

    def run(self):
        """
        The main method of the class.
        """

        common.logger.debug(5, "Creator::run() called")
        start = time.time()
        # Instantiate ScriptWriter
        script_writer = None
        if self.cfg_params['CRAB.scheduler'].find("glit") != -1: ## checking scheduler: if glite(coll) output_sandbox will be limited
            script_writer = ScriptWriter('crab_template.sh', 1) ## flag that indicates if limit or not
        else:
            script_writer = ScriptWriter('crab_template.sh', 0)

        # Loop over jobs
        argsList = []
        njc = 0
#        block = -1 # first block is 0  BL--DS
#        lastDest=''
        for nj in range(self.total_njobs):
            if njc == self.ncjobs : break

            common.logger.debug(1,"Creating job # "+`(nj+1)`)

            # Prepare configuration file

            self.job_type.modifySteeringCards(nj)

            # Create XML script and declare related jobs
            argsList.append( str(nj+1)+' '+ self.job_type.getJobTypeArguments(nj, self.cfg_params['CRAB.scheduler']) )
            self.job_type.setArgsList(argsList)

            run_jobToSave = {'status' :'C'}
#            common._db.updateRunJob_(nj, run_jobToSave ) ## New BL--DS

            outputSandbox=self.job_type.outputSandbox(nj)
            # check if out!=err
## New BL--DS
#            common.jobDB.setOutputSandbox(nj, outputSandbox)
#            common.jobDB.setTaskId(nj, common.taskDB.dict('taskId'))

            job_ToSave={'outputFiles': outputSandbox}

            common._db.updateJob_(nj, job_ToSave ) ## Nes BL--DS

#            currDest=common.jobDB.destination(nj)
#            if (currDest!=lastDest):
#                block+=1
#                lastDest = currDest
#                pass
#            common.jobDB.setBlock(nj,block)
            njc = njc + 1
            pass

        # Create script (sh)
        script_writer.modifyTemplateScript()
        os.chmod(common._db.queryTask('scriptName'), 0744) ## Modified BL--DS
       # common: write input sandbox --- This is now a task attribute... not per job ## BL--DS
        isb = {'globalSandbox': self.job_type.inputSandbox(1)}
        common._db.updateTask_(isb) 
        ####
        common.scheduler.declare(self.total_njobs ) 

        common.scheduler.sched_parameter()

        common.logger.message('Creating '+str(self.total_njobs)+' jobs, please wait...')

        stop = time.time()
        common.logger.debug(2, "Creation Time: "+str(stop - start))
        common.logger.write("Creation Time: "+str(stop - start))

        msg = '\nTotal of %d jobs created'%njc
        if njc != self.ncjobs: msg = msg + ' from %d requested'%self.ncjobs
        msg = msg + '.\n'
        common.logger.message(msg)
        
        return
