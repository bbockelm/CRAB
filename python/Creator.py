from Actor import Actor
from WorkSpace import WorkSpace
from JobList import JobList
from JobDB import JobDB
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

        # Set number of jobs to be created
        # --------------------------------->  Boss4: changed, "all" by default 

        self.ncjobs = ncjobs
        if ncjobs == 'all' : self.ncjobs = self.total_njobs
        if ncjobs > self.total_njobs : self.ncjobs = self.total_njobs
        
        # This is code for proto-monitoring
        code=common.taskDB.dict("CODE")
        if self.job_type.name() == 'CMSSW':
            try: 
                self.primaryDataset = cfg_params['CMSSW.datasetpath'].split("/")[1]
            except:
                self.primaryDataset = 'None'
            try:
                self.ProcessedDataset = cfg_params['CMSSW.datasetpath'].split("/")[3]
            except:
                self.ProcessedDataset = '  '
            common.taskDB.setDict("CODE",(code+'::'+str(self.job_type.name())+'::'+str(self.ncjobs)+'::'+str(self.primaryDataset)+'::'+str(self.ProcessedDataset)))
            pass

        # This is code for dashboard
          
        #First checkProxy
        common.scheduler.checkProxy()
        try:
            fl = open(common.work_space.shareDir() + '/' + self.cfg_params['apmon'].fName, 'w')
            self.cfg_params['GridName'] = runCommand("voms-proxy-info -identity")
            common.logger.debug(5, "GRIDNAME: "+self.cfg_params['GridName'])
            taskType = 'analysis'
            try: VO = cfg_params['EDG.virtual_organization']
            except KeyError: VO = 'cms'

            params = {'tool': common.prog_name,\
                      'JSToolVersion': common.prog_version_str, \
                      'tool_ui': os.environ['HOSTNAME'], \
                      'scheduler': self.cfg_params['CRAB.scheduler'], \
                      'GridName': self.cfg_params['GridName'].strip(), \
                      'taskType': taskType, \
                      'vo': VO, \
                      'user': self.cfg_params['user']}
            jtParam = self.job_type.getParams()
            for i in jtParam.iterkeys():
                params[i] = string.strip(jtParam[i])
            for j, k in params.iteritems():
#                print "Values: %s %s"%(j, k)
                fl.write(j + ':' + k + '\n')
            fl.close()
        except:
            exctype, value = sys.exc_info()[:2]
            common.logger.message("Creator::run Exception raised in collection params for dashboard: %s %s"%(exctype, value))
            pass

        #TODO: deprecated code, not needed,
        # will be eliminated when WorkSpace.saveConfiguration()
        # will be improved.
        #
        # Set/Save Job Type name

        try:
            jt = common.taskDB.dict("JobType")
            if self.job_type_name:
                if ( jt != self.job_type_name+'\n' ):
                    msg = 'Job Type mismatch: requested <' + self.job_type_name
                    msg += '>, found <' + jt[:-1] + '>.'
                    raise CrabException(msg)
                pass
            else:
                self.job_type_name = jt[:-1]
                pass
            pass
        except CrabException:
            # Save job type name
            common.taskDB.setDict("JobType",self.job_type_name)
            pass
        #end of deprecated code

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

        script_writer = ScriptWriter('crab_template.sh')

        # Loop over jobs
        argsList = []

        njc = 0
        block = -1 # first block is 0
        lastDest=''
        for nj in range(self.total_njobs):
            if njc == self.ncjobs : break
            st = common.jobDB.status(nj)
            if st != 'X': continue

            common.logger.debug(1,"Creating job # "+`(nj+1)`)

            # Prepare configuration file

            self.job_type.modifySteeringCards(nj)
            # Create script (sh)
            script_writer.modifyTemplateScript()
            os.chmod(common.taskDB.dict("ScriptName"), 0744)

            # Create XML script and declare related jobs 
            argsList.append( str(nj+1)+' '+ self.job_type.getJobTypeArguments(nj, self.cfg_params['CRAB.scheduler']) )

            common.jobDB.setStatus(nj, 'C')
            # common: write input and output sandbox
            common.jobDB.setInputSandbox(nj, self.job_type.inputSandbox(nj))

            outputSandbox=self.job_type.outputSandbox(nj)
            # check if out!=err
            common.jobDB.setOutputSandbox(nj, outputSandbox)
            common.jobDB.setTaskId(nj, self.cfg_params['taskId'])

            currDest=common.jobDB.destination(nj)
            if (currDest!=lastDest):
                block+=1
                lastDest = currDest
                pass
            common.jobDB.setBlock(nj,block)
            njc = njc + 1
            pass

        ####
        common.scheduler.createXMLSchScript(self.total_njobs, argsList)
        common.logger.message('Creating '+str(self.total_njobs)+' jobs, please wait...')
        common.scheduler.declareJob_()   #Add for BOSS4

        stop = time.time()
        common.logger.debug(2, "Creation Time: "+str(stop - start))
        common.logger.write("Creation Time: "+str(stop - start))

        common.jobDB.save()
        common.taskDB.save()
        msg = '\nTotal of %d jobs created'%njc
        if njc != self.ncjobs: msg = msg + ' from %d requested'%self.ncjobs
        msg = msg + '.\n'
        common.logger.message(msg)
        
        return
