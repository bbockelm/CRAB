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
# marco
class Creator(Actor):
    def __init__(self, job_type_name, cfg_params, ncjobs, job_type):
        self.job_type_name = job_type_name
#        self.job_type = None
# marco
        self.job_type = job_type
        self.cfg_params = cfg_params
        self.total_njobs = 0
        self.total_number_of_events = 0
        self.job_number_of_events = 0
        self.first_event = 0

# marco
#        self.createJobTypeObject()
#        self.createJobTypeObject()   ### Daniele bug fix
        common.logger.debug(5, __name__+": JobType "+self.job_type.name()+" created")

        self.job_type.prepareSteeringCards()
        common.logger.debug(5, __name__+": Steering cards prepared")

        self.defineTotalNumberOfJobs_()
        common.logger.debug(5, __name__+": total # of jobs = "+`self.total_njobs`)

        # Set number of jobs to be created

        self.ncjobs = ncjobs
        if ncjobs == 'all' : self.ncjobs = self.total_njobs
        if ncjobs > self.total_njobs : self.ncjobs = self.total_njobs
        
        fileCODE1 = open(common.work_space.shareDir()+"/.code","a")
        ### commented for FAMOS  
        if self.job_type.name() == 'ORCA' or self.job_type.name() == 'ORCA_DBSDLS':
            self.owner = cfg_params['ORCA.owner']
            self.dataset = cfg_params['ORCA.dataset']
            fileCODE1.write('::'+str(self.job_type.name())+'::'+str(self.ncjobs)+'::'+str(self.dataset)+'::'+str(self.owner))
            pass
        elif self.job_type.name() == 'FAMOS':
            self.inputFile = cfg_params['FAMOS.input_lfn']
            self.executable = cfg_params['FAMOS.executable']
            fileCODE1.write('::'+str(self.job_type.name())+'::'+str(self.ncjobs)+'::'+str(self.inputFile)+'::'+str(self.executable))
            pass
        fileCODE1.close()


        #TODO: deprecated code, not needed,
        # will be eliminated when WorkSpace.saveConfiguration()
        # will be improved.
        #
        # Set/Save Job Type name

        jt_fname = common.work_space.shareDir() + 'jobtype'
        if os.path.exists(jt_fname):
            # Read stored job type name
            jt_file = open(jt_fname, 'r')
            jt = jt_file.read()
            if self.job_type_name:
                if ( jt != self.job_type_name+'\n' ):
                    msg = 'Job Type mismatch: requested <' + self.job_type_name
                    msg += '>, found <' + jt[:-1] + '>.'
                    raise CrabException(msg)
                pass
            else:
                self.job_type_name = jt[:-1]
                pass
            jt_file.close()
            pass
        else:
            # Save job type name
            jt_file = open(jt_fname, 'w')
            jt_file.write(self.job_type_name+'\n')
            jt_file.close()
            pass
        #end of deprecated code

        common.logger.debug(5, "Creator constructor finished")
        return

    def defineTotalNumberOfJobs_(self):
        """
        Calculates the total number of jobs to be created.
        """

        try:
            self.first_event = int(self.cfg_params['USER.first_event'])
        except KeyError:
            self.first_event = 0
        common.logger.debug(1,"First event ot be analyzed: "+str(self.first_event))

        # TODO Could we find a better way to get this number?
        maxAvailableEvents = int(self.job_type.maxEvents)
        common.logger.debug(1,"Available events: "+str(maxAvailableEvents))

        # some sanity check
        if self.first_event>=maxAvailableEvents:
            raise CrabException('First event is bigger than maximum number of available events!')

        # the total number of events to be analyzed
        try:
            n = self.cfg_params['USER.total_number_of_events']
            if n == 'all': n = '-1'
            if n == '-1':
                self.total_number_of_events = (maxAvailableEvents - self.first_event)
                common.logger.debug(1,"Analysing all available events "+str(self.total_number_of_events))
            else:
                if maxAvailableEvents<(int(n)+self.first_event):
                    raise CrabException('(First event + total events)='+str(int(n)+self.first_event)+' is bigger than maximum number of available events '+str(maxAvailableEvents)+' !! Use "total_number_of_events=-1" to analyze to whole dataset')
                self.total_number_of_events = int(n)
        except KeyError:
            common.logger.message("total_number_of_events not defined, set it to maximum available")
            self.total_number_of_events = (maxAvailableEvents - self.first_event)
            pass
        common.logger.message("Total number of events to be analyzed: "+str(self.total_number_of_events))


        # read user directives
        eventPerJob=0
        try:
            eventPerJob = self.cfg_params['USER.job_number_of_events']
        except KeyError:
            pass
        
        jobsPerTask=0
        try:
            jobsPerTask = int(self.cfg_params['USER.total_number_of_jobs'])
        except KeyError:
            pass

        # If both the above set, complain and use event per jobs
        if eventPerJob>0 and jobsPerTask>0:
            msg = 'Warning. '
            msg += 'job_number_of_events and total_number_of_jobs are both defined '
            msg += 'Using job_number_of_events.'
            common.logger.message(msg)
            jobsPerTask = 0 
        if eventPerJob==0 and jobsPerTask==0:
            msg = 'Warning. '
            msg += 'job_number_of_events and total_number_of_jobs are not defined '
            msg += 'Creating just one job for all events.'
            common.logger.message(msg)
            jobsPerTask = 1 

        # first case: events per job defined
        if eventPerJob>0:
            n=eventPerJob
            if n == 'all' or n == '-1' or (int(n)>self.total_number_of_events and self.total_number_of_events>0):
                common.logger.message("Asking more events than available: set it to maximum available")
                self.job_number_of_events = self.total_number_of_events
                self.total_njobs = 1
            else:
                self.job_number_of_events = int(n)
                self.total_njobs = int((self.total_number_of_events-1)/self.job_number_of_events)+1
        # second case: jobs per task defined
        elif jobsPerTask>0:
            common.logger.debug(2,"total number of events: "+str(self.total_number_of_events)+" JobPerTask "+str(jobsPerTask))
            self.job_number_of_events = int(math.floor((self.total_number_of_events)/jobsPerTask))
            self.total_njobs = jobsPerTask
        # should not happen...
        else:
            raise CrabException('Somthing wrong with splitting')

        common.logger.debug(2,"total number of events: "+str(self.total_number_of_events)+
            " events per job: "+str(self.job_number_of_events))

        return

    def writeJobsSpecsToDB(self):
        """
        Write firstEvent and maxEvents in the DB for future use
        """

        common.jobDB.load()
        # case one: write first and max events
        nJobs=self.nJobs()

        firstEvent=self.first_event
        lastJobsNumberOfEvents = self.job_number_of_events

        try:
            self.events_management = common.analisys_common_info['events_management']
            print "self.events_management = ", self.events_management
        except KeyError: self.events_management = 0

        # last jobs is different...
        for job in range(nJobs-1):
            common.jobDB.setFirstEvent(job, firstEvent)
            common.jobDB.setMaxEvents(job, self.job_number_of_events)
            ### fede checks parameter for famos and orca
            #try:
            #    self.events_management = common.analisys_common_info['events_management']
            #    print "self.events_management = ", self.events_management
            #except KeyError: self.events_management = 0
            firstEvent=firstEvent+self.job_number_of_events

        # this is the last job
        common.jobDB.setFirstEvent(nJobs-1, firstEvent)
        if int(self.events_management) == 1:
            lastJobsNumberOfEvents= (self.total_number_of_events+self.first_event)-firstEvent
        common.jobDB.setMaxEvents(nJobs-1, lastJobsNumberOfEvents)
    
        if (lastJobsNumberOfEvents!=self.job_number_of_events):
            common.logger.message(str(self.total_njobs-1)+' jobs will be created for '+str(self.job_number_of_events)+' events each plus 1 for '+str(lastJobsNumberOfEvents)+' events for a total of '+str(self.job_number_of_events*(self.total_njobs-1)+lastJobsNumberOfEvents)+' events')
        else:
            common.logger.message(str(self.total_njobs)+' jobs will be created for '+str(self.job_number_of_events)+' events each for a total of '+str(self.job_number_of_events*(self.total_njobs-1)+lastJobsNumberOfEvents)+' events')

        # case two (to be implemented) write eventCollections for each jobs

        # save the DB
        common.jobDB.save()
        return

    def nJobs(self):
        return self.total_njobs
    
    def createJobTypeObject(self):
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

        self.job_type = klass(self.cfg_params)
        return

    def jobType(self):
        return self.job_type

    def run(self):
        """
        The main method of the class.
        """

        common.logger.debug(5, "Creator::run() called")

        # Instantiate ScriptWriter

        script_writer = ScriptWriter('crab_template.sh')

        # Loop over jobs

        njc = 0
        for nj in range(self.total_njobs):
            if njc == self.ncjobs : break
            st = common.jobDB.status(nj)
            if st != 'X': continue

            common.logger.message("Creating job # "+`(nj+1)`)

            # Prepare configuration file

            self.job_type.modifySteeringCards(nj)
            # Create script (sh)
            #print "nel for prima del modifyTemplateScript, nj vale", nj
            script_writer.modifyTemplateScript(nj)
            os.chmod(common.job_list[nj].scriptFilename(), 0744)

            # Create scheduler scripts (jdl)
            common.scheduler.createSchScript(nj)

            common.jobDB.setStatus(nj, 'C')
            # common: write input and output sandbox
            common.jobDB.setInputSandbox(nj, self.job_type.inputSandbox(nj))

            outputSandbox=self.job_type.outputSandbox(nj)
            stdout=common.job_list[nj].stdout() 
            stderr=common.job_list[nj].stderr()
            outputSandbox.append(common.job_list[nj].stdout())
            # check if out!=err
            if stdout != stderr:
                outputSandbox.append(common.job_list[nj].stderr())
            common.jobDB.setOutputSandbox(nj, outputSandbox)
            common.jobDB.setTaskId(nj, self.cfg_params['taskId'])

            njc = njc + 1
            pass

        ####

        common.jobDB.save()

        msg = '\nTotal of %d jobs created'%njc
        if njc != self.ncjobs: msg = msg + ' from %d requested'%self.ncjobs
        msg = msg + '.\n'
        common.logger.message(msg)
        return
