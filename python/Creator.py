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

import os, string

class Creator(Actor):
    def __init__(self, job_type_name, cfg_params, ncjobs):
        self.job_type_name = job_type_name
        self.job_type = None
        self.cfg_params = cfg_params
        self.total_njobs = 0
        self.ncjobs = 0                  # nb of jobs to be created
        self.total_number_of_events = 0
        self.job_number_of_events = 0
        
        #

        self.createJobTypeObject()
        common.logger.debug(5, __name__+": JobType "+self.job_type.name()+" created")

        self.job_type.prepareSteeringCards()
        common.logger.debug(5, __name__+": Steering cards prepared")

        self.defineTotalNumberOfJobs_()
        common.logger.debug(5, __name__+": total # of jobs = "+`self.total_njobs`)

        # Set number of jobs to be created

        self.ncjobs = ncjobs
        if ncjobs == 'all' : self.ncjobs = self.total_njobs
        if ncjobs > self.total_njobs : self.ncjobs = self.total_njobs

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
            n = self.cfg_params['USER.total_number_of_events']
            if n == 'all': n = '-1'
            self.total_number_of_events = int(n)
        except KeyError:
            self.total_number_of_events = -1
            pass
        
        try:
            n = self.cfg_params['USER.job_number_of_events']
            self.job_number_of_events = int(n)
        except KeyError:
            msg = 'Warning. '
            msg += 'Number of events per job is not defined by user.\n'
            msg += 'Set to the total number of events.'
            common.logger.message(msg)
            self.job_number_of_events = self.total_number_of_events
            pass

        self.total_njobs = int((self.total_number_of_events-1)/self.job_number_of_events)+1
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

            common.logger.debug(6, "Creator::run(): job # "+`nj`)

            # Prepare configuration file

            self.job_type.modifySteeringCards(nj)

            # Create JDL
            # Maybe, it worths to move this call into Submitter,
            # i.e. to create scheduler-specific file at submission time ?

            common.scheduler.createJDL(nj)

            # Create script

            script_writer.modifyTemplateScript(nj)
            os.chmod(common.job_list[nj].scriptFilename(), 0744)

            common.jobDB.setStatus(nj, 'C')
            njc = njc + 1
            pass

        ####
        
        common.jobDB.save()

        msg = '\nTotal of %d jobs created'%njc
        if njc != self.ncjobs: msg = msg + ' from %d requested'%self.ncjobs
        msg = msg + '.\n'
        common.logger.message(msg)
        return
