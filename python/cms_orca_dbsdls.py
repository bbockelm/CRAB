from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import DataDiscovery
import DataLocation
import Scram
import TarBall

import os, string, re

###Fabio
import math

class Orca_dbsdls(JobType):
    def __init__(self, cfg_params):
        JobType.__init__(self, 'ORCA_DBSDLS')
        common.logger.debug(3,'ORCA::__init__')

        self.analisys_common_info = {}
        # Marco.
        self._params = {}
        self.cfg_params = cfg_params

        log = common.logger
        
        self.scram = Scram.Scram(cfg_params)
        scramArea = ''
        self.additional_inbox_files = []
        self.scriptExe = ''

        self.version = self.scram.getSWVersion()
        self.setParam_('application', self.version)
        common.analisys_common_info['sw_version'] = self.version
        common.analisys_common_info['copy_input_data'] = 0

        self.total_number_of_jobs = 0
        self.job_number_of_events = 0

        ### collect Data cards
        try:
            self.owner = cfg_params['ORCA.owner']
            self.setParam_('owner', self.owner)
            log.debug(6, "Orca::Orca(): owner = "+self.owner)
            self.dataset = cfg_params['ORCA.dataset']
            self.setParam_('dataset', self.dataset)
            log.debug(6, "Orca::Orca(): dataset = "+self.dataset)
        except KeyError:
            msg = "Error: owner and/or dataset not defined "
            raise CrabException(msg)

        self.dataTiers = []
        try:
            tmpDataTiers = string.split(cfg_params['ORCA.data_tier'],',')
            for tmp in tmpDataTiers:
                tmp=string.strip(tmp)
                self.dataTiers.append(tmp)
                pass
            pass
        except KeyError:
            pass
        log.debug(6, "Orca::Orca(): dataTiers = "+str(self.dataTiers))

        ## now the application
        try:
            self.executable = cfg_params['ORCA.executable']
            log.debug(6, "Orca::Orca(): executable = "+self.executable)
            self.setParam_('exe', self.executable)
        except KeyError:
            msg = "Error: executable not defined "
            raise CrabException(msg)

        try:
            self.orcarc_file = cfg_params['ORCA.orcarc_file']
            log.debug(6, "Orca::Orca(): orcarc file = "+self.orcarc_file)
            if (not os.path.exists(self.orcarc_file)):
                raise CrabException("User defined .orcarc file "+self.orcarc_file+" does not exist")
        except KeyError:
            log.message("Using empty orcarc file")
            self.orcarc_file = ''

        # output files
        try:
            self.output_file = []

            tmp = cfg_params['ORCA.output_file']
            if tmp != '':
                tmpOutFiles = string.split(cfg_params['ORCA.output_file'],',')
                log.debug(7, 'Orca::Orca(): output files '+str(tmpOutFiles))
                for tmp in tmpOutFiles:
                    tmp=string.strip(tmp)
                    self.output_file.append(tmp)
                    pass

            else:
                log.message("No output file defined: only stdout/err will be available")
                pass
            pass
        except KeyError:
            log.message("No output file defined: only stdout/err will be available")
            pass

        # script_exe file as additional file in inputSandbox
        try:
           self.scriptExe = cfg_params['ORCA.script_exe']
           self.additional_inbox_files.append(self.scriptExe)
        except KeyError:
           pass
        if self.scriptExe != '':
           if os.path.isfile(self.scriptExe):
              pass
           else:
              log.message("WARNING. file "+self.scriptExe+" not found")
              sys.exit()
                  
        ## additional input files
        try:
            tmpAddFiles = string.split(cfg_params['USER.additional_input_files'],',')
            for tmp in tmpAddFiles:
                tmp=string.strip(tmp)
                self.additional_inbox_files.append(tmp)
                pass
            pass
        except KeyError:
            pass

        try:
            self.total_number_of_events = int(cfg_params['USER.total_number_of_events'])
        except KeyError:
            msg = 'Must define total_number_of_events and job_number_of_events'
            raise CrabException(msg)

        try:
            self.first_event = int(cfg_params['USER.first_event'])
        except KeyError:
            self.first_event = 0
            pass
        log.debug(6, "Orca::Orca(): total number of events = "+`self.total_number_of_events`)
        #log.debug(6, "Orca::Orca(): events per job = "+`self.job_number_of_events`)
        log.debug(6, "Orca::Orca(): first event = "+`self.first_event`)

#DBSDLS-start
## Initialize the variables that are extracted from DBS/DLS and needed in other places of the code 
        self.maxEvents=0  # max events available   ( --> check the requested nb. of evts in Creator.py)
        self.DBSPaths={}  # all dbs paths requested ( --> input to the site local discovery script)
## Perform the data location and discovery (based on DBS/DLS)
        self.DataDiscoveryAndLocation(cfg_params)
#DBSDLS-end          

        self.TarBaller = TarBall.TarBall(self.executable, self.scram)
        self.tgzNameWithPath = self.TarBaller.prepareTarBall()

        try:
            self.ML = int(cfg_params['USER.activate_monalisa'])
        except KeyError:
            self.ML = 0
            pass

        self.setTaskid_()
        self.setParam_('taskId', self.cfg_params['taskId'])

        return

    def split(self, jobParams):
        """
        This method returns the list of orca_dbsdls specific job type items 
        needed to run the jobs
        """
        common.jobDB.load()
        njobs = self.total_number_of_jobs
        # create the empty structure
        for i in range(njobs):
            jobParams.append("")
        
        # fill the both the list and the DB (part of the code taken from jobsToDB)
        firstEvent = self.first_event
        lastJobsNumberOfEvents = self.job_number_of_events
        # last jobs is different...
        for job in range(njobs-1):
            jobParams[job] = [firstEvent, lastJobsNumberOfEvents]
            common.jobDB.setArguments(job, jobParams[job])
            firstEvent += self.job_number_of_events

        # this is the last job
        lastJobsNumberOfEvents = (self.total_number_of_events + self.first_event) - firstEvent
        status = common.jobDB.status(njobs - 1)
        jobParams[njobs - 1] = [firstEvent, lastJobsNumberOfEvents]
        common.jobDB.setArguments(njobs - 1, jobParams[njobs - 1])
        
        if (lastJobsNumberOfEvents!=self.job_number_of_events):
            common.logger.message(str(self.total_number_of_jobs-1)+' jobs will be created for '+str(self.job_number_of_events)+' events each plus 1 for '+str(lastJobsNumberOfEvents)+' events for a total of '+str(self.job_number_of_events*(self.total_number_of_jobs-1)+lastJobsNumberOfEvents)+' events')
        else:
            common.logger.message(str(self.total_number_of_jobs)+' jobs will be created for '+str(self.job_number_of_events)+' events each for a total of '+str(self.job_number_of_events*(self.total_number_of_jobs-1)+lastJobsNumberOfEvents)+' events')
 
        common.jobDB.save()
        return
    
    def getJobTypeArguments(self, nj, sched):
        params = common.jobDB.arguments(nj)
        
        if sched=="EDG" or sched=="CONDOR" or sched=="GRID":
            parString = "" + str(params[0])+' '+str(params[1])
        elif sched=="BOSS":
            parString = "" + str(params[0])+' '+str(params[1])
        else:
            return ""        
        return parString
    
    def numberOfJobs(self):
        first_event = self.first_event
        maxAvailableEvents = int(self.maxEvents)
        common.logger.debug(1,"Available events: "+str(maxAvailableEvents))
        if first_event>=maxAvailableEvents:
            raise CrabException('First event is bigger than maximum number of available events!')

        try:
            n = self.total_number_of_events
            if n == 'all': n = '-1'
            if n == '-1':
                tot_num_events = (maxAvailableEvents - first_event)
                common.logger.debug(1,"Analysing all available events "+str(tot_num_events))
            else:
                if maxAvailableEvents<(int(n)+ first_event): # + self.first_event):
                    raise CrabException('(First event + total events)='+str(int(n)+first_event)+' is bigger than maximum number of available events '+str(maxAvailableEvents)+' !! Use "total_number_of_events=-1" to analyze to whole dataset')
                tot_num_events = int(n)
        except KeyError:
            common.logger.message("total_number_of_events not defined, set it to maximum available")
            tot_num_events = (maxAvailableEvents - first_event)
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
            #if n == 'all' or n == '-1' or (int(n)>self.total_number_of_events and self.total_number_of_events>0):
            if n == 'all' or n == '-1' or (int(n)>tot_num_events and tot_num_events>0):
                common.logger.message("Asking more events than available: set it to maximum available")
                job_num_events = tot_num_events
                tot_num_jobs = 1
            else:
                job_num_events = int(n)
                tot_num_jobs = int((tot_num_events-1)/job_num_events)+1
                
        elif jobsPerTask>0:
            common.logger.debug(2,"total number of events: "+str(tot_num_events)+" JobPerTask "+str(jobsPerTask))
            job_num_events = int(math.floor((tot_num_events)/jobsPerTask))
            tot_num_jobs = jobsPerTask
            
        # should not happen...
        else:
            raise CrabException('Something wrong with splitting')

        common.logger.debug(2,"total number of events: "+str(tot_num_events)+" events per job: "+str(job_num_events))
        
        #used by jobsToDB for logs
        self.job_number_of_events = job_num_events
        self.total_number_of_jobs = tot_num_jobs 
        return tot_num_jobs
    
 
    def jobsToDB(self, nJobs):
        """
         Fill the DB with proper entries for ORCA and ORCA-DBS-DLS     
        """

        firstEvent = self.first_event
        lastJobsNumberOfEvents = self.job_number_of_events

        # last jobs is different...
        for job in range(nJobs-1):
            common.jobDB.setFirstEvent(job, firstEvent)
            common.jobDB.setMaxEvents(job, self.job_number_of_events)
            firstEvent=firstEvent+self.job_number_of_events

        # this is the last job
        common.jobDB.setFirstEvent(nJobs-1, firstEvent)
        lastJobsNumberOfEvents= (self.total_number_of_events+self.first_event)-firstEvent
        common.jobDB.setMaxEvents(nJobs-1, lastJobsNumberOfEvents)
    
        if (lastJobsNumberOfEvents!=self.job_number_of_events):
            common.logger.message(str(self.total_number_of_jobs-1)+' jobs will be created for '+str(self.job_number_of_events)+' events each plus 1 for '+str(lastJobsNumberOfEvents)+' events for a total of '+str(self.job_number_of_events*(self.total_number_of_jobs-1)+lastJobsNumberOfEvents)+' events')
        else:
            common.logger.message(str(self.total_number_of_jobs)+' jobs will be created for '+str(self.job_number_of_events)+' events each for a total of '+str(self.job_number_of_events*(self.total_number_of_jobs-1)+lastJobsNumberOfEvents)+' events')
        
        return    


    def wsSetupEnvironment(self, nj):
        """
        Returns part of a job script which prepares
        the execution environment for the job 'nj'.
        """

        # Prepare JobType-independent part
        txt = '' 
   
        txt += "NJob=$1\n"
        txt += "FirstEvent=$2\n"
        txt += "MaxEvents=$3\n"
        txt += 'echo "MonitorID=`echo ' + self._taskId + '`" | tee -a $RUNTIME_AREA/$repo\n'
        ### OLI_DANIELE
        txt += 'if [ $middleware == LCG ]; then \n' 
        txt += '    echo "MonitorJobID=`echo ${NJob}_$EDG_WL_JOBID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    echo "SyncGridJobId=`echo $EDG_WL_JOBID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    echo "SyncCE=`edg-brokerinfo getCE`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += 'elif [ $middleware == OSG ]; then\n'

        # OLI: added monitoring for dashbord, use hash of crab.cfg
        if common.scheduler.boss_scheduler_name == 'condor_g':
            # create hash of cfg file
            hash = makeCksum(common.work_space.cfgFileName())
            txt += '    echo "MonitorJobID=`echo ${NJob}_'+hash+'_$GLOBUS_GRAM_JOB_CONTACT`" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '    echo "SyncGridJobId=`echo $GLOBUS_GRAM_JOB_CONTACT`" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '    echo "SyncCE=`echo $hostname`" | tee -a $RUNTIME_AREA/$repo\n'
        else :
            txt += '    echo "MonitorJobID=`echo ${NJob}_$EDG_WL_JOBID`" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '    echo "SyncGridJobId=`echo $EDG_WL_JOBID`" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '    echo "SyncCE=`$EDG_WL_LOG_DESTINATION`" | tee -a $RUNTIME_AREA/$repo\n'

        txt += 'fi\n'
        txt += 'dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '\n'

        ## OLI_Daniele at this level  middleware already known

        txt += 'if [ $middleware == LCG ]; then \n' 
        txt += self.wsSetupCMSLCGEnvironment_()
        txt += 'elif [ $middleware == OSG ]; then\n'
        txt += '    time=`date -u +"%s"`\n'
        txt += '    WORKING_DIR=$OSG_WN_TMP/cms_$time\n'
        txt += '    echo "Creating working directory: $WORKING_DIR"\n'
        txt += '    /bin/mkdir -p $WORKING_DIR\n'
        txt += '    if [ ! -d $WORKING_DIR ] ;then\n'
        txt += '        echo "OSG WORKING DIR ==> $WORKING_DIR could not be created on on WN `hostname`"\n'
    
        txt += '        echo "JOB_EXIT_STATUS = 1"\n'
        txt += '        exit 1\n'
        txt += '    fi\n'
        txt += '\n'
        txt += '    echo "Change to working directory: $WORKING_DIR"\n'
        txt += '    cd $WORKING_DIR\n'
        txt += self.wsSetupCMSOSGEnvironment_() 
        txt += 'fi\n'

        # Prepare JobType-specific part
        scram = self.scram.commandName()
        txt += '\n\n'
        txt += 'echo "### SPECIFIC JOB SETUP ENVIRONMENT ###"\n'
        txt += scram+' project ORCA '+self.version+'\n'
        txt += 'status=$?\n'
        txt += 'if [ $status != 0 ] ; then\n'
        txt += '    echo "SET_EXE_ENV 1 ==>ERROR ORCA '+self.version+' not found on `hostname`" \n'
        txt += '    echo "JOB_EXIT_STATUS = 10034"\n'
        txt += '    echo "JobExitCode=10034" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'

        ## OLI_Daniele
        txt += '    if [ $middleware == OSG ]; then \n'
        txt += '        echo "Remove working directory: $WORKING_DIR"\n'
        txt += '        cd $RUNTIME_AREA\n'
        txt += '        /bin/rm -rf $WORKING_DIR\n'
        txt += '        if [ -d $WORKING_DIR ] ;then\n'
        txt += '            echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '        fi\n'
        txt += '    fi \n'
        txt += '    exit 1\n'
        txt += 'fi \n'
        txt += 'echo "ORCA_VERSION =  '+self.version+'"\n'
        txt += 'cd '+self.version+'\n'
        ### needed grep for bug in scramv1 ###

        #txt += 'eval `'+scram+' runtime -sh | grep -v SCRAMRT_LSB_JOBNAME`\n'

        # Handle the arguments:
        txt += "\n"
        txt += "## ARGUMNETS: $1 Job Number\n"
        txt += "## ARGUMNETS: $2 First Event for this job\n"
        txt += "## ARGUMNETS: $3 Max Event for this job\n"
        txt += "\n"
        txt += "narg=$#\n"
        txt += "if [ $narg -lt 3 ]\n"
        txt += "then\n"
        txt += "    echo 'SET_EXE_ENV 1 ==> ERROR Too few arguments' +$narg+ \n"
        txt += '    echo "JOB_EXIT_STATUS = 50113"\n'
        txt += '    echo "JobExitCode=50113" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'

        ## OLI_Daniele
        txt += '    if [ $middleware == OSG ]; then \n'
        txt += '        echo "Remove working directory: $WORKING_DIR"\n'
        txt += '        cd $RUNTIME_AREA\n'
        txt += '        /bin/rm -rf $WORKING_DIR\n'
        txt += '        if [ -d $WORKING_DIR ] ;then\n'
        txt += '            echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '        fi\n'
        txt += '    fi \n'
        txt += "    exit 1\n"
        txt += "fi\n"
        txt += "\n"

        # Prepare job-specific part
        job = common.job_list[nj]
        orcarc = os.path.basename(job.configFilename())
        txt += '\n'
        #DBSDLS-start
        #### site-local catalogue discovery mechanism:
        ## check that the site configuration file exists  
        txt += 'echo "### Site Local Catalogue Discovery ### "\n'
        txt += 'if [ $middleware == LCG ]; then \n' 
        txt += '   if [ -f $VO_CMS_SW_DIR/cms_site_config ];  then \n'
        txt += '      dbsdls_cms_site_config=$VO_CMS_SW_DIR/cms_site_config\n'
        txt += '   else\n'
        txt += '      echo "Site Local Catalogue Discovery Failed: No site configuration file $VO_CMS_SW_DIR/cms_site_config !" \n'
        txt += '      if [ $middleware == OSG ]; then \n'
        txt += '         echo "Remove working directory: $WORKING_DIR"\n'
        txt += '         cd $RUNTIME_AREA\n'
        txt += '         /bin/rm -rf $WORKING_DIR\n'
        txt += '         if [ -d $WORKING_DIR ] ;then\n'
        txt += '            echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '         fi\n'
        txt += '      fi \n'
        txt += '      echo "JOB_EXIT_STATUS = 1"\n'
        txt += '      echo "JobExitCode=10037" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '      dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '      exit 1\n'
        txt += '   fi \n'
        txt += 'elif [ $middleware == OSG ]; then\n'
        txt += '   if [ -f $GRID3_APP_DIR/cmssoft/cms_site_config ];  then \n'
        txt += '      dbsdls_cms_site_config=$GRID3_APP_DIR/cmssoft/cms_site_config\n'
        txt += '   elif [ -f $OSG_APP/cmssoft/cms_site_config ];  then \n'
        txt += '      dbsdls_cms_site_config=$OSG_APP/cmssoft/cms_site_config\n'
        txt += '   else\n'
        txt += '      echo "Site Local Catalogue Discovery Failed: No site configuration file $GRID3_APP_DIR/cmssoft/cms_site_config or $OSG_APP/cmssoft/cms_site_config !" \n'
        txt += '      if [ $middleware == OSG ]; then \n'
        txt += '         echo "Remove working directory: $WORKING_DIR"\n'
        txt += '         cd $RUNTIME_AREA\n'
        txt += '         /bin/rm -rf $WORKING_DIR\n'
        txt += '         if [ -d $WORKING_DIR ] ;then\n'
        txt += '            echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '         fi\n'
        txt += '      fi \n'
        txt += '      echo "JOB_EXIT_STATUS = 1"\n'
        txt += '      echo "JobExitCode=10037" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '      exit 1\n'
        txt += '   fi \n'
        txt += 'fi\n'
        txt += 'echo "Site Local Catalog Discovery, selected site configuration: $dbsdls_cms_site_config"\n'
        ## look for a site local script sent as inputsandbox otherwise use the default one under $VO_CMS_SW_DIR for LCG or $GRID3_APP_DIR/cmssoft or $OSG_APP/cmssoft for OSG
        txt += 'if [ -f $RUNTIME_AREA/cms_site_catalogue.sh ];  then \n'
        txt += ' sitelocalscript=$RUNTIME_AREA/cms_site_catalogue.sh \n'
        txt += 'elif [ $middleware == LCG ]; then \n' 
        txt += '   if [ -f $VO_CMS_SW_DIR/cms_site_catalogue.sh ]; then \n'
        txt += '   sitelocalscript=$VO_CMS_SW_DIR/cms_site_catalogue.sh \n'
        txt += '   else  \n'
        txt += '      echo "Site Local Catalogue Discovery Failed: No site local script cms_site_catalogue.sh !"\n'
        txt += '      if [ $middleware == OSG ]; then \n'
        txt += '         echo "Remove working directory: $WORKING_DIR"\n'
        txt += '         cd $RUNTIME_AREA\n'
        txt += '         /bin/rm -rf $WORKING_DIR\n'
        txt += '         if [ -d $WORKING_DIR ] ;then\n'
        txt += '            echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '         fi\n'
        txt += '      fi \n'
        txt += '      echo "JOB_EXIT_STATUS = 1"\n'
        txt += '      echo "JobExitCode=10038" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '      dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '      exit 1\n'
        txt += '   fi \n'
        txt += 'elif [ $middleware == OSG ]; then\n'
        txt += '   if [ -f $GRID3_APP_DIR/cmssoft/cms_site_catalogue.sh ];  then \n'
        txt += '      sitelocalscript=$GRID3_APP_DIR/cmssoft/cms_site_catalogue.sh\n'
        txt += '   elif [ -f $OSG_APP/cmssoft/cms_site_catalogue.sh ];  then \n'
        txt += '      sitelocalscript=$OSG_APP/cmssoft/cms_site_catalogue.sh\n'
        txt += '   else\n'
        txt += '      echo "Site Local Catalogue Discovery Failed: No site local script cms_site_catalogue.sh !"\n'
        txt += '      echo "JOB_EXIT_STATUS = 1"\n'
        txt += '      if [ $middleware == OSG ]; then \n'
        txt += '         echo "Remove working directory: $WORKING_DIR"\n'
        txt += '         cd $RUNTIME_AREA\n'
        txt += '         /bin/rm -rf $WORKING_DIR\n'
        txt += '         if [ -d $WORKING_DIR ] ;then\n'
        txt += '            echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '         fi\n'
        txt += '      fi \n'
        txt += '      echo "JOB_EXIT_STATUS = 1"\n'
        txt += '      echo "JobExitCode=10038" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '      dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '      exit 1\n'
        txt += '   fi \n'
        txt += 'fi\n'
        txt += 'echo "Site Local Catalog Discovery, selected local script: $sitelocalscript"\n'
        ## execute the site local configuration script with the user requied data as input
        inputdata=string.join(self.DBSPaths,' ')
        sitecatalog_cmd='$sitelocalscript -c $dbsdls_cms_site_config '+inputdata
        txt += sitecatalog_cmd+'\n'
        txt += 'sitestatus=$?\n'
        txt += 'if [ ! -f inputurl_orcarc ] || [ $sitestatus -ne 0 ]; then\n'
        txt += '   echo "Site Local Catalogue Discovery Failed: exiting with $sitestatus"\n'
        txt += '   echo "'+sitecatalog_cmd+'"\n'
        txt += '   if [ $middleware == OSG ]; then \n'
        txt += '      echo "Remove working directory: $WORKING_DIR"\n'
        txt += '      cd $RUNTIME_AREA\n'
        txt += '      /bin/rm -rf $WORKING_DIR\n'
        txt += '      if [ -d $WORKING_DIR ] ;then\n'
        txt += '         echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '      fi\n'
        txt += '   fi \n'
        txt += '   echo "JOB_EXIT_STATUS = 1"\n'
        txt += '   echo "JobExitCode=10039" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '   dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '   exit 1\n'
        txt += 'fi \n'
        ## append the orcarc fragment about the Input catalogues to the .orcarc
        txt += 'cp $RUNTIME_AREA/'+orcarc+' .orcarc\n'
        txt +=' cat inputurl_orcarc >> .orcarc\n'
        #DBSDLS-end

        if len(self.additional_inbox_files) > 0:
            for file in self.additional_inbox_files:
                file = os.path.basename(file)
                txt += 'if [ -e $RUNTIME_AREA/'+file+' ] ; then\n'
                txt += '    cp $RUNTIME_AREA/'+file+' .\n'
                txt += '    chmod +x '+file+'\n'
                txt += 'fi\n'
            pass 

        ### OLI_DANIELE
        txt += "echo 'SET_EXE_ENV 0 ==> job setup ok'\n"
        txt += 'echo "### END JOB SETUP ENVIRONMENT ###"\n\n'

        txt += 'echo "FirstEvent=$FirstEvent" >> .orcarc\n'
        txt += 'echo "MaxEvents=$MaxEvents" >> .orcarc\n'
        if self.ML:
            txt += 'echo "MonalisaJobId=$NJob" >> .orcarc\n'

        txt += '\n'
        txt += 'echo "***** cat .orcarc *********"\n'
        txt += 'cat .orcarc\n'
        txt += 'echo "****** end .orcarc ********"\n'
        return txt
    
    def wsBuildExe(self, nj):
        """
        Put in the script the commands to build an executable
        or a library.
        """

        txt = ""

        if os.path.isfile(self.tgzNameWithPath):
            txt += 'echo "tar xzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+'"\n'
            txt += 'tar xzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+'\n'
            txt += 'untar_status=$? \n'
            txt += 'if [ $untar_status -ne 0 ]; then \n'
            txt += '    echo "SET_EXE 1 ==> ERROR Untarring .tgz file failed"\n'
            txt += '    echo "JOB_EXIT_STATUS = $untar_status" \n'
            txt += '    echo "SanityCheckCode=$untar_status" | tee -a $repo\n'

            ### OLI_DANIELE
            txt += '   if [ $middleware == OSG ]; then \n'
            txt += '       echo "Remove working directory: $WORKING_DIR"\n'
            txt += '       cd $RUNTIME_AREA\n'
            txt += '       /bin/rm -rf $WORKING_DIR\n'
            txt += '       if [ -d $WORKING_DIR ] ;then\n'
            txt += '           echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
            txt += '       fi\n'
            txt += '   fi \n'
            txt += '   \n'
            txt += '   exit $untar_status \n'
            txt += 'else \n'
            txt += '    echo "Successful untar" \n'
            txt += 'fi \n'
            # TODO: what does this code do here ?
            # SL check that lib/Linux__... is present
            txt += 'mkdir -p lib/${SCRAM_ARCH} \n'
            pass
        txt += 'eval `'+self.scram.commandName()+' runtime -sh |grep -v SCRAMRT_LSB_JOBNAME`'+'\n'

        return txt

    def wsRenameOutput(self, nj):
        """
        Returns part of a job script which renames the produced files.
        """

        txt = '\n'
        file_list = ''
        for fileWithSuffix in self.output_file:
            output_file_num = self.numberFile_(fileWithSuffix, '$NJob')
            file_list=file_list+output_file_num+' '
            txt += '\n'
            txt += 'ls \n'
            txt += '\n'
            txt += 'ls '+fileWithSuffix+'\n'
            txt += 'exe_result=$?\n'
            txt += 'if [ $exe_result -ne 0 ] ; then\n'
            txt += '    echo "ERROR: No output file to manage"\n'
            txt += '    echo "JOB_EXIT_STATUS = $exe_result"\n'
            txt += '    echo "JobExitCode=60302" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
  
            ### OLI_DANIELE
            txt += '    if [ $middleware == OSG ]; then \n'
            txt += '        cd $RUNTIME_AREA\n'
            txt += '        echo "prepare dummy output file"\n'
            txt += '        echo "Processing of job output failed" > $RUNTIME_AREA/'+output_file_num+'\n'
            txt += '        echo "Remove working directory: $WORKING_DIR"\n'
            txt += '        /bin/rm -rf $WORKING_DIR\n'
            txt += '        if [ -d $WORKING_DIR ] ;then\n'
            txt += '            echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
            txt += '        fi\n'
            txt += '    fi \n'
            txt += '    \n'
            txt += '    exit $exe_result \n'
            txt += 'else\n'
            txt += '    cp '+fileWithSuffix+' $RUNTIME_AREA/'+output_file_num+'\n'
            txt += 'fi\n'
                      
            pass
       
     
        txt += 'cd $RUNTIME_AREA\n'
        file_list=file_list[:-1]
        txt += 'file_list="'+file_list+'"\n'
        ### OLI_DANIELE
        txt += 'if [ $middleware == OSG ]; then\n'  
        txt += '    cd $RUNTIME_AREA\n'
        txt += '    echo "Remove working directory: $WORKING_DIR"\n'
        txt += '    /bin/rm -rf $WORKING_DIR\n'
        txt += '    if [ -d $WORKING_DIR ] ;then\n'
        txt += '        echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '    fi\n'
        txt += 'fi\n'
        txt += '\n'

        return txt

    def executableName(self):
        if self.scriptExe != '':
            return "./" + os.path.basename(self.scriptExe)
        else:
            return self.executable

#DBSDLS-start
    def DataDiscoveryAndLocation(self, cfg_params):

        fun = "Orca::DataDiscoveryAndLocation()"

        ## Contact the DBS
        try:
           self.pubdata=DataDiscovery.DataDiscovery(self.owner,
                                                    self.dataset,
                                                    self.dataTiers,
                                                    cfg_params)
           self.pubdata.fetchDBSInfo()

        except DataDiscovery.NotExistingDatasetError, ex :
                msg = 'ERROR ***: failed Data Discovery in DBS : %s'%ex.getErrorMessage()
                raise CrabException(msg)

        except DataDiscovery.NoDataTierinProvenanceError, ex :
                msg = 'ERROR ***: failed Data Discovery in DBS : %s'%ex.getErrorMessage()
                raise CrabException(msg)
        except DataDiscovery.DataDiscoveryError, ex:
                msg = 'ERROR ***: failed Data Discovery in DBS  %s'%ex.getErrorMessage()
                raise CrabException(msg)

        ## get list of all required data in the form of dbs paths  (dbs path = /dataset/datatier/owner)
        self.DBSPaths=self.pubdata.getDBSPaths()
        common.logger.message("Required data are : ")
        for path in self.DBSPaths:
          common.logger.message(" --> "+path )

        ## get max number of events
        #common.logger.debug(10,"number of events for primary fileblocks %i"%self.pubdata.getMaxEvents())
        self.maxEvents=self.pubdata.getMaxEvents() ##  self.maxEvents used in Creator.py 
        common.logger.message("\nThe number of available events is %s"%self.maxEvents)

        ## get fileblocks corresponding to the required data
        fb=self.pubdata.getFileBlocks()


        ## Contact the DLS and build a list of sites hosting the fileblocks
        try:
          dataloc=DataLocation.DataLocation(self.pubdata.getFileBlocks(),cfg_params)
          dataloc.fetchDLSInfo()
        except DataLocation.DataLocationError , ex:
                msg = 'ERROR ***: failes Data Location in DLS \n %s '%ex.getErrorMessage()
                raise CrabException(msg)


        sites=dataloc.getSites()

        if len(sites)==0:
            msg = 'No sites hosting all the needed data! Exiting... '
            raise CrabException(msg)
        common.logger.message("List of Sites hosting the data : "+str(sites))
        common.logger.debug(6, "List of Sites: "+str(sites))
        common.analisys_common_info['sites']=sites    ## used in SchedulerEdg.py in createSchScript
        self.setParam_('TargetCE', ','.join(sites))

        return

#DBDDLS-stop


    def nJobs(self):
        # TODO: should not be here !
        # JobType should have no internal knowledge about submitted jobs
        # One possibility is to use len(common.job_list).
        """ return the number of job to be created """
        return len(common.job_list)

    def prepareSteeringCards(self):
        """
        modify the orcarc card provided by the user, 
        writing a new card into share dir
        """
        infile = ''
        try:
          infile = open(self.orcarc_file,'r')
        except:
          self.orcarc_file = 'empty.orcarc'
          cmd='touch '+self.orcarc_file
          runCommand(cmd)
          infile = open(self.orcarc_file,'r')
            
        outfile = open(common.work_space.jobDir()+self.name()+'.orcarc', 'w')
           
        inline=infile.readlines()
        ### remove from user card these lines ###
        wordRemove=['InputFileCatalogURL', 'InputCollections', 'FirstEvent', 'MaxEvents', 'TFileAdaptor']
        for line in inline:
            word = string.strip(string.split(line,'=')[0])

            if word not in wordRemove:
                outfile.write(line)
            else:
                continue
            pass

        outfile.write('\n\n##### The following cards have been created by CRAB: DO NOT TOUCH #####\n')
        outfile.write('TFileAdaptor = true\n')

        outfile.write('MonRecAlisaBuilder=false\n')

        outfile.write('InputCollections=/System/'+self.owner+'/'+self.dataset+'/'+self.dataset+'\n')

        infile.close()
        outfile.close()
        return
    
    def modifySteeringCards(self, nj):
        """
        Creates steering cards file modifying a template file
        """
        return

    def cardsBaseName(self):
        """
        Returns name of user orcarc card-file
        """
        return os.path.split (self.orcarc_file)[1]

    ### content of input_sanbdox ...
    def inputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL input sandbox.
        """
        inp_box = []
        # dict added to delete duplicate from input sandbox file list
        seen = {}
        ## code
        if os.path.isfile(self.tgzNameWithPath):
            inp_box.append(self.tgzNameWithPath)

##DBSDLS: no orcarc_CE and init_CE.sh produced on UI , thus not inserting them in inputSandbox
#        ## orcarc
#        for o in self.allOrcarcs:
#          for f in o.fileList():
#            if (f not in seen.keys()):
#              inp_box.append(common.work_space.jobDir()+f)
#              seen[f] = 1

        ## config
        inp_box.append(common.job_list[nj].configFilename())
        ## additional input files
        #inp_box = inp_box + self.additional_inbox_files
        return inp_box

    ### and of output_sandbox
    def outputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL output sandbox.
        """
        out_box = []

        stdout=common.job_list[nj].stdout()
        stderr=common.job_list[nj].stderr()
        #out_box.append(stdout)
        #out_box.append(stderr)

        ## User Declared output files
        for out in self.output_file:
            n_out = nj + 1 
            #FEDE 
            #out_box.append(self.version+'/'+self.numberFile_(out,str(n_out)))
            out_box.append(self.numberFile_(out,str(n_out)))
        return out_box

    def getRequirements(self):
        """
       return job requirements to add to jdl files 
        """
        req = ''
        if common.analisys_common_info['sites']:
            if common.analisys_common_info['sw_version']:
                req='Member("VO-cms-' + \
                     common.analisys_common_info['sw_version'] + \
                     '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'
            if len(common.analisys_common_info['sites'])>0:
                req = req + ' && ('
                for i in range(len(common.analisys_common_info['sites'])):
                    req = req + 'other.GlueCEInfoHostName == "' \
                         + common.analisys_common_info['sites'][i] + '"'
                    if ( i < (int(len(common.analisys_common_info['sites']) - 1)) ):
                        req = req + ' || '
            req = req + ')'
        #print "req = ", req 
        return req
         
    def numberFile_(self, file, txt):
        """
        append _'txt' before last extension of a file
        """
        p = string.split(file,".")
        # take away last extension
        name = p[0]
        for x in p[1:-1]:
           name=name+"."+x
        # add "_txt"
        if len(p)>1:
          ext = p[len(p)-1]
          #result = name + '_' + str(txt) + "." + ext
          result = name + '_' + txt + "." + ext
        else:
          #result = name + '_' + str(txt)
          result = name + '_' + txt
        
        return result


    def stdOut(self):
        return self.stdOut_

    def stdErr(self):
        return self.stdErr_

    # marco
    def setParam_(self, param, value):
        self._params[param] = value

    def getParams(self):
        return self._params

    def setTaskid_(self):
        self._taskId = self.cfg_params['taskId']
        
    def getTaskid(self):
        return self._taskId
    # marco

    def configFilename(self):
        """ return the config filename """
        return self.name()+'.orcarc'

    ### OLI_DANIELE
    def wsSetupCMSOSGEnvironment_(self):
        """
        Returns part of a job script which is prepares
        the execution environment and which is common for all CMS jobs.
        """
        txt = '\n'
        txt += '   echo "### SETUP CMS OSG  ENVIRONMENT ###"\n'
        txt += '   if [ -f $GRID3_APP_DIR/cmssoft/cmsset_default.sh ] ;then\n'
        txt += '      # Use $GRID3_APP_DIR/cmssoft/cmsset_default.sh to setup cms software\n'
        txt += '       source $GRID3_APP_DIR/cmssoft/cmsset_default.sh '+self.version+'\n'
        txt += '   elif [ -f $OSG_APP/cmssoft/cmsset_default.sh ] ;then\n'
        txt += '      # Use $OSG_APP/cmssoft/cmsset_default.sh to setup cms software\n'
        txt += '       source $OSG_APP/cmssoft/cmsset_default.sh '+self.version+'\n'
        txt += '   else\n'
        txt += '       echo "SET_CMS_ENV 10020 ==> ERROR $GRID3_APP_DIR/cmssoft/cmsset_default.sh and $OSG_APP/cmssoft/cmsset_default.sh file not found"\n'
        txt += '       echo "JOB_EXIT_STATUS = 10020"\n'
        txt += '       echo "JobExitCode=10020" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '       dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '       exit 1\n'
        txt += '\n'
        txt += '       echo "Remove working directory: $WORKING_DIR"\n'
        txt += '       cd $RUNTIME_AREA\n'
        txt += '       /bin/rm -rf $WORKING_DIR\n'
        txt += '       if [ -d $WORKING_DIR ] ;then\n'
        txt += '           echo "OSG WORKING DIR ==> $WORKING_DIR could not be deleted on on WN `hostname`"\n'
        txt += '       fi\n'
        txt += '\n'
        txt += '       exit 1\n'
        txt += '   fi\n'
        txt += '\n'
        txt += '   echo "SET_CMS_ENV 0 ==> setup cms environment ok"\n'
        txt += '   echo " END SETUP CMS OSG  ENVIRONMENT "\n'

        return txt
 
    ### OLI_DANIELE
    def wsSetupCMSLCGEnvironment_(self):
        """
        Returns part of a job script which is prepares
        the execution environment and which is common for all CMS jobs.
        """
        txt  = '   \n'
        txt += '   echo " ### SETUP CMS LCG  ENVIRONMENT ### "\n'
        txt += '   if [ ! $VO_CMS_SW_DIR ] ;then\n'
        txt += '       echo "SET_CMS_ENV 10031 ==> ERROR CMS software dir not found on WN `hostname`"\n'
        txt += '       echo "JOB_EXIT_STATUS = 10031" \n'
        txt += '       echo "JobExitCode=10031" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '       dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '       exit 1\n'
        txt += '   else\n'
        txt += '       echo "Sourcing environment... "\n'
        txt += '       if [ ! -s $VO_CMS_SW_DIR/cmsset_default.sh ] ;then\n'
        txt += '           echo "SET_CMS_ENV 10020 ==> ERROR cmsset_default.sh file not found into dir $VO_CMS_SW_DIR"\n'
        txt += '           echo "JOB_EXIT_STATUS = 10020"\n'
        txt += '           echo "JobExitCode=10020" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '           dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '           exit 1\n'
        txt += '       fi\n'
        txt += '       echo "sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '       source $VO_CMS_SW_DIR/cmsset_default.sh\n'
        txt += '       result=$?\n'
        txt += '       if [ $result -ne 0 ]; then\n'
        txt += '           echo "SET_CMS_ENV 10032 ==> ERROR problem sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '           echo "JOB_EXIT_STATUS = 10032"\n'
        txt += '           echo "JobExitCode=10032" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '           dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '           exit 1\n'
        txt += '       fi\n'
        txt += '   fi\n'
        txt += '   \n'
        txt += '   string=`cat /etc/redhat-release`\n'
        txt += '   echo $string\n'
        txt += '   if [[ $string = *alhalla* ]]; then\n'
        txt += '       echo "SCRAM_ARCH= $SCRAM_ARCH"\n'
        txt += '   elif [[ $string = *Enterprise* ]] || [[ $string = *cientific* ]]; then\n'
        txt += '       export SCRAM_ARCH=slc3_ia32_gcc323\n'
        txt += '       echo "SCRAM_ARCH= $SCRAM_ARCH"\n'
        txt += '   else\n'
        txt += '       echo "SET_CMS_ENV 1 ==> ERROR OS unknown, LCG environment not initialized"\n'
        txt += '       echo "JOB_EXIT_STATUS = 10033"\n'
        txt += '       echo "JobExitCode=10033" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '       dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '       exit 1\n'
        txt += '   fi\n'
        txt += '   echo "SET_CMS_ENV 0 ==> setup cms environment ok"\n'
        txt += '   echo "### END SETUP CMS LCG ENVIRONMENT ###"\n'
        return txt

