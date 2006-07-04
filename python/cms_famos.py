from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import PubDB
import orcarcBuilder
import orcarcBuilderOld
import Scram
import TarBall

import os, string, re

class Famos(JobType):
    def __init__(self, cfg_params):
        JobType.__init__(self, 'FAMOS')
        common.logger.debug(3,'FAMOS::__init__')

        self.analisys_common_info = {}

        self._params = {}
        self.cfg_params = cfg_params

        log = common.logger
        
        self.scram = Scram.Scram(cfg_params)
        scramArea = ''
        self.additional_inbox_files = []
        self.scriptExe = ''

        ### georgia
        self.input_pu_files = []
        self.orcarc_pu_files = []
        self.in_file_list = []
        self.cmkin_file_list = []

        self.version = self.scram.getSWVersion()
        self.setParam_('application', self.version)
        common.analisys_common_info['sw_version'] = self.version

# added today
        self.total_number_of_jobs = 0
        self.job_number_of_events = 0

        try:
            self.executable = cfg_params['FAMOS.executable']
            self.setParam_('exe', self.executable)
            log.debug(6, "Famos::Famos(): executable = "+self.executable)
        except KeyError:
            msg = "Error: executable not defined "
            raise CrabException(msg)

        try:
            self.orcarc_file = cfg_params['FAMOS.orcarc_file']
            log.debug(6, "Famos::Famos(): orcarc file = "+self.orcarc_file)
            if (not os.path.exists(self.orcarc_file)):
                raise CrabException("User defined .orcarc file "+self.orcarc_file+" does not exist")
        except KeyError:
            log.message("Using empty orcarc file")
            self.orcarc_file = ''

        ### FEDE  
        common.analisys_common_info['copy_input_data'] = 1
        common.analisys_common_info['events_management'] = 0

        try:
            self.input_lfn = cfg_params['FAMOS.input_lfn']
        except KeyError:
            log.message("LFN of input ntuple for FAMOS")

        ### georgia 15.03
        try:
            self.events_per_ntuple = cfg_params['FAMOS.events_per_ntuple']
        except KeyError:
            log.message("number of events per input file for FAMOS")
              
        try:
            self.input_pu_lfn = cfg_params['FAMOS.input_pu_lfn']
        except KeyError:
            log.message("lfn of input pile-up ntuples")

        try:
            self.number_pu_ntuples = cfg_params['FAMOS.number_pu_ntuples']
        except KeyError:
            log.message("number of pu ntuples per job")

        ## fill the list of pile-up files

        pu_file = self.input_pu_lfn
        r = string.split(pu_file,".")
        exte = r[len(r)-1]
        t = string.split(r[0],"/")
        pname= t[len(t)-1]
        self.usernamepu = t[0]
         
        n_pu = int(self.number_pu_ntuples)

        ind = 1
        while ind < n_pu+1:
            # pu_file = t[0]+'/'+pname+str(ind)+"."+exte
            pu_file = pname+str(ind)+"."+exte
            orcarc_file_num = ' `pwd`/'+pu_file
            self.input_pu_files.append(pu_file)
            self.orcarc_pu_files.append(orcarc_file_num)
            ind = ind + 1

        # output files
        try:
            self.output_file = []

            tmp = cfg_params['FAMOS.output_file']
            if tmp != '':
                tmpOutFiles = string.split(cfg_params['FAMOS.output_file'],',')
                log.debug(7, 'Famos::Famos(): output files '+str(tmpOutFiles))
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
           self.scriptExe = cfg_params['FAMOS.script_exe']
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

        ### Added from georgia (but appears also in Creator.py)
        try:
            self.job_number_of_events = int(cfg_params['FAMOS.job_number_of_events'])
            log.debug(6, "Famos::Famos(): job number of events = "+`self.job_number_of_events`)
        except KeyError:
            msg = 'Must define total_number_of_events and job_number_of_events'
            raise CrabException(msg)
        
        try:
            self.total_number_of_events = int(cfg_params['FAMOS.total_number_of_events'])
            self.maxEvents=self.total_number_of_events
            log.debug(6, "Famos::Famos(): total number of events = "+`self.total_number_of_events`)
        except KeyError:
            msg = 'Must define total_number_of_events and job_number_of_events'
            raise CrabException(msg)

        try:
            self.first_event = int(cfg_params['FAMOS.first_event'])
            log.debug(6, "Famos::Famos(): first event = "+`self.first_event`)
        except KeyError:
            self.first_event = 0
            pass

        
        ### georgia for FAMOS

        nnt = int(self.total_number_of_events)/int(self.events_per_ntuple) 
        self.ntj = int(self.job_number_of_events)/int(self.events_per_ntuple)
        
        in_file = self.input_lfn
        p = string.split(in_file,".")
        ext = p[len(p)-1]
        q = string.split(p[0],"/")
        name= q[len(q)-1]

        self.username = q[0]

        index = 1
        while index < nnt+1:
            input_file_num = name+'_'+str(index)+"."+ext
            cmkin_file_num = ' `pwd`/'+input_file_num
            self.in_file_list.append(input_file_num)
            self.cmkin_file_list.append(cmkin_file_num)
            index = index + 1

        ### georgia

        # [-- self.checkNevJobs() --]

        ## SL
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
        This method returns the list of orca specific job type items
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
            jobParams[job] = [str(firstEvent), str(lastJobsNumberOfEvents)]
            common.jobDB.setArguments(job, jobParams[job])
            firstEvent += self.job_number_of_events
            
            # this is the last job
            #                lastJobsNumberOfEvents = (self.total_number_of_events + self.first_event) - firstEvent
            status = common.jobDB.status(njobs - 1)
            jobParams[njobs - 1] = [str(firstEvent), str(lastJobsNumberOfEvents)]
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
            eventPerJob = self.cfg_params['FAMOS.job_number_of_events']
        except KeyError:
            pass
        
        jobsPerTask=0
        try:
            jobsPerTask = int(self.cfg_params['FAMOS.total_number_of_jobs'])
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
        Fill the DB with proper entries for FAMOS
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
        txt = self.wsSetupCMSEnvironment_()

        # Prepare JobType-specific part
        scram = self.scram.commandName()
        txt += '\n\n'
        txt += 'echo "### SPECIFIC JOB SETUP ENVIRONMENT ###"\n'
        txt += scram+' project FAMOS '+self.version+'\n'
        txt += 'status=$?\n'
        txt += 'if [ $status != 0 ] ; then\n'
        txt += '   echo "SET_EXE_ENV 1 ==>ERROR FAMOS '+self.version+' not found on `hostname`" \n'
        txt += '   echo "JOB_EXIT_STATUS = 10034"\n'
        txt += '   echo "JobExitCode=10034" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '   dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '   rm -f $RUNTIME_AREA/$repo \n'
        txt += '   echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '   echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '   exit 1\n'
        txt += 'fi \n'
        txt += 'echo "FAMOS_VERSION =  '+self.version+'"\n'
        txt += 'cd '+self.version+'\n'
        ### needed grep for bug in scramv1 ###
        #txt += 'eval `'+scram+' runtime -sh | grep -v SCRAMRT_LSB_JOBNAME`\n'

        # Handle the arguments:
        txt += "\n"
        txt += "## ARGUMENTS: $1 Job Number\n"
        txt += "## ARGUMENTS: $2 First Event for this job\n"
        txt += "## ARGUMENTS: $3 Max Event for this job\n"
        txt += "\n"
        txt += "narg=$#\n"
        txt += "if [ $narg -lt 3 ]\n"
        txt += "then\n"
        txt += "    echo 'SET_EXE_ENV 1 ==> ERROR Too few arguments' +$narg+ \n"
        txt += '    echo "JOB_EXIT_STATUS = 50113"\n'
        txt += '    echo "JobExitCode=50113" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    rm -f $RUNTIME_AREA/$repo \n'
        txt += '    echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += "    exit 1\n"
        txt += "fi\n"
        txt += "\n"
        txt += "NJob=$1\n"
        txt += "FirstEvent=0\n"
        txt += "MaxEvents=$3\n"

#        if int(self.copy_input_data) == 1:

#print "input_pu_files : ", self.input_pu_files
#print "orcarc_pu_files : ", self.orcarc_pu_files

        txt += '\n'
        txt += 'input_file_list="'+string.join(self.in_file_list)+' "\n'
        txt += 's="'+string.join(self.cmkin_file_list)+' "\n'
        txt += '\n'
        txt += 'cur_pu_list="'+string.join(self.input_pu_files)+' "\n'
        txt += 'orcarc_pu_list="'+string.join(self.orcarc_pu_files)+' "\n'
        txt += '\n'
        txt += 'ntj="'+str(self.ntj)+' " \n'
        txt += 'let "imin=$NJob*$ntj - $ntj + 1" \n'
        txt += 'let "imax=$imin + $ntj - 1" \n'
        txt += 'cur_file_list=`echo $input_file_list |cut -d" " -f $imin-$imax` \n'
        txt += 'cur_cmkin_file_list=`echo $s |cut -d" " -f $imin-$imax` \n'

#        txt +='input_lfn='+q[0]+'\n'
        txt += 'input_lfn='+self.username+'\n'
        txt += 'pu_lfn='+self.usernamepu+'\n'
### georgia

        # Prepare job-specific part
        job = common.job_list[nj]
        orcarc = os.path.basename(job.configFilename())
        txt += '\n'
        txt += 'cp $RUNTIME_AREA/'+orcarc+' .orcarc\n'
        txt += 'if [ -e $RUNTIME_AREA/orcarc_$CE ] ; then\n'
        txt += '  cat $RUNTIME_AREA/orcarc_$CE .orcarc >> .orcarc_tmp\n'
        txt += '  mv .orcarc_tmp .orcarc\n'
        txt += 'fi\n'
        txt += '\n'

        if len(self.additional_inbox_files) > 0:
            for file in self.additional_inbox_files:
                file = os.path.basename(file)
                txt += 'if [ -e $RUNTIME_AREA/'+file+' ] ; then\n'
                txt += '   cp $RUNTIME_AREA/'+file+' .\n'
                txt += '   chmod +x '+file+'\n'
                txt += 'fi\n'
                txt += 'pwd \n'
                txt += 'ls -al \n'
            pass 

## changed by georgia
        txt += 'echo "FirstEvent=$FirstEvent" >> .orcarc\n'
        txt += 'echo "MaxEvents=$MaxEvents" >> .orcarc\n'
        txt += 'echo "CMKIN:File=$cur_cmkin_file_list" >> .orcarc\n'
        txt += 'echo "InputCollections=/Fake/fake/fake/fake" >> .orcarc\n'
## added for pile-up ntuples (georgia)
        txt += 'for file in $orcarc_pu_list \n'
        txt += 'do \n'
        txt += '  echo "PUGenerator:PUCollection=$file" >> .orcarc\n'
        txt += 'done \n'
        
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
            txt += '   echo "SET_EXE 1 ==> ERROR Untarring .tgz file failed"\n'
            txt += '   echo "JOB_EXIT_STATUS = $untar_status" \n'
            txt += '   echo "SanityCheckCode = $untar_status" | tee -a $repo\n'
            txt += '   exit 1 \n'
            txt += 'else \n'
            txt += '   echo "Successful untar" \n'
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
            txt += 'ls '+fileWithSuffix+'\n'
            txt += 'exe_result=$?\n'
            txt += 'if [ $exe_result -ne 0 ] ; then\n'
            txt += '   echo "ERROR: No output file to manage"\n'
            txt += '   echo "JOB_EXIT_STATUS = $exe_result"\n'
            txt += '   echo "JobExitCode=60302" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '   dumpStatus $RUNTIME_AREA/$repo\n'
            txt += '   rm -f $RUNTIME_AREA/$repo \n'
            txt += '   echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
            txt += '   echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '   exit 1 \n'
            txt += 'else\n'
            txt += '   cp '+fileWithSuffix+' $RUNTIME_AREA/'+output_file_num+'\n'
            txt += 'fi\n'
            pass
       
        txt += 'cd $RUNTIME_AREA\n'
        file_list=file_list[:-1]
        txt += 'file_list="'+file_list+'"\n'
        return txt

    def executableName(self):
        if self.scriptExe != '':
            return "./" + os.path.basename(self.scriptExe)
        else:
            return self.executable


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
        wordRemove=['CMKIN:File', 'InputCollections', 'FirstEvent', 'MaxEvents', 'PUGenerator:PUCollection']
        for line in inline:
            word = string.strip(string.split(line,'=')[0])

            if word not in wordRemove:
                outfile.write(line)
            else:
                continue
            pass

        outfile.write('\n\n##### The following cards have been created by CRAB: DO NOT TOUCH #####\n')

        outfile.write('MonRecAlisaBuilder=false\n')

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
        ## config
        inp_box.append(common.job_list[nj].configFilename())
        inp_box = inp_box + self.additional_inbox_files
        #print "inp_box = ", inp_box
        return inp_box

    ### and of output_sandbox
    def outputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL output sandbox.
        """
        out_box = []

        stdout=common.job_list[nj].stdout()
        stderr=common.job_list[nj].stderr()

        ## User Declared output files
        for out in self.output_file:
            n_out = nj + 1 
            #FEDE 
            #out_box.append(self.version+'/'+self.numberFile_(out,str(n_out)))
            out_box.append(self.numberFile_(out,str(n_out)))
        return out_box

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
          result = name + '_' + str(txt) + "." + ext
        # result = name + '_' + txt + "." + ext
        else:
          #result = name + '_' + str(txt)
          result = name + '_' + txt
        
        return result

    def getRequirements(self):
        """
       return job requirements to add to jdl files 
        """
        req = ''
        if common.analisys_common_info['sw_version']:
            req='Member("VO-cms-' + \
                 common.analisys_common_info['sw_version'] + \
                 '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'
        return req

    # georgia for FAMOS
     # marco

    def configFilename(self):
        """ return the config filename """
        print  self.name()+'.orcarc'
        return self.name()+'.orcarc'
               

    def stdOut(self):
        return self.stdOut_

    def stdErr(self):
        return self.stdErr_

    def setParam_(self, param, value):
        self._params[param] = value

    def getParams(self):
        return self._params

    def setTaskid_(self):
        self._taskId = self.cfg_params['user'] + '_' + string.split(common.work_space.topDir(),'/')[-2] 
        
    def getTaskid(self):
        return self._taskId
    # marco
