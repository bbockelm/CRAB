from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import PubDB
import orcarcBuilder
import orcarcBuilderOld
import Scram

import os, string, re

class Famos(JobType):
    def __init__(self, cfg_params):
        JobType.__init__(self, 'FAMOS')
        common.logger.debug(3,'FAMOS::__init__')

        self.analisys_common_info = {}

        log = common.logger
        
        self.scram = Scram.Scram(cfg_params)
        scramArea = ''
        self.additional_inbox_files = []
        self.scriptExe = ''

        self.version = self.scram.getSWVersion()
        common.analisys_common_info['sw_version'] = self.version

        try: self.VO = cfg_params['EDG.virtual_organization']
        except KeyError: self.VO = 'cms'
        try:
            self.executable = cfg_params['FAMOS.executable']
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

        try:
            self.total_number_of_events = int(cfg_params['USER.total_number_of_events'])
            self.maxEvents=self.total_number_of_events
            log.debug(6, "Famos::Famos(): total number of events = "+`self.total_number_of_events`)
        except KeyError:
            msg = 'Must define total_number_of_events and job_number_of_events'
            raise CrabException(msg)

        try:
            self.first = int(cfg_params['USER.first_event'])
            log.debug(6, "Famos::Famos(): first event = "+`self.first`)
        except KeyError:
            self.first = 0
            pass
        # [-- self.checkNevJobs() --]

        try:
            self.tgzNameWithPath = self.scram.getTarBall(self.executable)
        except KeyError:
            msg = 'Sth wrong with self.scram.getTarBall(self.executable)'
            raise CrabException(msg)

        try:
            self.ML = int(cfg_params['USER.activate_monalisa'])
        except KeyError:
            self.ML = 0
            pass
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
        txt += '   echo "JOB_EXIT_STATUS = 5"\n'
        txt += '   echo "SanityCheckCode = 5" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '   dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '   exit 5 \n'
        txt += 'fi \n'
        txt += 'echo "FAMOS_VERSION =  '+self.version+'"\n'
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
        txt += '    echo "JOB_EXIT_STATUS = 1"\n'
        txt += '    echo "SanityCheckCode = 1" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += "    exit 1\n"
        txt += "fi\n"
        txt += "\n"
        txt += "NJob=$1\n"
        txt += "FirstEvent=$2\n"
        txt += "MaxEvents=$3\n"

#        if int(self.copy_input_data) == 1:

        in_file = self.input_lfn
        p = string.split(in_file,".")
        ext = p[len(p)-1]
        q = string.split(p[0],"/")
        name= q[len(q)-1]
        txt +='the_ntuple='+name+'_$NJob.'+ext+'\n'
        txt +='input_lfn='+q[0]+'/'+'$the_ntuple\n'
        txt +='echo "$input_lfn" \n'
        txt +='echo "$the_ntuple" \n'

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

        txt += 'echo "FirstEvent=$FirstEvent" >> .orcarc\n'
        txt += 'echo "MaxEvents=$MaxEvents" >> .orcarc\n'
        txt += 'echo "CMKIN:File=`pwd`/$the_ntuple" >> .orcarc\n'
        txt += 'echo "InputCollections=/Fake/fake/fake/fake" >> .orcarc\n'
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
            txt += 'echo "tar xzvf ../'+os.path.basename(self.tgzNameWithPath)+'"\n'
            txt += 'tar xzvf ../'+os.path.basename(self.tgzNameWithPath)+'\n'
            txt += 'untar_status=$? \n'
            txt += 'if [ $untar_status -ne 0 ]; then \n'
            txt += '   echo "SET_EXE 1 ==> ERROR Untarring .tgz file failed"\n'
            txt += '   echo "JOB_EXIT_STATUS = $untar_status" \n'
            txt += '   echo "SanityCheckCode = $untar_status" | tee -a $repo\n'
            txt += '   exit $untar_status \n'
            txt += 'else \n'
            txt += '   echo "Successful untar" \n'
            txt += 'fi \n'
            # TODO: what does this code do here ?
            # SL check that lib/Linux__... is present
            txt += 'mkdir -p lib/${SCRAM_ARCH} \n'
            txt += 'eval `'+self.scram.commandName()+' runtime -sh |grep -v SCRAMRT_LSB_JOBNAME`'+'\n'
            pass
        
        return txt

    def wsRenameOutput(self, nj):
        """
        Returns part of a job script which renames the produced files.
        """

        txt = '\n'
        file_list = ''
        for fileWithSuffix in self.output_file:
            output_file_num = self.numberFile_(fileWithSuffix, '$NJob')
            file_list=file_list+output_file_num+','
            txt += 'ls '+fileWithSuffix+'\n'
            txt += 'exe_result=$?\n'
            txt += 'if [ $exe_result -ne 0 ] ; then\n'
            txt += '   echo "ERROR: No output file to manage"\n'
            txt += '   echo "JOB_EXIT_STATUS = $exe_result"\n'
            txt += '   echo "SanityCheckCode = $exe_result" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '   dumpStatus $RUNTIME_AREA/$repo\n'
            txt += '   exit $exe_result \n'
            txt += 'else\n'
            txt += '   cp '+fileWithSuffix+' $RUNTIME_AREA/'+output_file_num+'\n'
            txt += 'fi\n'
            txt += 'cd $RUNTIME_AREA\n'
                      
            pass
       
        file_list=file_list[:-1]
        txt += 'file_list='+file_list+'\n'
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
        wordRemove=['CMKIN:File', 'InputCollections', 'FirstEvent', 'MaxEvents']
        for line in inline:
            word = string.strip(string.split(line,'=')[0])

            if word not in wordRemove:
                outfile.write(line)
            else:
                continue
            pass

        outfile.write('\n\n##### The following cards have been created by CRAB: DO NOT TOUCH #####\n')

        if (self.ML) :
            outfile.write('MonalisaAddPid = false\n')
            outfile.write('ExtraPackages=RecApplPlugins\n')
            outfile.write('MonRecAlisaBuilder=true\n')
            ## TaskId is username+crab_0_date_time : that should be unique
            TaskID = os.getlogin()+'_'+string.split(common.work_space.topDir(),'/')[-2]
            outfile.write('MonalisaApplName='+TaskID+'\n')
            outfile.write('MonalisaNode=192.91.245.5\n')
            outfile.write('MonalisaPort=58884\n')
            pass


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
        #print "req = ", req 
        return req

    def stdOut(self):
        return self.stdOut_

    def stdErr(self):
        return self.stdErr_
