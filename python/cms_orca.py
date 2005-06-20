from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import PubDB
import orcarcBuilder

import os, string, re

class Orca(JobType):
    def __init__(self, cfg_params):
        JobType.__init__(self, 'ORCA')

        self.analisys_common_info = {}

        try:
            self.build = cfg_params['USER.build_mode']
        except KeyError:
            self.build = 'tgz'
            pass
        
        self.tgz_name = 'default.tgz'
        self.tgzNameWithPath = None

        log = common.logger
        
        scramArea = ''
        try:
            scramArea = os.environ["LOCALRT"]
        except KeyError:
            msg = self.name()+' job type cannot be created:\n'
            msg += '  LOCALRT env variable not set\n'
            msg += '  Did you do eval `scram runtime ...` from your ORCA area ?\n'
            raise CrabException(msg)
        log.debug(6, "Orca::Orca(): SCRAM area is "+scramArea)

        try:
            self.version = self.findSwVersion_(scramArea)
            log.debug(6, "Orca::Orca(): version = "+self.version)
            self.owner = cfg_params['USER.owner']
            log.debug(6, "Orca::Orca(): owner = "+self.owner)
            self.dataset = cfg_params['USER.dataset']
            log.debug(6, "Orca::Orca(): dataset = "+self.dataset)
            self.executable = cfg_params['USER.executable']
            log.debug(6, "Orca::Orca(): executable = "+self.executable)
            self.orcarc_file = cfg_params['USER.orcarc_file']
            log.debug(6, "Orca::Orca(): orcarc file = "+self.orcarc_file)

            # allow multiple output files

            self.output_file = []

            tmp = cfg_params['USER.output_file']
            if tmp != '':
                tmpOutFiles = string.split(cfg_params['USER.output_file'],',')
                log.debug(7, 'Orca::Orca(): output files '+str(tmpOutFiles))
                for tmp in tmpOutFiles:
                    tmp=string.strip(tmp)
                    self.output_file.append(tmp)
                    pass

                # output files with num added
                self.output_file_num = []
                for tmp in self.output_file:
                    self.output_file_num.append(tmp)
                    pass
                pass
            pass

        except KeyError:
            msg = self.name()+' job type cannot be created:\n'
            msg = msg + '   not all mandatory fields present in the'\
                  ' [USER] section.\n   List of mandatory fields:\n'
            msg = msg + \
                  'USER.owner\n'+\
                  'USER.dataset\n'+\
                  'USER.executable\n'+\
                  'USER.orcarc_file\n'+\
                  'USER.output_file\n'
            raise CrabException(msg)

        self.dataTiers = []
        try:
            tmpDataTiers = string.split(cfg_params['USER.data_tier'],',')
            for tmp in tmpDataTiers:
                tmp=string.strip(tmp)
                self.dataTiers.append(tmp)
                pass
            pass
        except KeyError:
            pass
        log.debug(6, "Orca::Orca(): data tiers = "+str(self.dataTiers))

        try:
            tmpAddFiles = string.split(cfg_params['USER.additional_input_files'],',')
            for tmp in tmpAddFiles:
                tmp=string.strip(tmp)
                common.additional_inbox_files.append(tmp)
                pass
            pass
        except KeyError:
            pass

        try:
            self.total_number_of_events = int(cfg_params['USER.total_number_of_events'])
            self.job_number_of_events = int(cfg_params['USER.job_number_of_events'])
        except KeyError:
            msg = 'Must define total_number_of_events and job_number_of_events'
            raise CrabException(msg)

        try:
            self.first = int(cfg_params['USER.first_event'])
        except KeyError:
            self.first = 0
            pass
        log.debug(6, "Orca::Orca(): total number of events = "+`self.total_number_of_events`)
        log.debug(6, "Orca::Orca(): events per job = "+`self.job_number_of_events`)
        log.debug(6, "Orca::Orca(): first event = "+`self.first`)
        
        self.maxEvents=0 # max events available in any PubDB
        self.connectPubDB()
          
        self.checkNevJobs()

        self.prepareTgz_()

        common.analisys_common_info = self.analisys_common_info # TODO: ???
        return

    def wsSetupEnvironment(self, nj):
        """
        Returns part of a job script which prepares
        the execution environment for the job 'nj'.
        """

        # Prepare JobType-independent part
        txt = self.wsSetupCMSEnvironment_()

        # Prepare JobType-specific part
        txt += '\n'
        txt += 'scram project ORCA '+self.version+'\n'
        txt += 'status=$?\n'
        txt += 'if [ $status != 0 ] ; then\n'
        txt += '   echo "Warning: ORCA '+self.version+' not found on `hostname`" \n'
        txt += '   exit 1 \n'
        txt += 'fi \n'
        txt += 'cd '+self.version+'\n'
        txt += 'eval `scram runtime -sh`\n'

        # Prepare job-specific part
        job = common.job_list[nj]
        orcarc = os.path.basename(job.configFilename())
        txt += '\n'
        txt += 'cp $RUNTIME_AREA/'+orcarc+' .orcarc\n'
        txt += 'if [ -e $RUNTIME_AREA/orcarc_$CE ] ; then\n'
        txt += '  cat $RUNTIME_AREA/orcarc_$CE .orcarc >> .orcarc_tmp\n'
        txt += '  mv .orcarc_tmp .orcarc\n'
        txt += '  cp $RUNTIME_AREA/init_$CE.sh init.sh\n'
        txt += 'fi\n'
        txt += 'echo "***** cat .orcarc *********"\n'
        txt += 'cat .orcarc\n'
        txt += 'echo "****** end .orcarc ********"\n'
        txt += '\n'
        txt += 'chmod +x ./init.sh\n'
        txt += './init.sh\n'
        txt += 'exitStatus=$?\n'
        txt += 'if [ $exitStatus != 0 ] ; then\n'
        txt += '  echo "StageIn init script failed!"\n'
        txt += '  exit $exitStatus\n'
        txt += 'fi\n'
        return txt
    
    def wsBuildExe(self, nj):
        """
        Put in the script the commands to build an executable
        or a library.
        """
        if self.build != 'tgz':
            msg = 'USER. Unknown build_mode '+self.build
            raise CrabException(msg)

        txt = ""

        if os.path.isfile(self.tgz):
            txt += 'echo "tar xzvf ../'+os.path.basename(self.tgz)+'"\n'
            txt += 'tar xzvf ../'+os.path.basename(self.tgz)+'\n'
            txt += 'untar_status=$? \n'
            txt += 'if [ $untar_status -ne 0 ]; then \n'
            txt += '   echo "Untarring .tgz file failed ... exiting" \n'
            txt += '   exit 1 \n'
            txt += 'else \n'
            txt += '   echo "Successful untar" \n'
            txt += 'fi \n'
            # TODO: what does this code do here ?
            # SL check that lib/Linux__... is present
            txt += 'mkdir -p lib/Linux__2.4 \n'
            txt += 'eval `scram runtime -sh`'+'\n'
            pass

        return txt

    def wsRenameOutput(self, nj):
        """
        Returns part of a job script which renames the produced files.
        """
        txt = '\n'
        for i in range(len(self.output_file)):
            txt += 'cp '+self.output_file[i]+' '+self.output_file_num[i]+'\n'
            pass
        return txt

    def executableName(self):
        return self.executable

    def checkNevJobs(self):
        """Check the number of jobs and num events per job"""
        
        # check if total_number_of_events==-1
        if self.total_number_of_events==-1:
            self.total_number_of_events=int(self.maxEvents)
        if self.total_number_of_events==0:
            msg = 'Max events available is '+str(self.total_number_of_events)
            raise CrabException(msg)


        # Check if job_number_of_events>total_number_of_events, in case warning and set =
        if self.job_number_of_events>self.total_number_of_events:
            msg='Asking '+str(self.job_number_of_events)+' per job but only '+str(self.total_number_of_events)+' in total: '
            msg=msg+'setting job_number_of_events to '+str(self.total_number_of_events)
            common.logger.message(msg)
            self.job_number_of_events=self.total_number_of_events
            pass

        return

    def connectPubDB(self):

        fun = "Orca::connectPubDB()"
        
        self.allOrcarcs = []
        # first check if the info from PubDB have been already processed
        if os.path.exists(common.work_space.shareDir()+'PubDBSummaryFile') :
            common.logger.debug(6, fun+": info from PubDB has been already processed -- use it")
            f = open( common.work_space.shareDir()+'PubDBSummaryFile', 'r' )
            for i in f.readlines():
                a=string.split(i,' ')
                self.allOrcarcs.append(orcarcBuilder.constructFromFile(a[0:-1]))
                pass
            for o in self.allOrcarcs:
                # o.dump()
                if o.Nevents >= self.maxEvents:
                    self.maxEvents= o.Nevents
                    pass
                pass
            pass

        else:  # PubDB never queried
            common.logger.debug(6, fun+": PubDB was never queried -- do it")
            # New PubDB class by SL
            try:
                self.pubdb = PubDB.PubDB(self.owner,
                                         self.dataset,
                                         self.dataTiers)
            except PubDB.RefDBError:
                msg = 'ERROR ***: accessing PubDB'
                raise CrabException(msg)

            self.pubDBResults = self.pubdb.getAllPubDBsInfo()
            if len(self.pubDBResults)==0:
                msg = 'Owner Dataset not published with asked dataTiers! '+\
                      self.owner+' '+ self.dataset+' '+self.dataTiers
                raise CrabException(msg)

            common.logger.debug(6, fun+": PubDB info ("+`len(self.pubDBResults)`+"):\n")
            for aa in self.pubDBResults:
                for bb in aa:
                    common.logger.debug(6, str(bb))
                    pass
                pass
            common.logger.debug(6, fun+": End of PubDB info\n")

            self.builder = orcarcBuilder.orcarcBuilder()

            currDir = os.getcwd()
            os.chdir(common.work_space.jobDir())
            tmpAllOrcarcs = self.builder.createOrcarcAndInit(self.pubDBResults)
            os.chdir(currDir)

            self.maxEvents=0
            for o in tmpAllOrcarcs:
                numEvReq=self.total_number_of_events
                if ((numEvReq == '-1') | (numEvReq <= o.Nevents)):
                    self.allOrcarcs.append(o)
                    if o.Nevents >= self.maxEvents:
                        self.maxEvents= o.Nevents
                        pass
                    pass
                pass
          
            # set maximum number of event available

            # I save to a file self.allOrcarcs
          
            PubDBSummaryFile = open(common.work_space.shareDir()+'PubDBSummaryFile','w')
            for o in self.allOrcarcs:
                for d in o.content():
                    PubDBSummaryFile.write(d)
                    PubDBSummaryFile.write(' ')
                    pass
                PubDBSummaryFile.write('\n')
                pass
            PubDBSummaryFile.close()

            # for o in self.allOrcarcs:
            #   o.dump()
            pass

        # build a list of sites
        ces= []
        for o in self.allOrcarcs:
            ces.append(o.CE)
            pass

        if len(ces)==0:
            msg = 'No PubDBs publish enough events! '
            msg += `self.total_number_of_events`
            raise CrabException(msg)

        common.logger.debug(6, "List of CEs: "+str(ces))
        self.analisys_common_info['sites']=ces

        return

    def findSwVersion_(self, path):
        """ Find Sw version using proper scram env """
        scramEnvFile = path+"/.SCRAM/Environment"
        reVer = re.compile(r'SCRAM_PROJECTVERSION')

        try:
            for line in open(scramEnvFile, "r").readlines():
                if reVer.match(line):
                    ver = string.strip(string.split(line, '=')[1])
                    return ver
                pass
            pass
        except IOError, e:
            msg = 'Cannot find SCRAM project version:\n'
            msg += str(e)
            raise CrabException(msg)
        return
        
    def prepareTgz_(self):
 
        self.tgzNameWithPath = common.work_space.shareDir()+self.tgz_name

        if os.path.exists(self.tgzNameWithPath):
            scramArea = os.environ["LOCALRT"]
            n = len(string.split(scramArea, '/'))
            swVersion = self.findSwVersion_(scramArea)
            self.analisys_common_info['sw_version'] = swVersion
            return

        # Prepare a tar gzipped file with user binaries.

        # First of all declare the user Scram area
        try:
          scramArea = os.environ["LOCALRT"]
          #print scramArea,'\n'
          n = len(string.split(scramArea, '/'))
          swVersion = self.findSwVersion_(scramArea)
          #print swVersion,'\n'
          self.analisys_common_info['sw_version'] = swVersion
          swArea = os.environ["CMS_PATH"]
          #print swArea,'\n'
  #        swArea = "/opt/exp_software/cms"
          if scramArea.find(swArea) == -1:
              # Calculate length of scram dir to extract software version
              crabArea = os.getcwd()
              os.chdir(scramArea)

## SL use SCRAM_ARCH instead of Linux__2.4
              if os.environ.has_key('SCRAM_ARCH'):
                scramArch = os.environ["SCRAM_ARCH"]
              else:
                scramArch = 'Linux__2.4'
              #print scramArch,'\n'
        #      libDir = 'lib/'+scramArch+'/'
              libDir = ''
              binDir = 'bin/'+scramArch+'/'


              exe = binDir + self.executable
### here assume that executable is in user directory
              #print '##'+exe+'##\n'
              if os.path.isfile(exe):
                  cmd = 'ldd ' + exe + ' | grep ' + scramArea
                  myCmd = os.popen(cmd)
                  tarcmd = 'tar zcvf ' + self.tgzNameWithPath + ' ' 
                  for line in (myCmd):
                      line = string.split(line, '=>')
                      lib = 'lib/' + scramArch + '/' + string.strip(line[0]) + ' '
                      tarcmd = tarcmd + lib
                  status = myCmd.close()
                  tarcmd = tarcmd + ' ' + exe
                  cout = runCommand(tarcmd)
              else:
### If not, check if it's standard

                cmd = 'which ' + self.executable
                myCmd = os.popen(cmd)
                exe = string.strip(myCmd.readline()[0:-1])
                #print '##'+exe+'##\n'
                status = myCmd.close()
                if not os.path.isfile(exe):
                  raise common.InternalError(__name__,'prepareTgz','User executable not found')
                #os.system('touch '+common.tgzNameWithPath)
              os.chdir(crabArea)
          else:
              cmd = 'which ' + self.executable
              cout = runCommand(cmd)
              if not cout:
                  raise common.InternalError(__name__,'prepareTgz','Executable not found')
        except KeyError:
          raise common.InternalError(__name__,'prepareTgz','Undefined SCRAM area')
        pass

    def nJobs(self):
        # TODO: should not be here !
        # JobType should have no internal knowledge about submitted jobs
        # One possibility is to use len(common.job_list).
        """ return the number of job to be created """
        return int((self.total_number_of_events-1)/self.job_number_of_events)+1
    
    def modifySteeringCards(self, nj):
        # add jobs information to the orcarc card, 
        # starting from card into share dir 
        """
        Creates steering cards file modifying a template file
        taken from RefDB or given by user.
        """
        infile =  open(common.work_space.shareDir()+self.cardsBaseName(), 'r')
        outfile = open(common.job_list[nj].configFilename(),'w')  
        ### job splitting      
        Nev_job = self.job_number_of_events
        outfile.write('InputCollections=/System/'+self.owner+'/'+self.dataset+'/'+self.dataset+'\n')
        outfile.write('FirstEvent = '+ str(self.first + (nj*Nev_job)) +'\n')
        
        if nj==(self.nJobs()-1):
          Nev_job=self.total_number_of_events - (self.first + (nj*Nev_job))

        outfile.write('MaxEvents = '+str(Nev_job)+'\n')

        if len(self.output_file)>0 :
          for i in range(len(self.output_file)): 
            p = string.split(self.output_file[i],".")
            file = p[0]
            for x in p[1:-1]:
               file=file+"."+x
            if len(p)>1:
              ext = p[len(p)-1]
              self.output_file_num[i] = file + "_" +str(nj + 1) + "." + ext
            else:
              self.output_file_num[i] = file + "_" +str(nj + 1)

        outfile.write(infile.read())
        outfile.close()
        return

    def cardsBaseName(self):
        """
        Returns name of user orcarc card-file
        """
        return os.path.split (self.orcarc_file)[1]

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
          runCommand(cmd,0)
          infile = open(self.orcarc_file,'r')
            
        outfile = open(common.work_space.shareDir()+self.cardsBaseName(), 'w')
           
        inline=infile.readlines()
        ### remove from user card these lines ###
        for i in range (len(inline)):
           if string.find(inline[i], 'InputFileCatalogURL') == -1:  
              if string.find(inline[i], 'InputCollections') == -1:
                 if string.find(inline[i], 'FirstEvent') == -1: 
                    if string.find(inline[i], 'MaxEvents') == -1: 
                       outfile.write(inline[i])
           else:
              continue
        infile.close()
        outfile.close()
        return

### content of input_sanbdox ...
    def inputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL input sandbox.
        """
        inp_box = []
        self.tgz = self.tgzNameWithPath
        if os.path.isfile(self.tgz):
            inp_box.append(self.tgz)
        #else:
           #print 'tgz not found!!!'
        for o in self.allOrcarcs:
          for f in o.fileList():
            inp_box.append(common.work_space.jobDir()+f)
        inp_box.append(common.job_list[nj].configFilename())
        return inp_box

### and of output_sandbox
    def outputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL output sandbox.
        """
        out_box = []

        if len(self.output_file_num)>0 :
          for out in self.output_file_num:
            out_box.append(self.version+'/'+out)
        return out_box
