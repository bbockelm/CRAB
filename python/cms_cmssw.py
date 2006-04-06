from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common

import DataDiscovery
import DataLocation
import Scram

import os, string, re

class Cmssw(JobType):
    def __init__(self, cfg_params):
        JobType.__init__(self, 'CMSSW')
        common.logger.debug(3,'CMSSW::__init__')

        self.analisys_common_info = {}

        log = common.logger
        
        self.scram = Scram.Scram(cfg_params)
        scramArea = ''
        self.additional_inbox_files = []
        self.scriptExe = ''
        self.executable = ''
        self.tgz_name = 'default.tgz'

        self.version = self.scram.getSWVersion()
        common.analisys_common_info['sw_version'] = self.version

        ### collect Data cards
        try:
            self.owner = cfg_params['CMSSW.owner']
            log.debug(6, "CMSSW::CMSSW(): owner = "+self.owner)
            self.dataset = cfg_params['CMSSW.dataset']
            log.debug(6, "CMSSW::CMSSW(): dataset = "+self.dataset)
        except KeyError:
            msg = "Error: owner and/or dataset not defined "
            raise CrabException(msg)

        self.dataTiers = []
        try:
            tmpDataTiers = string.split(cfg_params['CMSSW.data_tier'],',')
            for tmp in tmpDataTiers:
                tmp=string.strip(tmp)
                self.dataTiers.append(tmp)
                pass
            pass
        except KeyError:
            pass
        log.debug(6, "Cmssw::Cmssw(): dataTiers = "+str(self.dataTiers))

        ## now the application
        try:
            self.executable = cfg_params['CMSSW.executable']
            log.debug(6, "CMSSW::CMSSW(): executable = "+self.executable)
            msg = "Default executable cmsRun overridden. Switch to " + self.executable
            log.debug(3,msg)
        except KeyError:
            self.executable = 'cmsRun'
            msg = "User executable not defined. Use cmsRun"
            log.debug(3,msg)
            pass

        try:
            self.pset = cfg_params['CMSSW.pset']
            log.debug(6, "Cmssw::Cmssw(): PSet file = "+self.pset)
            if (not os.path.exists(self.pset)):
                raise CrabException("User defined PSet file "+self.pset+" does not exist")
        except KeyError:
            raise CrabException("PSet file missing. Cannot run cmsRun ")

        # output files
        try:
            self.output_file = []

            tmp = cfg_params['CMSSW.output_file']
            if tmp != '':
                tmpOutFiles = string.split(cfg_params['CMSSW.output_file'],',')
                log.debug(7, 'cmssw::cmssw(): output files '+str(tmpOutFiles))
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
           self.scriptExe = cfg_params['CMSSW.script_exe']
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
            tmpAddFiles = string.split(cfg_params['CMSSW.additional_input_files'],',')
            for tmp in tmpAddFiles:
                tmp=string.strip(tmp)
                self.additional_inbox_files.append(tmp)
                pass
            pass
        except KeyError:
            pass

        try:
            self.total_number_of_events = int(cfg_params['CMSSW.total_number_of_events'])
        except KeyError:
            msg = 'Must define total_number_of_events and job_number_of_events'
            raise CrabException(msg)
            
#Marco: FirstEvent is nolonger used inside PSet
#        try:
#            self.first = int(cfg_params['CMSSW.first_event'])
#        except KeyError:
#            self.first = 0
#            pass
#        log.debug(6, "Orca::Orca(): total number of events = "+`self.total_number_of_events`)
        #log.debug(6, "Orca::Orca(): events per job = "+`self.job_number_of_events`)
#        log.debug(6, "Orca::Orca(): first event = "+`self.first`)
        
        CEBlackList = []
        try:
            tmpBad = string.split(cfg_params['EDG.ce_black_list'],',')
            for tmp in tmpBad:
                tmp=string.strip(tmp)
                CEBlackList.append(tmp)
        except KeyError:
            pass

        self.reCEBlackList=[]
        for bad in CEBlackList:
            self.reCEBlackList.append(re.compile( bad ))

        common.logger.debug(5,'CEBlackList: '+str(CEBlackList))

        CEWhiteList = []
        try:
            tmpGood = string.split(cfg_params['EDG.ce_white_list'],',')
            #tmpGood = ['cern']
            for tmp in tmpGood:
                tmp=string.strip(tmp)
                #if (tmp == 'cnaf'): tmp = 'webserver' ########## warning: temp. patch
                CEWhiteList.append(tmp)
        except KeyError:
            pass

        #print 'CEWhiteList: ',CEWhiteList
        self.reCEWhiteList=[]
        for Good in CEWhiteList:
            self.reCEWhiteList.append(re.compile( Good ))

        common.logger.debug(5,'CEWhiteList: '+str(CEWhiteList))

        #DBSDLS-start
        ## Initialize the variables that are extracted from DBS/DLS and needed in other places of the code 
        self.maxEvents=0  # max events available   ( --> check the requested nb. of evts in Creator.py)
        self.DBSPaths={}  # all dbs paths requested ( --> input to the site local discovery script)
        ## Perform the data location and discovery (based on DBS/DLS)
        self.DataDiscoveryAndLocation(cfg_params)
        #DBSDLS-end          

        self.tgzNameWithPath = self.getTarBall(self.executable)

    def DataDiscoveryAndLocation(self, cfg_params):

        fun = "CMSSW::DataDiscoveryAndLocation()"

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
        common.logger.debug(10,"number of events for primary fileblocks %i"%self.pubdata.getMaxEvents())
        self.maxEvents=self.pubdata.getMaxEvents() ##  self.maxEvents used in Creator.py 
        common.logger.message("\nThe number of available events is %s"%self.maxEvents)

        ## get fileblocks corresponding to the required data
        fb=self.pubdata.getFileBlocks()
        common.logger.debug(5,"fileblocks are %s"%fb)

        ## Contact the DLS and build a list of sites hosting the fileblocks
        try:
            dataloc=DataLocation.DataLocation(self.pubdata.getFileBlocks(),cfg_params)
            dataloc.fetchDLSInfo()
        except DataLocation.DataLocationError , ex:
            msg = 'ERROR ***: failed Data Location in DLS \n %s '%ex.getErrorMessage()
            raise CrabException(msg)
        
        allsites=dataloc.getSites()
        common.logger.debug(5,"sites are %s"%allsites)
        sites=self.checkBlackList(allsites)
        common.logger.debug(5,"sites are (after black list) %s"%sites)
        sites=self.checkWhiteList(sites)
        common.logger.debug(5,"sites are (after white list) %s"%sites)

        if len(sites)==0:
            msg = 'No sites hosting all the needed data! Exiting... '
            raise CrabException(msg)
        common.logger.message("List of Sites hosting the data : "+str(sites)) 
        common.logger.debug(6, "List of Sites: "+str(sites))
        common.analisys_common_info['sites']=sites    ## used in SchedulerEdg.py in createSchScript
        return
        
    def checkBlackList(self, allSites):
        if len(self.reCEBlackList)==0: return allSites
        sites = []
        for site in allSites:
            common.logger.debug(10,'Site '+site)
            good=1
            for re in self.reCEBlackList:
                if re.search(site):
                    common.logger.message('CE in black list, skipping site '+site)
                    good=0
                pass
            if good: sites.append(site)
        if len(sites) == 0:
            common.logger.debug(3,"No sites found after BlackList")
        return sites

    def checkWhiteList(self, allsites):

        if len(self.reCEWhiteList)==0: return pubDBUrls
        sites = []
        for site in allsites:
            #print 'connecting to the URL ',url
            good=0
            for re in self.reCEWhiteList:
                if re.search(site):
                    common.logger.debug(5,'CE in white list, adding site '+site)
                    good=1
                if not good: continue
                sites.append(site)
        if len(sites) == 0:
            common.logger.message("No sites found after WhiteList\n")
        else:
            common.logger.debug(5,"Selected sites via WhiteList are "+str(sites)+"\n")
        return sites

    def getTarBall(self, exe):
        """
        Return the TarBall with lib and exe
        """
        
        # if it exist, just return it
        self.tgzNameWithPath = common.work_space.shareDir()+self.tgz_name
        if os.path.exists(self.tgzNameWithPath):
            return self.tgzNameWithPath

        # Prepare a tar gzipped file with user binaries.
        self.buildTar_(exe)

        return string.strip(self.tgzNameWithPath)

    def buildTar_(self, executable):

        # First of all declare the user Scram area
        swArea = self.scram.getSWArea_()
        #print "swArea = ", swArea
        swVersion = self.scram.getSWVersion()
        #print "swVersion = ", swVersion
        swReleaseTop = self.scram.getReleaseTop_()
        #print "swReleaseTop = ", swReleaseTop
        
        ## check if working area is release top
        if swReleaseTop == '' or swArea == swReleaseTop:
            return

        filesToBeTarred = []
        ## First find the executable
        if (self.executable != ''):
            exeWithPath = self.scram.findFile_(executable)
#           print exeWithPath
            if ( not exeWithPath ):
                raise CrabException('User executable '+executable+' not found')
 
            ## then check if it's private or not
            if exeWithPath.find(swReleaseTop) == -1:
                # the exe is private, so we must ship
                common.logger.debug(5,"Exe "+exeWithPath+" to be tarred")
                path = swArea+'/'
                exe = string.replace(exeWithPath, path,'')
                filesToBeTarred.append(exe)
                pass
            else:
                # the exe is from release, we'll find it on WN
                pass
 
        ## Now get the libraries: only those in local working area
        libDir = 'lib'
        lib = swArea+'/' +libDir
        common.logger.debug(5,"lib "+lib+" to be tarred")
        if os.path.exists(lib):
            filesToBeTarred.append(libDir)
 
        ## Now check if the Data dir is present
        dataDir = 'src/Data/'
        if os.path.isdir(swArea+'/'+dataDir):
            filesToBeTarred.append(dataDir)
 
        ## Create the tar-ball
        if len(filesToBeTarred)>0:
            cwd = os.getcwd()
            os.chdir(swArea)
            tarcmd = 'tar zcvf ' + self.tgzNameWithPath + ' ' 
            for line in filesToBeTarred:
                tarcmd = tarcmd + line + ' '
            cout = runCommand(tarcmd)
            if not cout:
                raise CrabException('Could not create tar-ball')
            os.chdir(cwd)
        else:
            common.logger.debug(5,"No files to be to be tarred")
        
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
        txt += scram+' project CMSSW '+self.version+'\n'
        txt += 'status=$?\n'
        txt += 'if [ $status != 0 ] ; then\n'
        txt += '   echo "SET_EXE_ENV 1 ==>ERROR CMSSW '+self.version+' not found on `hostname`" \n'
        txt += '   echo "JOB_EXIT_STATUS = 5"\n'
        txt += '   echo "SanityCheckCode = 5" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '   dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '   exit 5 \n'
        txt += 'fi \n'
        txt += 'echo "CMSSW_VERSION =  '+self.version+'"\n'
        txt += 'cd '+self.version+'\n'
        ### needed grep for bug in scramv1 ###
        txt += 'eval `'+scram+' runtime -sh | grep -v SCRAMRT_LSB_JOBNAME`\n'

        # Handle the arguments:
        txt += "\n"
        txt += "## ARGUMNETS: $1 Job Number\n"
        # txt += "## ARGUMNETS: $2 First Event for this job\n"
        # txt += "## ARGUMNETS: $3 Max Event for this job\n"
        txt += "\n"
        txt += "narg=$#\n"
        txt += "if [ $narg -lt 1 ]\n"
        txt += "then\n"
        txt += "    echo 'SET_EXE_ENV 1 ==> ERROR Too few arguments' +$narg+ \n"
        txt += '    echo "JOB_EXIT_STATUS = 1"\n'
        txt += '    echo "SanityCheckCode = 1" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += "    exit 1\n"
        txt += "fi\n"
        txt += "\n"
        txt += "NJob=$1\n"
        # txt += "FirstEvent=$2\n"
        # txt += "MaxEvents=$3\n"

        # Prepare job-specific part
        job = common.job_list[nj]
        pset = os.path.basename(job.configFilename())
        txt += '\n'
        txt += 'cp $RUNTIME_AREA/'+pset+' pset.cfg\n'
        # txt += 'if [ -e $RUNTIME_AREA/orcarc_$CE ] ; then\n'
        # txt += '  cat $RUNTIME_AREA/orcarc_$CE .orcarc >> .orcarc_tmp\n'
        # txt += '  mv .orcarc_tmp .orcarc\n'
        # txt += 'fi\n'
        # txt += 'if [ -e $RUNTIME_AREA/init_$CE.sh ] ; then\n'
        # txt += '  cp $RUNTIME_AREA/init_$CE.sh init.sh\n'
        # txt += 'fi\n'

        if len(self.additional_inbox_files) > 0:
            for file in self.additional_inbox_files:
                txt += 'if [ -e $RUNTIME_AREA/'+file+' ] ; then\n'
                txt += '   cp $RUNTIME_AREA/'+file+' .\n'
                txt += '   chmod +x '+file+'\n'
                txt += 'fi\n'
            pass 

        # txt += '\n'
        # txt += 'chmod +x ./init.sh\n'
        # txt += './init.sh\n'
        # txt += 'exitStatus=$?\n'
        # txt += 'if [ $exitStatus != 0 ] ; then\n'
        # txt += '  echo "SET_EXE_ENV 1 ==> ERROR StageIn init script failed"\n'
        # txt += '  echo "JOB_EXIT_STATUS = $exitStatus" \n'
        # txt += '  echo "SanityCheckCode = $exitStatus" | tee -a $RUNTIME_AREA/$repo\n'
        # txt += '  dumpStatus $RUNTIME_AREA/$repo\n'
        # txt += '  exit $exitStatus\n'
        # txt += 'fi\n'
        # txt += "echo 'SET_EXE_ENV 0 ==> job setup ok'\n"
        txt += 'echo "### END JOB SETUP ENVIRONMENT ###"\n\n'

        # txt += 'echo "FirstEvent=$FirstEvent" >> .orcarc\n'
        # txt += 'echo "MaxEvents=$MaxEvents" >> .orcarc\n'
        # if self.ML:
        #     txt += 'echo "MonalisaJobId=$NJob" >> .orcarc\n'

        txt += '\n'
        txt += 'echo "***** cat pset.cfg *********"\n'
        txt += 'cat pset.cfg\n'
        txt += 'echo "****** end pset.cfg ********"\n'
        return txt

    def modifySteeringCards(self, nj):
        """
        modify the card provided by the user, 
        writing a new card into share dir
        """
        
    def executableName(self):
        return self.executable

    def executableArgs(self):
        return "-p pset.cfg"

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
        ## additional input files
        for file in self.additional_inbox_files:
            inp_box.append(common.work_space.cwdDir()+file)
        #print "sono inputSandbox, inp_box = ", inp_box
        return inp_box

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
            out_box.append(self.numberFile_(out,str(n_out)))
        return out_box
        return []

    def prepareSteeringCards(self):
        """
        Make initial modifications of the user's steering card file.
        """
        infile = open(self.pset,'r')
            
        outfile = open(common.work_space.jobDir()+self.name()+'.cfg', 'w')
           
        outfile.write('\n\n##### The following cards have been created by CRAB: DO NOT TOUCH #####\n')

        outfile.write('InputCollections=/System/'+self.owner+'/'+self.dataset+'/'+self.dataset+'\n')

        infile.close()
        outfile.close()
        return

    def wsRenameOutput(self, nj):
        """
        Returns part of a job script which renames the produced files.
        """

        txt = '\n'
        file_list = ''
        for fileWithSuffix in self.output_file:
            output_file_num = self.numberFile_(fileWithSuffix, '$NJob')
            file_list=file_list+output_file_num+','
            txt += '\n'
            txt += 'ls \n'
            txt += '\n'
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
