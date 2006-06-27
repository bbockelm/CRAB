from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import PsetManipulator  

import DBSInfo_EDM
import DataDiscovery_EDM
import DataLocation_EDM
import Scram

import os, string, re

class Cmssw(JobType):
    def __init__(self, cfg_params):
        JobType.__init__(self, 'CMSSW')
        common.logger.debug(3,'CMSSW::__init__')

        self.analisys_common_info = {}
        # Marco.
        self._params = {}
        self.cfg_params = cfg_params
        log = common.logger
        
        self.scram = Scram.Scram(cfg_params)
        scramArea = ''
        self.additional_inbox_files = []
        self.scriptExe = ''
        self.executable = ''
        self.tgz_name = 'default.tgz'


        self.version = self.scram.getSWVersion()
        self.setParam_('application', self.version)
        common.analisys_common_info['sw_version'] = self.version
        ### FEDE
        common.analisys_common_info['copy_input_data'] = 0
        common.analisys_common_info['events_management'] = 1

        ### collect Data cards
        try:
            tmp =  cfg_params['CMSSW.datasetpath']
            log.debug(6, "CMSSW::CMSSW(): datasetPath = "+tmp)
            if string.lower(tmp)=='none':
                self.datasetPath = None
            else:
                self.datasetPath = tmp
        except KeyError:
            msg = "Error: datasetpath not defined "  
            raise CrabException(msg)

        # ML monitoring
        # split dataset path style: /PreProdR3Minbias/SIM/GEN-SIM
        if not self.datasetPath:
            self.setParam_('dataset', 'None')
            self.setParam_('owner', 'None')
        else:
            datasetpath_split = self.datasetPath.split("/")
            self.setParam_('dataset', datasetpath_split[1])
            self.setParam_('owner', datasetpath_split[-1])

        self.setTaskid_()
        self.setParam_('taskId', self.cfg_params['taskId'])

        self.dataTiers = []

        ## now the application
        try:
            self.executable = cfg_params['CMSSW.executable']
            self.setParam_('exe', self.executable)
            log.debug(6, "CMSSW::CMSSW(): executable = "+self.executable)
            msg = "Default executable cmsRun overridden. Switch to " + self.executable
            log.debug(3,msg)
        except KeyError:
            self.executable = 'cmsRun'
            self.setParam_('exe', self.executable)
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
            self.scriptExe = cfg_params['USER.script_exe']
            self.additional_inbox_files.append(self.scriptExe)
            if self.scriptExe != '':
               if not os.path.isfile(self.scriptExe):
                  msg ="WARNING. file "+self.scriptExe+" not found"
                  raise CrabException(msg)
        except KeyError:
           pass
                  
        ## additional input files
        try:
            tmpAddFiles = string.split(cfg_params['CMSSW.additional_input_files'],',')
            for tmp in tmpAddFiles:
                if not os.path.exists(tmp):
                    raise CrabException("Additional input file not found: "+tmp)
                tmp=string.strip(tmp)
                self.additional_inbox_files.append(tmp)
                pass
            pass
        except KeyError:
            pass

        # files per job
        try:
            self.filesPerJob = int(cfg_params['CMSSW.files_per_jobs']) #Daniele
            self.selectFilesPerJob = 1
        except KeyError:
            self.filesPerJob = 0
            self.selectFilesPerJob = 0

        ## Events per job
        try:
            self.eventsPerJob =int( cfg_params['CMSSW.events_per_job'])
            self.selectEventsPerJob = 1
        except KeyError:
            self.eventsPerJob = -1
            self.selectEventsPerJob = 0
    
        # To be implemented
        # ## number of jobs
        # try:
        #     self.numberOfJobs =int( cfg_params['CMSSW.number_of_job'])
        #     self.selectNumberOfJobs = 1
        # except KeyError:
        #     self.selectNumberOfJobs = 0

        if (self.selectFilesPerJob == self.selectEventsPerJob):
            msg = 'Must define either files_per_jobs or events_per_job'
            raise CrabException(msg)

        if (self.selectEventsPerJob  and not self.datasetPath == None):
            msg = 'Splitting according to events_per_job available only with None as datasetpath'
            raise CrabException(msg)
    
        try:
            self.total_number_of_events = int(cfg_params['CMSSW.total_number_of_events'])
        except KeyError:
            msg = 'Must define total_number_of_events'
            raise CrabException(msg)
        
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
            for tmp in tmpGood:
                tmp=string.strip(tmp)
                CEWhiteList.append(tmp)
        except KeyError:
            pass

        #print 'CEWhiteList: ',CEWhiteList
        self.reCEWhiteList=[]
        for Good in CEWhiteList:
            self.reCEWhiteList.append(re.compile( Good ))

        common.logger.debug(5,'CEWhiteList: '+str(CEWhiteList))

        self.PsetEdit = PsetManipulator.PsetManipulator(self.pset) #Daniele Pset

        #DBSDLS-start
        ## Initialize the variables that are extracted from DBS/DLS and needed in other places of the code 
        self.maxEvents=0  # max events available   ( --> check the requested nb. of evts in Creator.py)
        self.DBSPaths={}  # all dbs paths requested ( --> input to the site local discovery script)
        ## Perform the data location and discovery (based on DBS/DLS)
        ## SL: Don't if NONE is specified as input (pythia use case)
        common.analisys_common_info['sites']=None
        if self.datasetPath:
            self.DataDiscoveryAndLocation(cfg_params)
        #DBSDLS-end          

        self.tgzNameWithPath = self.getTarBall(self.executable)

        # modify Pset
        if (self.datasetPath): # standard job
            self.PsetEdit.maxEvent(self.eventsPerJob) #Daniele  
            self.PsetEdit.inputModule("INPUT") #Daniele

        else:  # pythia like job
            self.PsetEdit.maxEvent(self.eventsPerJob) #Daniele  
            self.PsetEdit.pythiaSeed("INPUT") #Daniele
            try:
                self.sourceSeed = int(cfg_params['CMSSW.pythia_seed'])
            except KeyError:
                self.sourceSeed = 123456
                common.logger.message("No seed given, will use "+str(self.sourceSeed))
        
        self.PsetEdit.psetWriter(self.configFilename())
    
        ## Select Splitting
        if self.selectFilesPerJob: self.jobSplittingPerFiles()
        elif self.selectEventsPerJob: self.jobSplittingPerEvents()
        else:
            msg = 'Don\'t know how to split...'
            raise CrabException(msg)


    def DataDiscoveryAndLocation(self, cfg_params):

        common.logger.debug(10,"CMSSW::DataDiscoveryAndLocation()")

        datasetPath=self.datasetPath

        ## TODO
        dataTiersList = ""
        dataTiers = dataTiersList.split(',')

        ## Contact the DBS
        try:
            self.pubdata=DataDiscovery_EDM.DataDiscovery_EDM(datasetPath, dataTiers, cfg_params)
            self.pubdata.fetchDBSInfo()

        except DataDiscovery_EDM.NotExistingDatasetError, ex :
            msg = 'ERROR ***: failed Data Discovery in DBS : %s'%ex.getErrorMessage()
            raise CrabException(msg)

        except DataDiscovery_EDM.NoDataTierinProvenanceError, ex :
            msg = 'ERROR ***: failed Data Discovery in DBS : %s'%ex.getErrorMessage()
            raise CrabException(msg)
        except DataDiscovery_EDM.DataDiscoveryError, ex:
            msg = 'ERROR ***: failed Data Discovery in DBS  %s'%ex.getErrorMessage()
            raise CrabException(msg)

        ## get list of all required data in the form of dbs paths  (dbs path = /dataset/datatier/owner)
        ## self.DBSPaths=self.pubdata.getDBSPaths()
        common.logger.message("Required data are :"+self.datasetPath)

        filesbyblock=self.pubdata.getFiles()
        self.AllInputFiles=filesbyblock.values()
        self.files = self.AllInputFiles        

        ## TEMP
    #    self.filesTmp = filesbyblock.values()
    #    self.files = []
    #    locPath='rfio:cmsbose2.bo.infn.it:/flatfiles/SE00/cms/fanfani/ProdTest/'
    #    locPath=''
    #    tmp = []
    #    for file in self.filesTmp[0]:
    #        tmp.append(locPath+file)
    #    self.files.append(tmp)
        ## END TEMP

        ## get max number of events
        #common.logger.debug(10,"number of events for primary fileblocks %i"%self.pubdata.getMaxEvents())
        self.maxEvents=self.pubdata.getMaxEvents() ##  self.maxEvents used in Creator.py 
        common.logger.message("\nThe number of available events is %s"%self.maxEvents)

        ## Contact the DLS and build a list of sites hosting the fileblocks
        try:
            dataloc=DataLocation_EDM.DataLocation_EDM(filesbyblock.keys(),cfg_params)
            dataloc.fetchDLSInfo()
        except DataLocation_EDM.DataLocationError , ex:
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
        self.setParam_('TargetCE', ','.join(sites))
        return
    
    def jobSplittingPerFiles(self):
        """
        Perform job splitting based on number of files to be accessed per job
        """
        common.logger.debug(5,'Splitting per input files')
        common.logger.message('Required '+str(self.filesPerJob)+' files per job ')
        common.logger.message('Required '+str(self.total_number_of_events)+' events in total ')

        ## TODO: SL need to have (from DBS) a detailed list of how many events per each file
        n_tot_files = (len(self.files[0]))
        #print "n_tot_files = ", n_tot_files
        ## SL: this is wrong if the files have different number of events
        #print "self.maxEvents = ", self.maxEvents
        evPerFile = int(self.maxEvents)/n_tot_files
        #print "evPerFile = int(self.maxEvents)/n_tot_files =  ", evPerFile

        common.logger.debug(5,'Events per File '+str(evPerFile))

        ## if asked to process all events, do it
        if self.total_number_of_events == -1:
            self.total_number_of_events=self.maxEvents
            self.total_number_of_jobs = int(n_tot_files)*1/int(self.filesPerJob)
            common.logger.message(str(self.total_number_of_jobs)+' jobs will be created for all available events '+str(self.total_number_of_events)+' events')
        
        else:
            #print "self.total_number_of_events = ", self.total_number_of_events
            #print "evPerFile = ", evPerFile
            self.total_number_of_files = int(self.total_number_of_events/evPerFile)
            #print "self.total_number_of_files = int(self.total_number_of_events/evPerFile) = " , self.total_number_of_files
            ## SL: if ask for less event than what is computed to be available on a
            ##     file, process the first file anyhow.
            if self.total_number_of_files == 0:
                self.total_number_of_files = self.total_number_of_files + 1
                 

            common.logger.debug(5,'N files  '+str(self.total_number_of_files))

            check = 0
            
            ## Compute the number of jobs
            #self.total_number_of_jobs = int(n_tot_files)*1/int(self.filesPerJob)
            #print "self.total_number_of_files = ", self.total_number_of_files
            #print "self.filesPerJob = ", self.filesPerJob
            self.total_number_of_jobs = int(self.total_number_of_files/self.filesPerJob)
            #print "self.total_number_of_jobs = ", self.total_number_of_jobs 
            common.logger.debug(5,'N jobs  '+str(self.total_number_of_jobs))

            ## is there any remainder?
            check = int(self.total_number_of_files) - (int(self.total_number_of_jobs)*self.filesPerJob)

            common.logger.debug(5,'Check  '+str(check))

            if check > 0:
                self.total_number_of_jobs =  self.total_number_of_jobs + 1
                common.logger.message('Warning: last job will be created with '+str(check)+' files')

            #common.logger.message(str(self.total_number_of_jobs)+' jobs will be created for a total of '+str((self.total_number_of_jobs-1)*self.filesPerJob*evPerFile + check*evPerFile)+' events')
            common.logger.message(str(self.total_number_of_jobs)+' jobs will be created for a total of '+str((self.total_number_of_jobs)*self.filesPerJob*evPerFile + check*evPerFile)+' events')
            pass

        list_of_lists = []
        for i in xrange(0, int(n_tot_files), self.filesPerJob):
            parString = "\\{" 
            
            params = self.files[0][i: i+self.filesPerJob]
            for i in range(len(params) - 1):
                parString += '\\\"' + params[i] + '\\\"\,'
            
            parString += '\\\"' + params[len(params) - 1] + '\\\"\\}'
            list_of_lists.append(parString)
            pass

        self.list_of_args = list_of_lists
        #print self.list_of_args
        return

    def jobSplittingPerEvents(self):
        """
        Perform job splitting based on number of event per job
        """
        common.logger.debug(5,'Splitting per events')
        common.logger.message('Required '+str(self.eventsPerJob)+' events per job ')
        common.logger.message('Required '+str(self.total_number_of_events)+' events in total ')

        if (self.total_number_of_events < 0):
            msg='Cannot split jobs per Events with "-1" as total number of events'
            raise CrabException(msg)

        self.total_number_of_jobs = int(self.total_number_of_events/self.eventsPerJob)

        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
        print "self.total_number_of_events = ", self.total_number_of_events
        print "self.eventsPerJob = ", self.eventsPerJob
        print "self.total_number_of_jobs = ", self.total_number_of_jobs
        print "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
        
        common.logger.debug(5,'N jobs  '+str(self.total_number_of_jobs))

        # is there any remainder?
        check = int(self.total_number_of_events) - (int(self.total_number_of_jobs)*self.eventsPerJob)

        common.logger.debug(5,'Check  '+str(check))

        if check > 0:
            common.logger.message('Warning: asked '+self.total_number_of_events+' but will do only '+(int(self.total_number_of_jobs)*self.eventsPerJob))

        common.logger.message(str(self.total_number_of_jobs)+' jobs will be created for a total of '+str(self.total_number_of_jobs*self.eventsPerJob)+' events')

        # argument is seed number.$i
        self.list_of_args = []
        for i in range(self.total_number_of_jobs):
            self.list_of_args.append(int(str(self.sourceSeed)+str(i)))
        print self.list_of_args

        return

    def split(self, jobParams):
 
        common.jobDB.load()
        #### Fabio
        njobs = self.total_number_of_jobs
        arglist = self.list_of_args
        # create the empty structure
        for i in range(njobs):
            jobParams.append("")
        
        for job in range(njobs):
            jobParams[job] = str(arglist[job])
            common.jobDB.setArguments(job, jobParams[job])

        common.jobDB.save()
        return
    
    def getJobTypeArguments(self, nj, sched):
        return common.jobDB.arguments(nj)
  
    def numberOfJobs(self):
        # Fabio
        return self.total_number_of_jobs

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

    def checkWhiteList(self, allSites):

        if len(self.reCEWhiteList)==0: return allSites
        sites = []
        for site in allSites:
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
 
        ## Now check if module dir is present
        moduleDir = 'module'
        if os.path.isdir(swArea+'/'+moduleDir):
            filesToBeTarred.append(moduleDir)

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
        txt = '' 
   
        ## OLI_Daniele at this level  middleware already known

        txt += 'if [ $middleware == LCG ]; then \n' 
        txt += self.wsSetupCMSLCGEnvironment_()
        txt += 'elif [ $middleware == OSG ]; then\n'
        txt += '    time=`date -u +"%s"`\n'
        txt += '    WORKING_DIR=$OSG_WN_TMP/cms_$time\n'
        txt += '    echo "Creating working directory: $WORKING_DIR"\n'
        txt += '    /bin/mkdir -p $WORKING_DIR\n'
        txt += '    if [ ! -d $WORKING_DIR ] ;then\n'
        txt += '        echo "SET_CMS_ENV 10016 ==> OSG $WORKING_DIR could not be created on WN `hostname`"\n'
        txt += '	echo "JOB_EXIT_STATUS = 10016"\n'
        txt += '	echo "JobExitCode=10016" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '	dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '        rm -f $RUNTIME_AREA/$repo \n'
        txt += '        echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '        echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
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
        txt += scram+' project CMSSW '+self.version+'\n'
        txt += 'status=$?\n'
        txt += 'if [ $status != 0 ] ; then\n'
        txt += '   echo "SET_EXE_ENV 10034 ==>ERROR CMSSW '+self.version+' not found on `hostname`" \n'
        txt += '   echo "JOB_EXIT_STATUS = 10034"\n'
        txt += '   echo "JobExitCode=10034" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '   dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '   rm -f $RUNTIME_AREA/$repo \n'
        txt += '   echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '   echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        ## OLI_Daniele
        txt += '    if [ $middleware == OSG ]; then \n'
        txt += '        echo "Remove working directory: $WORKING_DIR"\n'
        txt += '        cd $RUNTIME_AREA\n'
        txt += '        /bin/rm -rf $WORKING_DIR\n'
        txt += '        if [ -d $WORKING_DIR ] ;then\n'
        txt += '	    echo "SET_CMS_ENV 10018 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after CMSSW CMSSW_0_6_1 not found on `hostname`"\n'
        txt += '	    echo "JOB_EXIT_STATUS = 10018"\n'
        txt += '	    echo "JobExitCode=10018" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '	    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '            rm -f $RUNTIME_AREA/$repo \n'
        txt += '            echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '            echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '        fi\n'
        txt += '    fi \n'
        txt += '   exit 1 \n'
        txt += 'fi \n'
        txt += 'echo "CMSSW_VERSION =  '+self.version+'"\n'
        txt += 'cd '+self.version+'\n'
        ### needed grep for bug in scramv1 ###
        txt += 'eval `'+scram+' runtime -sh | grep -v SCRAMRT_LSB_JOBNAME`\n'

        # Handle the arguments:
        txt += "\n"
        txt += "## number of arguments (first argument always jobnumber)\n"
        txt += "\n"
        txt += "narg=$#\n"
        txt += "if [ $narg -lt 2 ]\n"
        txt += "then\n"
        txt += "    echo 'SET_EXE_ENV 1 ==> ERROR Too few arguments' +$narg+ \n"
        txt += '    echo "JOB_EXIT_STATUS = 50113"\n'
        txt += '    echo "JobExitCode=50113" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    rm -f $RUNTIME_AREA/$repo \n'
        txt += '    echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        ## OLI_Daniele
        txt += '    if [ $middleware == OSG ]; then \n'
        txt += '        echo "Remove working directory: $WORKING_DIR"\n'
        txt += '        cd $RUNTIME_AREA\n'
        txt += '        /bin/rm -rf $WORKING_DIR\n'
        txt += '        if [ -d $WORKING_DIR ] ;then\n'
        txt += '	    echo "SET_EXE_ENV 50114 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after Too few arguments for CRAB job wrapper"\n'
        txt += '	    echo "JOB_EXIT_STATUS = 50114"\n'
        txt += '	    echo "JobExitCode=50114" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '	    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '            rm -f $RUNTIME_AREA/$repo \n'
        txt += '            echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '            echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '        fi\n'
        txt += '    fi \n'
        txt += "    exit 1\n"
        txt += "fi\n"
        txt += "\n"

        # Prepare job-specific part
        job = common.job_list[nj]
        pset = os.path.basename(job.configFilename())
        txt += '\n'
        if (self.datasetPath): # standard job
            txt += 'InputFiles=$2\n'
            txt += 'echo "Inputfiles:<$InputFiles>"\n'
            txt += 'sed "s#{\'INPUT\'}#$InputFiles#" $RUNTIME_AREA/'+pset+' > pset.cfg\n'
        else:  # pythia like job
            txt += 'Seed=$2\n'
            txt += 'echo "Seed: <$Seed>"\n'
            txt += 'sed "s#INPUT#$Seed#" $RUNTIME_AREA/'+pset+' > pset.cfg\n'

        if len(self.additional_inbox_files) > 0:
            for file in self.additional_inbox_files:
                txt += 'if [ -e $RUNTIME_AREA/'+file+' ] ; then\n'
                txt += '   cp $RUNTIME_AREA/'+file+' .\n'
                txt += '   chmod +x '+file+'\n'
                txt += 'fi\n'
            pass 

        txt += 'echo "### END JOB SETUP ENVIRONMENT ###"\n\n'

        txt += '\n'
        txt += 'echo "***** cat pset.cfg *********"\n'
        txt += 'cat pset.cfg\n'
        txt += 'echo "****** end pset.cfg ********"\n'
        txt += '\n'
        # txt += 'echo "***** cat pset1.cfg *********"\n'
        # txt += 'cat pset1.cfg\n'
        # txt += 'echo "****** end pset1.cfg ********"\n'
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
            txt += '   echo "JobExitCode=$untar_status" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '   if [ $middleware == OSG ]; then \n'
            txt += '       echo "Remove working directory: $WORKING_DIR"\n'
            txt += '       cd $RUNTIME_AREA\n'
            txt += '       /bin/rm -rf $WORKING_DIR\n'
            txt += '       if [ -d $WORKING_DIR ] ;then\n'
            txt += '           echo "SET_EXE 50999 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after Untarring .tgz file failed"\n'
            txt += '           echo "JOB_EXIT_STATUS = 50999"\n'
            txt += '           echo "JobExitCode=50999" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '           dumpStatus $RUNTIME_AREA/$repo\n'
            txt += '           rm -f $RUNTIME_AREA/$repo \n'
            txt += '           echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
            txt += '           echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '       fi\n'
            txt += '   fi \n'
            txt += '   \n'
            txt += '   exit 1 \n'
            txt += 'else \n'
            txt += '   echo "Successful untar" \n'
            txt += 'fi \n'
            pass
        
        return txt

    def modifySteeringCards(self, nj):
        """
        modify the card provided by the user, 
        writing a new card into share dir
        """
        
    def executableName(self):
        return self.executable

    def executableArgs(self):
        return " -p pset.cfg"

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
        #for file in self.additional_inbox_files:
        #    inp_box.append(common.work_space.cwdDir()+file)
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
        return

    def wsRenameOutput(self, nj):
        """
        Returns part of a job script which renames the produced files.
        """

        txt = '\n'
        txt += '# directory content\n'
        txt += 'ls \n'
        file_list = ''
        for fileWithSuffix in self.output_file:
            output_file_num = self.numberFile_(fileWithSuffix, '$NJob')
            file_list=file_list+output_file_num+' '
            txt += '\n'
            txt += '# check output file\n'
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
            ### OLI_DANIELE
            if common.scheduler.boss_scheduler_name == 'condor_g':
                txt += '    if [ $middleware == OSG ]; then \n'
                txt += '        echo "prepare dummy output file"\n'
                txt += '        echo "Processing of job output failed" > $RUNTIME_AREA/'+output_file_num+'\n'
                txt += '    fi \n'
            txt += 'else\n'
            txt += '   cp '+fileWithSuffix+' $RUNTIME_AREA/'+output_file_num+'\n'
            txt += 'fi\n'
       
        txt += 'cd $RUNTIME_AREA\n'
        file_list=file_list[:-1]
        txt += 'file_list="'+file_list+'"\n'
        ### OLI_DANIELE
        txt += 'if [ $middleware == OSG ]; then\n'  
        txt += '    cd $RUNTIME_AREA\n'
        txt += '    echo "Remove working directory: $WORKING_DIR"\n'
        txt += '    /bin/rm -rf $WORKING_DIR\n'
        txt += '    if [ -d $WORKING_DIR ] ;then\n'
        txt += '	echo "SET_EXE 60999 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after cleanup of WN"\n'
        txt += '	echo "JOB_EXIT_STATUS = 60999"\n'
        txt += '	echo "JobExitCode=60999" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '	dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '        rm -f $RUNTIME_AREA/$repo \n'
        txt += '        echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '        echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    fi\n'
        txt += 'fi\n'
        txt += '\n'
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
        if common.analisys_common_info['sw_version']:
            req='Member("VO-cms-' + \
                 common.analisys_common_info['sw_version'] + \
                 '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'
        if common.analisys_common_info['sites']:
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

    def configFilename(self):
        """ return the config filename """
        return self.name()+'.cfg'

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
        txt += '       rm -f $RUNTIME_AREA/$repo \n'
        txt += '       echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '       echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '       exit 1\n'
        txt += '\n'
        txt += '       echo "Remove working directory: $WORKING_DIR"\n'
        txt += '       cd $RUNTIME_AREA\n'
        txt += '       /bin/rm -rf $WORKING_DIR\n'
        txt += '       if [ -d $WORKING_DIR ] ;then\n'
        txt += '	    echo "SET_CMS_ENV 10017 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after $GRID3_APP_DIR/cmssoft/cmsset_default.sh and $OSG_APP/cmssoft/cmsset_default.sh file not found"\n' 
        txt += '	    echo "JOB_EXIT_STATUS = 10017"\n' 
        txt += '	    echo "JobExitCode=10017" | tee -a $RUNTIME_AREA/$repo\n' 
        txt += '	    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '            rm -f $RUNTIME_AREA/$repo \n'
        txt += '            echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '            echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
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
        txt += '       rm -f $RUNTIME_AREA/$repo \n'
        txt += '       echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '       echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '       exit 1\n'
        txt += '   else\n'
        txt += '       echo "Sourcing environment... "\n'
        txt += '       if [ ! -s $VO_CMS_SW_DIR/cmsset_default.sh ] ;then\n'
        txt += '           echo "SET_CMS_ENV 10020 ==> ERROR cmsset_default.sh file not found into dir $VO_CMS_SW_DIR"\n'
        txt += '           echo "JOB_EXIT_STATUS = 10020"\n'
        txt += '           echo "JobExitCode=10020" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '           dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '           rm -f $RUNTIME_AREA/$repo \n'
        txt += '           echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '           echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
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
        txt += '           rm -f $RUNTIME_AREA/$repo \n'
        txt += '           echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '           echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
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
        txt += '       echo "SET_CMS_ENV 10033 ==> ERROR OS unknown, LCG environment not initialized"\n'
        txt += '       echo "JOB_EXIT_STATUS = 10033"\n'
        txt += '       echo "JobExitCode=10033" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '       dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '       rm -f $RUNTIME_AREA/$repo \n'
        txt += '       echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '       echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '       exit 1\n'
        txt += '   fi\n'
        txt += '   echo "SET_CMS_ENV 0 ==> setup cms environment ok"\n'
        txt += '   echo "### END SETUP CMS LCG ENVIRONMENT ###"\n'
        return txt

    def setParam_(self, param, value):
        self._params[param] = value

    def getParams(self):
        return self._params

    def setTaskid_(self):
        self._taskId = self.cfg_params['taskId']
        
    def getTaskid(self):
        return self._taskId
