from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import Scram
from Splitter import JobSplitter

import os, string, glob

class Cmssw(JobType):
    def __init__(self, cfg_params, ncjobs,skip_blocks, isNew):
        JobType.__init__(self, 'CMSSW')
        common.logger.debug(3,'CMSSW::__init__')
        self.skip_blocks = skip_blocks
        self.argsList = []

        self._params = {}
        self.cfg_params = cfg_params

        ### Temporary patch to automatically skip the ISB size check:
        server=self.cfg_params.get('CRAB.server_name',None)
        size = 9.5
        if server or common.scheduler.name().upper() in ['LSF','CAF']: size = 99999
        ### D.S.
        self.MaxTarBallSize = float(self.cfg_params.get('EDG.maxtarballsize',size))

        # number of jobs requested to be created, limit obj splitting
        self.ncjobs = ncjobs

        log = common.logger

        self.scram = Scram.Scram(cfg_params)
        self.additional_inbox_files = []
        self.scriptExe = ''
        self.executable = ''
        self.executable_arch = self.scram.getArch()
        self.tgz_name = 'default.tgz'
        self.scriptName = 'CMSSW.sh'
        self.pset = ''
        self.datasetPath = ''

        # set FJR file name
        self.fjrFileName = 'crab_fjr.xml'

        self.version = self.scram.getSWVersion()
        common.logger.write("CMSSW version is: "+str(self.version))
        version_array = self.version.split('_')
        self.CMSSW_major = 0
        self.CMSSW_minor = 0
        self.CMSSW_patch = 0
        try:
            self.CMSSW_major = int(version_array[1])
            self.CMSSW_minor = int(version_array[2])
            self.CMSSW_patch = int(version_array[3])
        except:
            msg = "Cannot parse CMSSW version string: " + self.version + " for major and minor release number!"
            raise CrabException(msg)

        if self.CMSSW_major < 1 or (self.CMSSW_major == 1 and self.CMSSW_minor < 5):
            msg = "CRAB supports CMSSW >= 1_5_x only. Use an older CRAB version."
            raise CrabException(msg)
            """
            As CMSSW versions are dropped we can drop more code:
            1.X dropped: drop support for running .cfg on WN
            2.0 dropped: drop all support for cfg here and in writeCfg
            2.0 dropped: Recheck the random number seed support
            """

        ### collect Data cards


        ### Temporary: added to remove input file control in the case of PU
        self.dataset_pu = cfg_params.get('CMSSW.dataset_pu', None)

        tmp =  cfg_params['CMSSW.datasetpath']
        log.debug(6, "CMSSW::CMSSW(): datasetPath = "+tmp)

        if tmp =='':
            msg = "Error: datasetpath not defined "
            raise CrabException(msg)
        elif string.lower(tmp)=='none':
            self.datasetPath = None
            self.selectNoInput = 1
        else:
            self.datasetPath = tmp
            self.selectNoInput = 0

        self.dataTiers = []
        
        self.debugWrap=''
        self.debug_wrapper = int(cfg_params.get('USER.debug_wrapper',0))
        if self.debug_wrapper == 1: self.debugWrap='--debug'
        ## now the application
        self.managedGenerators = ['madgraph','comphep']
        self.generator = cfg_params.get('CMSSW.generator','pythia').lower()
        self.executable = cfg_params.get('CMSSW.executable','cmsRun')
        log.debug(6, "CMSSW::CMSSW(): executable = "+self.executable)

        if not cfg_params.has_key('CMSSW.pset'):
            raise CrabException("PSet file missing. Cannot run cmsRun ")
        self.pset = cfg_params['CMSSW.pset']
        log.debug(6, "Cmssw::Cmssw(): PSet file = "+self.pset)
        if self.pset.lower() != 'none' :
            if (not os.path.exists(self.pset)):
                raise CrabException("User defined PSet file "+self.pset+" does not exist")
        else:
            self.pset = None

        # output files
        ## stuff which must be returned always via sandbox
        self.output_file_sandbox = []

        # add fjr report by default via sandbox
        self.output_file_sandbox.append(self.fjrFileName)

        # other output files to be returned via sandbox or copied to SE
        outfileflag = False
        self.output_file = []
        tmp = cfg_params.get('CMSSW.output_file',None)
        if tmp :
            self.output_file = [x.strip() for x in tmp.split(',')]
            outfileflag = True #output found
        #else:
        #    log.message("No output file defined: only stdout/err and the CRAB Framework Job Report will be available\n")

        # script_exe file as additional file in inputSandbox
        self.scriptExe = cfg_params.get('USER.script_exe',None)
        if self.scriptExe :
            if not os.path.isfile(self.scriptExe):
                msg ="ERROR. file "+self.scriptExe+" not found"
                raise CrabException(msg)
            self.additional_inbox_files.append(string.strip(self.scriptExe))

        if self.datasetPath == None and self.pset == None and self.scriptExe == '' :
            msg ="Error. script_exe  not defined"
            raise CrabException(msg)

        # use parent files...
        self.useParent = int(self.cfg_params.get('CMSSW.use_parent',0))

        ## additional input files
        if cfg_params.has_key('USER.additional_input_files'):
            tmpAddFiles = string.split(cfg_params['USER.additional_input_files'],',')
            for tmp in tmpAddFiles:
                tmp = string.strip(tmp)
                dirname = ''
                if not tmp[0]=="/": dirname = "."
                files = []
                if string.find(tmp,"*")>-1:
                    files = glob.glob(os.path.join(dirname, tmp))
                    if len(files)==0:
                        raise CrabException("No additional input file found with this pattern: "+tmp)
                else:
                    files.append(tmp)
                for file in files:
                    if not os.path.exists(file):
                        raise CrabException("Additional input file not found: "+file)
                    pass
                    self.additional_inbox_files.append(string.strip(file))
                pass
            pass
            common.logger.debug(5,"Additional input files: "+str(self.additional_inbox_files))
        pass


        ## New method of dealing with seeds
        self.incrementSeeds = []
        self.preserveSeeds = []
        if cfg_params.has_key('CMSSW.preserve_seeds'):
            tmpList = cfg_params['CMSSW.preserve_seeds'].split(',')
            for tmp in tmpList:
                tmp.strip()
                self.preserveSeeds.append(tmp)
        if cfg_params.has_key('CMSSW.increment_seeds'):
            tmpList = cfg_params['CMSSW.increment_seeds'].split(',')
            for tmp in tmpList:
                tmp.strip()
                self.incrementSeeds.append(tmp)

        self.firstRun = cfg_params.get('CMSSW.first_run',None)

        # Copy/return
        self.copy_data = int(cfg_params.get('USER.copy_data',0))
        self.return_data = int(cfg_params.get('USER.return_data',0))

        self.conf = {}
        self.conf['pubdata'] = None
        # number of jobs requested to be created, limit obj splitting DD
        #DBSDLS-start
        ## Initialize the variables that are extracted from DBS/DLS and needed in other places of the code
        self.maxEvents=0  # max events available   ( --> check the requested nb. of evts in Creator.py)
        self.DBSPaths={}  # all dbs paths requested ( --> input to the site local discovery script)
        self.jobDestination=[]  # Site destination(s) for each job (list of lists)
        ## Perform the data location and discovery (based on DBS/DLS)
        ## SL: Don't if NONE is specified as input (pythia use case)
        blockSites = {}
        if self.datasetPath:
            blockSites = self.DataDiscoveryAndLocation(cfg_params)
        #DBSDLS-end
        self.conf['blockSites']=blockSites

        ## Select Splitting
        splitByRun = int(cfg_params.get('CMSSW.split_by_run',0))

        if self.selectNoInput:
            if self.pset == None:
                self.algo = 'ForScript'
            else:
                self.algo = 'NoInput'
                self.conf['managedGenerators']=self.managedGenerators
                self.conf['generator']=self.generator
        elif splitByRun ==1:
            self.algo = 'RunBased'
        else:
            self.algo = 'EventBased'

#        self.algo = 'LumiBased'
        splitter = JobSplitter(self.cfg_params,self.conf)
        self.dict = splitter.Algos()[self.algo]()

        # modify Pset only the first time
        if isNew:
            if self.pset != None:
                import PsetManipulator as pp
                PsetEdit = pp.PsetManipulator(self.pset)
                try:
                    # Add FrameworkJobReport to parameter-set, set max events.
                    # Reset later for data jobs by writeCFG which does all modifications
                    PsetEdit.maxEvent(-1)
                    PsetEdit.skipEvent(0)
                    PsetEdit.psetWriter(self.configFilename())
                    ## If present, add TFileService to output files
                    if not int(cfg_params.get('CMSSW.skip_TFileService_output',0)):
                        tfsOutput = PsetEdit.getTFileService()
                        if tfsOutput:
                            if tfsOutput in self.output_file:
                                common.logger.debug(5,"Output from TFileService "+tfsOutput+" already in output files")
                            else:
                                outfileflag = True #output found
                                self.output_file.append(tfsOutput)
                                common.logger.message("Adding "+tfsOutput+" (from TFileService) to list of output files")
                            pass
                        pass
                    ## If present and requested, add PoolOutputModule to output files
                    if int(cfg_params.get('CMSSW.get_edm_output',0)):
                        edmOutput = PsetEdit.getPoolOutputModule()
                        if edmOutput:
                            if edmOutput in self.output_file:
                                common.logger.debug(5,"Output from PoolOutputModule "+edmOutput+" already in output files")
                            else:
                                self.output_file.append(edmOutput)
                                common.logger.message("Adding "+edmOutput+" (from PoolOutputModule) to list of output files")
                            pass
                        pass
                except CrabException:
                    msg='Error while manipulating ParameterSet: exiting...'
                    raise CrabException(msg)
            ## Prepare inputSandbox TarBall (only the first time)
            self.tgzNameWithPath = self.getTarBall(self.executable)

    def DataDiscoveryAndLocation(self, cfg_params):

        import DataDiscovery
        import DataLocation
        common.logger.debug(10,"CMSSW::DataDiscoveryAndLocation()")

        datasetPath=self.datasetPath

        ## Contact the DBS
        common.logger.message("Contacting Data Discovery Services ...")
        try:
            self.pubdata=DataDiscovery.DataDiscovery(datasetPath, cfg_params,self.skip_blocks)
            self.pubdata.fetchDBSInfo()

        except DataDiscovery.NotExistingDatasetError, ex :
            msg = 'ERROR ***: failed Data Discovery in DBS : %s'%ex.getErrorMessage()
            raise CrabException(msg)
        except DataDiscovery.NoDataTierinProvenanceError, ex :
            msg = 'ERROR ***: failed Data Discovery in DBS : %s'%ex.getErrorMessage()
            raise CrabException(msg)
        except DataDiscovery.DataDiscoveryError, ex:
            msg = 'ERROR ***: failed Data Discovery in DBS :  %s'%ex.getErrorMessage()
            raise CrabException(msg)

        self.filesbyblock=self.pubdata.getFiles()
        #print self.filesbyblock
        self.conf['pubdata']=self.pubdata

        ## get max number of events
        self.maxEvents=self.pubdata.getMaxEvents()

        ## Contact the DLS and build a list of sites hosting the fileblocks
        try:
            dataloc=DataLocation.DataLocation(self.filesbyblock.keys(),cfg_params)
            dataloc.fetchDLSInfo()

        except DataLocation.DataLocationError , ex:
            msg = 'ERROR ***: failed Data Location in DLS \n %s '%ex.getErrorMessage()
            raise CrabException(msg)


        unsorted_sites = dataloc.getSites()
        #print "Unsorted :",unsorted_sites
        sites = self.filesbyblock.fromkeys(self.filesbyblock,'')
        for lfn in self.filesbyblock.keys():
            #print lfn
            if unsorted_sites.has_key(lfn):
                #print "Found ",lfn
                sites[lfn]=unsorted_sites[lfn]
            else:
                #print "Not Found ",lfn
                sites[lfn]=[]
        #print sites

        #print "Sorted :",sites
        if len(sites)==0:
            msg = 'ERROR ***: no location for any of the blocks of this dataset: \n\t %s \n'%datasetPath
            msg += "\tMaybe the dataset is located only at T1's (or at T0), where analysis jobs are not allowed\n"
            msg += "\tPlease check DataDiscovery page https://cmsweb.cern.ch/dbs_discovery/\n"
            raise CrabException(msg)

        allSites = []
        listSites = sites.values()
        for listSite in listSites:
            for oneSite in listSite:
                allSites.append(oneSite)
        allSites = self.uniquelist(allSites)

        # screen output
        common.logger.message("Requested dataset: " + datasetPath + " has " + str(self.maxEvents) + " events in " + str(len(self.filesbyblock.keys())) + " blocks.\n")

        return sites


    def split(self, jobParams,firstJobID):

        arglist = self.dict['args']
        njobs = self.dict['njobs']
        self.jobDestination = self.dict['jobDestination']

        if njobs==0:
            raise CrabException("Ask to split "+str(njobs)+" jobs: aborting")

        # create the empty structure
        for i in range(njobs):
            jobParams.append("")

        listID=[]
        listField=[]
        for id in range(njobs):
            job = id + int(firstJobID)
            jobParams[id] = arglist[id]
            listID.append(job+1)
            job_ToSave ={}
            concString = ' '
            argu=''
            if len(jobParams[id]):
                argu +=   concString.join(jobParams[id] )
            job_ToSave['arguments']= str(job+1)+' '+argu
            job_ToSave['dlsDestination']= self.jobDestination[id]
            listField.append(job_ToSave)
            msg="Job "+str(job)+" Arguments:   "+str(job+1)+" "+argu+"\n"  \
            +"                     Destination: "+str(self.jobDestination[id])
            common.logger.debug(5,msg)
        common._db.updateJob_(listID,listField)
        self.argsList = (len(jobParams[0])+1)

        return

    def numberOfJobs(self):
        return self.dict['njobs']

    def getTarBall(self, exe):
        """
        Return the TarBall with lib and exe
        """
        self.tgzNameWithPath = common.work_space.pathForTgz()+self.tgz_name
        if os.path.exists(self.tgzNameWithPath):
            return self.tgzNameWithPath

        # Prepare a tar gzipped file with user binaries.
        self.buildTar_(exe)

        return string.strip(self.tgzNameWithPath)

    def buildTar_(self, executable):

        # First of all declare the user Scram area
        swArea = self.scram.getSWArea_()
        swReleaseTop = self.scram.getReleaseTop_()

        ## check if working area is release top
        if swReleaseTop == '' or swArea == swReleaseTop:
            common.logger.debug(3,"swArea = "+swArea+" swReleaseTop ="+swReleaseTop)
            return

        import tarfile
        try: # create tar ball
            tar = tarfile.open(self.tgzNameWithPath, "w:gz")
            ## First find the executable
            if (self.executable != ''):
                exeWithPath = self.scram.findFile_(executable)
                if ( not exeWithPath ):
                    raise CrabException('User executable '+executable+' not found')

                ## then check if it's private or not
                if exeWithPath.find(swReleaseTop) == -1:
                    # the exe is private, so we must ship
                    common.logger.debug(5,"Exe "+exeWithPath+" to be tarred")
                    path = swArea+'/'
                    # distinguish case when script is in user project area or given by full path somewhere else
                    if exeWithPath.find(path) >= 0 :
                        exe = string.replace(exeWithPath, path,'')
                        tar.add(path+exe,exe)
                    else :
                        tar.add(exeWithPath,os.path.basename(executable))
                    pass
                else:
                    # the exe is from release, we'll find it on WN
                    pass

            ## Now get the libraries: only those in local working area
            tar.dereference=True
            libDir = 'lib'
            lib = swArea+'/' +libDir
            common.logger.debug(5,"lib "+lib+" to be tarred")
            if os.path.exists(lib):
                tar.add(lib,libDir)

            ## Now check if module dir is present
            moduleDir = 'module'
            module = swArea + '/' + moduleDir
            if os.path.isdir(module):
                tar.add(module,moduleDir)
            tar.dereference=False

            ## Now check if any data dir(s) is present
            self.dataExist = False
            todo_list = [(i, i) for i in  os.listdir(swArea+"/src")]
            while len(todo_list):
                entry, name = todo_list.pop()
                if name.startswith('crab_0_') or  name.startswith('.') or name == 'CVS':
                    continue
                if os.path.isdir(swArea+"/src/"+entry):
                    entryPath = entry + '/'
                    todo_list += [(entryPath + i, i) for i in  os.listdir(swArea+"/src/"+entry)]
                    if name == 'data':
                        self.dataExist=True
                        common.logger.debug(5,"data "+entry+" to be tarred")
                        tar.add(swArea+"/src/"+entry,"src/"+entry)
                    pass
                pass

            ### CMSSW ParameterSet
            if not self.pset is None:
                cfg_file = common.work_space.jobDir()+self.configFilename()
                tar.add(cfg_file,self.configFilename())


            ## Add ProdCommon dir to tar
            prodcommonDir = './'
            prodcommonPath = os.environ['CRABDIR'] + '/' + 'external/'
            neededStuff = ['ProdCommon/__init__.py','ProdCommon/FwkJobRep', 'ProdCommon/CMSConfigTools', \
                           'ProdCommon/Core', 'ProdCommon/MCPayloads', 'IMProv', 'ProdCommon/Storage']
            for file in neededStuff:
                tar.add(prodcommonPath+file,prodcommonDir+file)

            ##### ML stuff
            ML_file_list=['report.py', 'DashboardAPI.py', 'Logger.py', 'ProcInfo.py', 'apmon.py']
            path=os.environ['CRABDIR'] + '/python/'
            for file in ML_file_list:
                tar.add(path+file,file)

            ##### Utils
            Utils_file_list=['parseCrabFjr.py','writeCfg.py', 'fillCrabFjr.py','cmscp.py']
            for file in Utils_file_list:
                tar.add(path+file,file)

            ##### AdditionalFiles
            tar.dereference=True
            for file in self.additional_inbox_files:
                tar.add(file,string.split(file,'/')[-1])
            tar.dereference=False
            common.logger.debug(5,"Files in "+self.tgzNameWithPath+" : "+str(tar.getnames()))

            tar.close()
        except IOError, exc:
            common.logger.write(str(exc))
            raise CrabException('Could not create tar-ball '+self.tgzNameWithPath)
        except tarfile.TarError, exc:
            common.logger.write(str(exc))
            raise CrabException('Could not create tar-ball '+self.tgzNameWithPath)

        ## check for tarball size
        tarballinfo = os.stat(self.tgzNameWithPath)
        if ( tarballinfo.st_size > self.MaxTarBallSize*1024*1024 ) :
            msg  = 'Input sandbox size of ' + str(float(tarballinfo.st_size)/1024.0/1024.0) + ' MB is larger than the allowed ' + str(self.MaxTarBallSize) \
               +'MB input sandbox limit \n'
            msg += '      and not supported by the direct GRID submission system.\n'
            msg += '      Please use the CRAB server mode by setting server_name=<NAME> in section [CRAB] of your crab.cfg.\n'
            msg += '      For further infos please see https://twiki.cern.ch/twiki/bin/view/CMS/CrabServer#CRABSERVER_for_Users'
            raise CrabException(msg)

        ## create tar-ball with ML stuff

    def wsSetupEnvironment(self, nj=0):
        """
        Returns part of a job script which prepares
        the execution environment for the job 'nj'.
        """
        # FUTURE: Drop support for .cfg when possible
        if (self.CMSSW_major >= 2 and self.CMSSW_minor >= 1) or (self.CMSSW_major >= 3):
            psetName = 'pset.py'
        else:
            psetName = 'pset.cfg'
        # Prepare JobType-independent part
        txt = '\n#Written by cms_cmssw::wsSetupEnvironment\n'
        txt += 'echo ">>> setup environment"\n'
        txt += 'if [ $middleware == LCG ]; then \n'
        txt += self.wsSetupCMSLCGEnvironment_()
        txt += 'elif [ $middleware == OSG ]; then\n'
        txt += '    WORKING_DIR=`/bin/mktemp  -d $OSG_WN_TMP/cms_XXXXXXXXXXXX`\n'
        txt += '    if [ ! $? == 0 ] ;then\n'
        txt += '        echo "ERROR ==> OSG $WORKING_DIR could not be created on WN `hostname`"\n'
        txt += '        job_exit_code=10016\n'
        txt += '        func_exit\n'
        txt += '    fi\n'
        txt += '    echo ">>> Created working directory: $WORKING_DIR"\n'
        txt += '\n'
        txt += '    echo "Change to working directory: $WORKING_DIR"\n'
        txt += '    cd $WORKING_DIR\n'
        txt += '    echo ">>> current directory (WORKING_DIR): $WORKING_DIR"\n'
        txt += self.wsSetupCMSOSGEnvironment_()
        #Setup SGE Environment
        txt += 'elif [ $middleware == SGE ]; then\n'
        txt += self.wsSetupCMSLCGEnvironment_()

        txt += 'fi\n'

        # Prepare JobType-specific part
        scram = self.scram.commandName()
        txt += '\n\n'
        txt += 'echo ">>> specific cmssw setup environment:"\n'
        txt += 'echo "CMSSW_VERSION =  '+self.version+'"\n'
        txt += scram+' project CMSSW '+self.version+'\n'
        txt += 'status=$?\n'
        txt += 'if [ $status != 0 ] ; then\n'
        txt += '    echo "ERROR ==> CMSSW '+self.version+' not found on `hostname`" \n'
        txt += '    job_exit_code=10034\n'
        txt += '    func_exit\n'
        txt += 'fi \n'
        txt += 'cd '+self.version+'\n'
        txt += 'SOFTWARE_DIR=`pwd`; export SOFTWARE_DIR\n'
        txt += 'echo ">>> current directory (SOFTWARE_DIR): $SOFTWARE_DIR" \n'
        txt += 'eval `'+scram+' runtime -sh | grep -v SCRAMRT_LSB_JOBNAME`\n'
        txt += 'if [ $? != 0 ] ; then\n'
        txt += '    echo "ERROR ==> Problem with the command: "\n'
        txt += '    echo "eval \`'+scram+' runtime -sh | grep -v SCRAMRT_LSB_JOBNAME \` at `hostname`"\n'
        txt += '    job_exit_code=10034\n'
        txt += '    func_exit\n'
        txt += 'fi \n'
        # Handle the arguments:
        txt += "\n"
        txt += "## number of arguments (first argument always jobnumber)\n"
        txt += "\n"
        txt += "if [ $nargs -lt "+str(self.argsList)+" ]\n"
        txt += "then\n"
        txt += "    echo 'ERROR ==> Too few arguments' +$nargs+ \n"
        txt += '    job_exit_code=50113\n'
        txt += "    func_exit\n"
        txt += "fi\n"
        txt += "\n"

        # Prepare job-specific part
        job = common.job_list[nj]
        if (self.datasetPath):
            self.primaryDataset = self.datasetPath.split("/")[1]
            DataTier = self.datasetPath.split("/")[2]
            txt += '\n'
            txt += 'DatasetPath='+self.datasetPath+'\n'

            txt += 'PrimaryDataset='+self.primaryDataset +'\n'
            txt += 'DataTier='+DataTier+'\n'
            txt += 'ApplicationFamily=cmsRun\n'

        else:
            self.primaryDataset = 'null'
            txt += 'DatasetPath=MCDataTier\n'
            txt += 'PrimaryDataset=null\n'
            txt += 'DataTier=null\n'
            txt += 'ApplicationFamily=MCDataTier\n'
        if self.pset != None:
            pset = os.path.basename(job.configFilename())
            txt += '\n'
            txt += 'cp  $RUNTIME_AREA/'+pset+' .\n'
            if (self.datasetPath): # standard job
                txt += 'InputFiles=${args[1]}; export InputFiles\n'
                if (self.useParent==1):
                    txt += 'ParentFiles=${args[2]}; export ParentFiles\n'
                    txt += 'MaxEvents=${args[3]}; export MaxEvents\n'
                    txt += 'SkipEvents=${args[4]}; export SkipEvents\n'
                else:
                    txt += 'MaxEvents=${args[2]}; export MaxEvents\n'
                    txt += 'SkipEvents=${args[3]}; export SkipEvents\n'
                txt += 'echo "Inputfiles:<$InputFiles>"\n'
                if (self.useParent==1): txt += 'echo "ParentFiles:<$ParentFiles>"\n'
                txt += 'echo "MaxEvents:<$MaxEvents>"\n'
                txt += 'echo "SkipEvents:<$SkipEvents>"\n'
            else:  # pythia like job
                argNum = 1
                txt += 'PreserveSeeds='  + ','.join(self.preserveSeeds)  + '; export PreserveSeeds\n'
                txt += 'IncrementSeeds=' + ','.join(self.incrementSeeds) + '; export IncrementSeeds\n'
                txt += 'echo "PreserveSeeds: <$PreserveSeeds>"\n'
                txt += 'echo "IncrementSeeds:<$IncrementSeeds>"\n'
                if (self.firstRun):
                    txt += 'export FirstRun=${args[%s]}\n' % argNum
                    txt += 'echo "FirstRun: <$FirstRun>"\n'
                    argNum += 1
                if (self.generator == 'madgraph'):
                    txt += 'export FirstEvent=${args[%s]}\n' % argNum
                    txt += 'echo "FirstEvent:<$FirstEvent>"\n'
                    argNum += 1
                elif (self.generator == 'comphep'):
                    txt += 'export CompHEPFirstEvent=${args[%s]}\n' % argNum
                    txt += 'echo "CompHEPFirstEvent:<$CompHEPFirstEvent>"\n'
                    argNum += 1
                txt += 'MaxEvents=${args[%s]}; export MaxEvents\n' % argNum

            txt += 'mv -f ' + pset + ' ' + psetName + '\n'


        if self.pset != None:
            # FUTURE: Can simply for 2_1_x and higher
            txt += '\n'
            if self.debug_wrapper == 1:
                txt += 'echo "***** cat ' + psetName + ' *********"\n'
                txt += 'cat ' + psetName + '\n'
                txt += 'echo "****** end ' + psetName + ' ********"\n'
                txt += '\n'
                txt += 'echo "***********************" \n'
                txt += 'which edmConfigHash \n'
                txt += 'echo "***********************" \n'
            if (self.CMSSW_major >= 2 and self.CMSSW_minor >= 1) or (self.CMSSW_major >= 3):
                txt += 'edmConfigHash ' + psetName + ' \n'
                txt += 'PSETHASH=`edmConfigHash ' + psetName + '` \n'
            else:
                txt += 'PSETHASH=`edmConfigHash < ' + psetName + '` \n'
            txt += 'echo "PSETHASH = $PSETHASH" \n'
            txt += '\n'
        return txt

    def wsUntarSoftware(self, nj=0):
        """
        Put in the script the commands to build an executable
        or a library.
        """

        txt = '\n#Written by cms_cmssw::wsUntarSoftware\n'

        if os.path.isfile(self.tgzNameWithPath):
            txt += 'echo ">>> tar xzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+' :" \n'
            txt += 'tar xzf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+'\n'
            if  self.debug_wrapper==1 :
                txt += 'tar tzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+'\n'
                txt += 'ls -Al \n'
            txt += 'untar_status=$? \n'
            txt += 'if [ $untar_status -ne 0 ]; then \n'
            txt += '   echo "ERROR ==> Untarring .tgz file failed"\n'
            txt += '   job_exit_code=$untar_status\n'
            txt += '   func_exit\n'
            txt += 'else \n'
            txt += '   echo "Successful untar" \n'
            txt += 'fi \n'
            txt += '\n'
            txt += 'echo ">>> Include $RUNTIME_AREA in PYTHONPATH:"\n'
            txt += 'if [ -z "$PYTHONPATH" ]; then\n'
            txt += '   export PYTHONPATH=$RUNTIME_AREA/\n'
            txt += 'else\n'
            txt += '   export PYTHONPATH=$RUNTIME_AREA/:${PYTHONPATH}\n'
            txt += 'echo "PYTHONPATH=$PYTHONPATH"\n'
            txt += 'fi\n'
            txt += '\n'

            pass

        return txt

    def wsBuildExe(self, nj=0):
        """
        Put in the script the commands to build an executable
        or a library.
        """

        txt = '\n#Written by cms_cmssw::wsBuildExe\n'
        txt += 'echo ">>> moving CMSSW software directories in `pwd`" \n'

        txt += 'rm -r lib/ module/ \n'
        txt += 'mv $RUNTIME_AREA/lib/ . \n'
        txt += 'mv $RUNTIME_AREA/module/ . \n'
        if self.dataExist == True:
            txt += 'rm -r src/ \n'
            txt += 'mv $RUNTIME_AREA/src/ . \n'
        if len(self.additional_inbox_files)>0:
            for file in self.additional_inbox_files:
                txt += 'mv $RUNTIME_AREA/'+os.path.basename(file)+' . \n'
        # txt += 'mv $RUNTIME_AREA/ProdCommon/ . \n'
        # txt += 'mv $RUNTIME_AREA/IMProv/ . \n'

        txt += 'echo ">>> Include $RUNTIME_AREA in PYTHONPATH:"\n'
        txt += 'if [ -z "$PYTHONPATH" ]; then\n'
        txt += '   export PYTHONPATH=$RUNTIME_AREA/\n'
        txt += 'else\n'
        txt += '   export PYTHONPATH=$RUNTIME_AREA/:${PYTHONPATH}\n'
        txt += 'echo "PYTHONPATH=$PYTHONPATH"\n'
        txt += 'fi\n'
        txt += '\n'

        return txt


    def executableName(self):
        if self.scriptExe:
            return "sh "
        else:
            return self.executable

    def executableArgs(self):
        # FUTURE: This function tests the CMSSW version. Can be simplified as we drop support for old versions
        if self.scriptExe:
            return self.scriptExe + " $NJob"
        else:
            ex_args = ""
            ex_args += " -j $RUNTIME_AREA/crab_fjr_$NJob.xml"
            # Type of config file depends on CMSSW version
            if self.CMSSW_major >= 2 :
                ex_args += " -p pset.py"
            else:
                ex_args += " -p pset.cfg"
            return ex_args

    def inputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL input sandbox.
        """
        inp_box = []
        if os.path.isfile(self.tgzNameWithPath):
            inp_box.append(self.tgzNameWithPath)
        inp_box.append(common.work_space.jobDir() + self.scriptName)
        return inp_box

    def outputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL output sandbox.
        """
        out_box = []

        ## User Declared output files
        for out in (self.output_file+self.output_file_sandbox):
            n_out = nj + 1
            out_box.append(numberFile(out,str(n_out)))
        return out_box


    def wsRenameOutput(self, nj):
        """
        Returns part of a job script which renames the produced files.
        """

        txt = '\n#Written by cms_cmssw::wsRenameOutput\n'
        txt += 'echo ">>> current directory (SOFTWARE_DIR): $SOFTWARE_DIR" \n'
        txt += 'echo ">>> current directory content:"\n'
        if self.debug_wrapper==1:
            txt += 'ls -Al\n'
        txt += '\n'

        for fileWithSuffix in (self.output_file):
            output_file_num = numberFile(fileWithSuffix, '$NJob')
            txt += '\n'
            txt += '# check output file\n'
            txt += 'if [ -e ./'+fileWithSuffix+' ] ; then\n'
            if (self.copy_data == 1):  # For OSG nodes, file is in $WORKING_DIR, should not be moved to $RUNTIME_AREA
                txt += '    mv '+fileWithSuffix+' '+output_file_num+'\n'
                txt += '    ln -s `pwd`/'+output_file_num+' $RUNTIME_AREA/'+fileWithSuffix+'\n'
            else:
                txt += '    mv '+fileWithSuffix+' $RUNTIME_AREA/'+output_file_num+'\n'
                txt += '    ln -s $RUNTIME_AREA/'+output_file_num+' $RUNTIME_AREA/'+fileWithSuffix+'\n'
            txt += 'else\n'
            txt += '    job_exit_code=60302\n'
            txt += '    echo "WARNING: Output file '+fileWithSuffix+' not found"\n'
            if common.scheduler.name().upper() == 'CONDOR_G':
                txt += '    if [ $middleware == OSG ]; then \n'
                txt += '        echo "prepare dummy output file"\n'
                txt += '        echo "Processing of job output failed" > $RUNTIME_AREA/'+output_file_num+'\n'
                txt += '    fi \n'
            txt += 'fi\n'
        file_list = []
        for fileWithSuffix in (self.output_file):
             file_list.append(numberFile('$SOFTWARE_DIR/'+fileWithSuffix, '$NJob'))

        txt += 'file_list="'+string.join(file_list,',')+'"\n'
        txt += '\n'
        txt += 'echo ">>> current directory (SOFTWARE_DIR): $SOFTWARE_DIR" \n'
        txt += 'echo ">>> current directory content:"\n'
        if self.debug_wrapper==1:
            txt += 'ls -Al\n'
        txt += '\n'
        txt += 'cd $RUNTIME_AREA\n'
        txt += 'echo ">>> current directory (RUNTIME_AREA):  $RUNTIME_AREA"\n'
        return txt

    def getRequirements(self, nj=[]):
        """
        return job requirements to add to jdl files
        """
        req = ''
        if self.version:
            req='Member("VO-cms-' + \
                 self.version + \
                 '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'
        if self.executable_arch:
            req+=' && Member("VO-cms-' + \
                 self.executable_arch + \
                 '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'

        req = req + ' && (other.GlueHostNetworkAdapterOutboundIP)'
        if ( common.scheduler.name() == "glitecoll" ) or ( common.scheduler.name() == "glite"):
            req += ' && other.GlueCEStateStatus == "Production" '

        return req

    def configFilename(self):
        """ return the config filename """
        # FUTURE: Can remove cfg mode for CMSSW >= 2_1_x
        if (self.CMSSW_major >= 2 and self.CMSSW_minor >= 1) or (self.CMSSW_major >= 3):
          return self.name()+'.py'
        else:
          return self.name()+'.cfg'

    def wsSetupCMSOSGEnvironment_(self):
        """
        Returns part of a job script which is prepares
        the execution environment and which is common for all CMS jobs.
        """
        txt = '\n#Written by cms_cmssw::wsSetupCMSOSGEnvironment_\n'
        txt += '    echo ">>> setup CMS OSG environment:"\n'
        txt += '    echo "set SCRAM ARCH to ' + self.executable_arch + '"\n'
        txt += '    export SCRAM_ARCH='+self.executable_arch+'\n'
        txt += '    echo "SCRAM_ARCH = $SCRAM_ARCH"\n'
        txt += '    if [ -f $OSG_APP/cmssoft/cms/cmsset_default.sh ] ;then\n'
        txt += '      # Use $OSG_APP/cmssoft/cms/cmsset_default.sh to setup cms software\n'
        txt += '        source $OSG_APP/cmssoft/cms/cmsset_default.sh '+self.version+'\n'
        txt += '    else\n'
        txt += '        echo "ERROR ==> $OSG_APP/cmssoft/cms/cmsset_default.sh file not found"\n'
        txt += '        job_exit_code=10020\n'
        txt += '        func_exit\n'
        txt += '    fi\n'
        txt += '\n'
        txt += '    echo "==> setup cms environment ok"\n'
        txt += '    echo "SCRAM_ARCH = $SCRAM_ARCH"\n'

        return txt

    def wsSetupCMSLCGEnvironment_(self):
        """
        Returns part of a job script which is prepares
        the execution environment and which is common for all CMS jobs.
        """
        txt = '\n#Written by cms_cmssw::wsSetupCMSLCGEnvironment_\n'
        txt += '    echo ">>> setup CMS LCG environment:"\n'
        txt += '    echo "set SCRAM ARCH and BUILD_ARCH to ' + self.executable_arch + ' ###"\n'
        txt += '    export SCRAM_ARCH='+self.executable_arch+'\n'
        txt += '    export BUILD_ARCH='+self.executable_arch+'\n'
        txt += '    if [ ! $VO_CMS_SW_DIR ] ;then\n'
        txt += '        echo "ERROR ==> CMS software dir not found on WN `hostname`"\n'
        txt += '        job_exit_code=10031\n'
        txt += '        func_exit\n'
        txt += '    else\n'
        txt += '        echo "Sourcing environment... "\n'
        txt += '        if [ ! -s $VO_CMS_SW_DIR/cmsset_default.sh ] ;then\n'
        txt += '            echo "ERROR ==> cmsset_default.sh file not found into dir $VO_CMS_SW_DIR"\n'
        txt += '            job_exit_code=10020\n'
        txt += '            func_exit\n'
        txt += '        fi\n'
        txt += '        echo "sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '        source $VO_CMS_SW_DIR/cmsset_default.sh\n'
        txt += '        result=$?\n'
        txt += '        if [ $result -ne 0 ]; then\n'
        txt += '            echo "ERROR ==> problem sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '            job_exit_code=10032\n'
        txt += '            func_exit\n'
        txt += '        fi\n'
        txt += '    fi\n'
        txt += '    \n'
        txt += '    echo "==> setup cms environment ok"\n'
        return txt

    def wsModifyReport(self, nj):
        """
        insert the part of the script that modifies the FrameworkJob Report
        """

        txt = ''
        publish_data = int(self.cfg_params.get('USER.publish_data',0))
        if (publish_data == 1):
        #if (self.copy_data == 1):
            txt = '\n#Written by cms_cmssw::wsModifyReport\n'
            #publish_data = int(self.cfg_params.get('USER.publish_data',0))


            txt += 'if [ $StageOutExitStatus -eq 0 ]; then\n'
            txt += '    FOR_LFN=$LFNBaseName\n'
            txt += 'else\n'
            txt += '    FOR_LFN=/copy_problems/ \n'
            txt += 'fi\n'

            txt += 'echo ">>> Modify Job Report:" \n'
            txt += 'chmod a+x $RUNTIME_AREA/ProdCommon/FwkJobRep/ModifyJobReport.py\n'
            txt += 'echo "SE = $SE"\n'
            txt += 'echo "SE_PATH = $SE_PATH"\n'
            txt += 'echo "FOR_LFN = $FOR_LFN" \n'
            txt += 'echo "CMSSW_VERSION = $CMSSW_VERSION"\n\n'


            args = 'fjr $RUNTIME_AREA/crab_fjr_$NJob.xml n_job $NJob for_lfn $FOR_LFN PrimaryDataset $PrimaryDataset  ApplicationFamily $ApplicationFamily ApplicationName $executable cmssw_version $CMSSW_VERSION psethash $PSETHASH se_name $SE se_path $SE_PATH'
            #if (publish_data == 1):
            processedDataset = self.cfg_params['USER.publish_data_name']
            txt += 'ProcessedDataset='+processedDataset+'\n'
            txt += 'echo "ProcessedDataset = $ProcessedDataset"\n'
            args += ' UserProcessedDataset $USER-$ProcessedDataset-$PSETHASH'

            txt += 'echo "$RUNTIME_AREA/ProdCommon/FwkJobRep/ModifyJobReport.py '+str(args)+'"\n'
            txt += '$RUNTIME_AREA/ProdCommon/FwkJobRep/ModifyJobReport.py '+str(args)+'\n'
            txt += 'modifyReport_result=$?\n'
            txt += 'if [ $modifyReport_result -ne 0 ]; then\n'
            txt += '    modifyReport_result=70500\n'
            txt += '    job_exit_code=$modifyReport_result\n'
            txt += '    echo "ModifyReportResult=$modifyReport_result" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '    echo "WARNING: Problem with ModifyJobReport"\n'
            txt += 'else\n'
            txt += '    mv NewFrameworkJobReport.xml $RUNTIME_AREA/crab_fjr_$NJob.xml\n'
            txt += 'fi\n'
        return txt

    def wsParseFJR(self):
        """
        Parse the FrameworkJobReport to obtain useful infos
        """
        txt = '\n#Written by cms_cmssw::wsParseFJR\n'
        txt += 'echo ">>> Parse FrameworkJobReport crab_fjr.xml"\n'
        txt += 'if [ -s $RUNTIME_AREA/crab_fjr_$NJob.xml ]; then\n'
        txt += '    if [ -s $RUNTIME_AREA/parseCrabFjr.py ]; then\n'
        txt += '        cmd_out=`python $RUNTIME_AREA/parseCrabFjr.py --input $RUNTIME_AREA/crab_fjr_$NJob.xml --dashboard $MonitorID,$MonitorJobID '+self.debugWrap+'`\n'
        if self.debug_wrapper==1 :
            txt += '        echo "Result of parsing the FrameworkJobReport crab_fjr.xml: $cmd_out"\n'
        txt += '        executable_exit_status=`python $RUNTIME_AREA/parseCrabFjr.py --input $RUNTIME_AREA/crab_fjr_$NJob.xml --exitcode`\n'
        txt += '        if [ $executable_exit_status -eq 50115 ];then\n'
        txt += '            echo ">>> crab_fjr.xml contents: "\n'
        txt += '            cat $RUNTIME_AREA/crab_fjr_$NJob.xml\n'
        txt += '            echo "Wrong FrameworkJobReport --> does not contain useful info. ExitStatus: $executable_exit_status"\n'
        txt += '        elif [ $executable_exit_status -eq -999 ];then\n'
        txt += '            echo "ExitStatus from FrameworkJobReport not available. not available. Using exit code of executable from command line."\n'
        txt += '        else\n'
        txt += '            echo "Extracted ExitStatus from FrameworkJobReport parsing output: $executable_exit_status"\n'
        txt += '        fi\n'
        txt += '    else\n'
        txt += '        echo "CRAB python script to parse CRAB FrameworkJobReport crab_fjr.xml is not available, using exit code of executable from command line."\n'
        txt += '    fi\n'
          #### Patch to check input data reading for CMSSW16x Hopefully we-ll remove it asap
        txt += '    if [ $executable_exit_status -eq 0 ];then\n'
        txt += '        echo ">>> Executable succeded  $executable_exit_status"\n'
        if (self.datasetPath and not (self.dataset_pu or self.useParent==1)) :
          # VERIFY PROCESSED DATA
            txt += '        echo ">>> Verify list of processed files:"\n'
            txt += '        echo $InputFiles |tr -d \'\\\\\' |tr \',\' \'\\n\'|tr -d \'"\' > input-files.txt\n'
            txt += '        python $RUNTIME_AREA/parseCrabFjr.py --input $RUNTIME_AREA/crab_fjr_$NJob.xml --lfn > processed-files.txt\n'
            txt += '        cat input-files.txt  | sort | uniq > tmp.txt\n'
            txt += '        mv tmp.txt input-files.txt\n'
            txt += '        echo "cat input-files.txt"\n'
            txt += '        echo "----------------------"\n'
            txt += '        cat input-files.txt\n'
            txt += '        cat processed-files.txt | sort | uniq > tmp.txt\n'
            txt += '        mv tmp.txt processed-files.txt\n'
            txt += '        echo "----------------------"\n'
            txt += '        echo "cat processed-files.txt"\n'
            txt += '        echo "----------------------"\n'
            txt += '        cat processed-files.txt\n'
            txt += '        echo "----------------------"\n'
            txt += '        diff -qbB input-files.txt processed-files.txt\n'
            txt += '        fileverify_status=$?\n'
            txt += '        if [ $fileverify_status -ne 0 ]; then\n'
            txt += '            executable_exit_status=30001\n'
            txt += '            echo "ERROR ==> not all input files processed"\n'
            txt += '            echo "      ==> list of processed files from crab_fjr.xml differs from list in pset.cfg"\n'
            txt += '            echo "      ==> diff input-files.txt processed-files.txt"\n'
            txt += '        fi\n'
        txt += '    fi\n'
        txt += 'else\n'
        txt += '    echo "CRAB FrameworkJobReport crab_fjr.xml is not available, using exit code of executable from command line."\n'
        txt += 'fi\n'
        txt += '\n'
        txt += 'if [ $executable_exit_status -ne 0 ] && [ $executable_exit_status -ne 50115 ] && [ $executable_exit_status -ne 50117 ] && [ $executable_exit_status -ne 30001 ];then\n'
        txt += '    echo ">>> Executable failed  $executable_exit_status"\n'
        txt += '    echo "ExeExitCode=$executable_exit_status" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    echo "EXECUTABLE_EXIT_STATUS = $executable_exit_status"\n'
        txt += '    job_exit_code=$executable_exit_status\n'
        txt += '    func_exit\n'
        txt += 'fi\n\n'
        txt += 'echo "ExeExitCode=$executable_exit_status" | tee -a $RUNTIME_AREA/$repo\n'
        txt += 'echo "EXECUTABLE_EXIT_STATUS = $executable_exit_status"\n'
        txt += 'job_exit_code=$executable_exit_status\n'

        return txt

    def setParam_(self, param, value):
        self._params[param] = value

    def getParams(self):
        return self._params

    def uniquelist(self, old):
        """
        remove duplicates from a list
        """
        nd={}
        for e in old:
            nd[e]=0
        return nd.keys()

    def outList(self,list=False):
        """
        check the dimension of the output files
        """
        txt = ''
        txt += 'echo ">>> list of expected files on output sandbox"\n'
        listOutFiles = []
        stdout = 'CMSSW_$NJob.stdout'
        stderr = 'CMSSW_$NJob.stderr'
        if len(self.output_file) <= 0:
            msg ="WARNING: no output files name have been defined!!\n"
            msg+="\tno output files will be reported back/staged\n"
            common.logger.message(msg)
        if (self.return_data == 1):
            for file in (self.output_file+self.output_file_sandbox):
                listOutFiles.append(numberFile(file, '$NJob'))
            listOutFiles.append(stdout)
            listOutFiles.append(stderr)
        else:
            for file in (self.output_file_sandbox):
                listOutFiles.append(numberFile(file, '$NJob'))
            listOutFiles.append(stdout)
            listOutFiles.append(stderr)
        txt += 'echo "output files: '+string.join(listOutFiles,' ')+'"\n'
        txt += 'filesToCheck="'+string.join(listOutFiles,' ')+'"\n'
        txt += 'export filesToCheck\n'

        if list : return self.output_file
        return txt
