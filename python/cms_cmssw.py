
__revision__ = "$Id: cms_cmssw.py,v 1.350 2010/01/21 16:17:13 ewv Exp $"
__version__ = "$Revision: 1.350 $"

from JobType import JobType
from crab_exceptions import *
from crab_util import *
import common
import Scram
from Splitter import JobSplitter

from IMProv.IMProvNode import IMProvNode
import os, string, glob

class Cmssw(JobType):
    def __init__(self, cfg_params, ncjobs,skip_blocks, isNew):
        JobType.__init__(self, 'CMSSW')
        common.logger.debug('CMSSW::__init__')
        self.skip_blocks = skip_blocks
        self.argsList = 2
        self.NumEvents=0
        self._params = {}
        self.cfg_params = cfg_params

        ### Temporary patch to automatically skip the ISB size check:
        self.server = self.cfg_params.get('CRAB.server_name',None) or \
                      self.cfg_params.get('CRAB.use_server',0)
        self.local  = common.scheduler.name().upper() in ['LSF','CAF','CONDOR','SGE','PBS']
        size = 9.5
        if self.server or self.local:
            size = 99999
        self.MaxTarBallSize = float(self.cfg_params.get('GRID.maxtarballsize',size))

        # number of jobs requested to be created, limit obj splitting
        self.ncjobs = ncjobs

        self.scram = Scram.Scram(cfg_params)
        self.additional_inbox_files = []
        self.scriptExe = ''
        self.executable = ''
        self.executable_arch = self.scram.getArch()
        self.tgz_name = 'default.tgz'
        self.scriptName = 'CMSSW.sh'
        self.pset = ''
        self.datasetPath = ''

        self.tgzNameWithPath = common.work_space.pathForTgz()+self.tgz_name
        # set FJR file name
        self.fjrFileName = 'crab_fjr.xml'

        self.version = self.scram.getSWVersion()
        common.logger.log(10-1,"CMSSW version is: "+str(self.version))
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

        if self.CMSSW_major < 2 or (self.CMSSW_major == 2 and self.CMSSW_minor < 1):
            msg = "CRAB supports CMSSW >= 2_1_x only. Use an older CRAB version."
            raise CrabException(msg)
            """
            As CMSSW versions are dropped we can drop more code:
            2.x dropped: drop check for lumi range setting
            """

        ### collect Data cards

        ### Temporary: added to remove input file control in the case of PU
        self.dataset_pu = cfg_params.get('CMSSW.dataset_pu', None)

        tmp =  cfg_params['CMSSW.datasetpath']
        common.logger.log(10-1, "CMSSW::CMSSW(): datasetPath = "+tmp)

        if tmp =='':
            msg = "Error: datasetpath not defined "
            raise CrabException(msg)
        elif string.lower(tmp)=='none':
            self.datasetPath = None
            self.selectNoInput = 1
            self.primaryDataset = 'null'
        else:
            self.datasetPath = tmp
            self.selectNoInput = 0
            ll = len(self.datasetPath.split("/"))
            if (ll < 4):
                msg = 'Your datasetpath has a invalid format ' + self.datasetPath + '\n'
                msg += 'Expected a path in format /PRIMARY/PROCESSED/TIER1-TIER2 or /PRIMARY/PROCESSED/TIER/METHOD for ADS'
                raise CrabException(msg)
            self.primaryDataset = self.datasetPath.split("/")[1]
            self.dataTier = self.datasetPath.split("/")[2]

        # Analysis dataset is primary/processed/tier/definition
        self.ads = False
        if self.datasetPath:
            self.ads = len(self.datasetPath.split("/")) > 4
        self.lumiMask = self.cfg_params.get('CMSSW.lumi_mask',None)

        # FUTURE: Can remove this check
        if self.ads and self.CMSSW_major < 3:
            common.logger.info('Warning: Analysis dataset support is incomplete in CMSSW 2_x.')
            common.logger.info('  Only file level, not lumi level, granularity is supported.')

        self.debugWrap=''
        self.debug_wrapper = int(cfg_params.get('USER.debug_wrapper',0))
        if self.debug_wrapper == 1: self.debugWrap='--debug'

        ## now the application
        self.managedGenerators = ['madgraph', 'comphep', 'lhe']
        self.generator = cfg_params.get('CMSSW.generator','pythia').lower()
        self.executable = cfg_params.get('CMSSW.executable','cmsRun')
        common.logger.log(10-1, "CMSSW::CMSSW(): executable = "+self.executable)

        if not cfg_params.has_key('CMSSW.pset'):
            raise CrabException("PSet file missing. Cannot run cmsRun ")
        self.pset = cfg_params['CMSSW.pset']
        common.logger.log(10-1, "Cmssw::Cmssw(): PSet file = "+self.pset)
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

        self.AdditionalArgs = cfg_params.get('USER.script_arguments',None)
        if self.AdditionalArgs : self.AdditionalArgs = string.replace(self.AdditionalArgs,',',' ')

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
            common.logger.debug("Additional input files: "+str(self.additional_inbox_files))
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

        # Copy/return/publish
        self.copy_data = int(cfg_params.get('USER.copy_data',0))
        self.return_data = int(cfg_params.get('USER.return_data',0))
        self.publish_data = int(cfg_params.get('USER.publish_data',0))
        if (self.publish_data == 1):
            if not cfg_params.has_key('USER.publish_data_name'):
                raise CrabException('Cannot publish output data, because you did not specify USER.publish_data_name parameter in the crab.cfg file')
            else:
                self.processedDataset = cfg_params['USER.publish_data_name']
        """
            #### check of length of datasetname to publish ####
                common.logger.debug("test 100 char limit on datasetname")
                print "test 100 char limit on datasetname"
                ###
                len_file = 0
                print "self.output_file = ", self.output_file
                for file in self.output_file:
                    length = len(file)
                    if length > len_file:
                        len_file = length
                print "len_file = ", len_file
                common.logger.debug("len_file = " + str(len_file))
                ###
                user = getUserName()
                len_user_name = len(user)
                common.logger.debug("user = " + user)
                print "len_user_name = ", len_user_name
                common.logger.debug("len_user_name = " + str(len_user_name))

                len_processedDataset = len(self.processedDataset)
                common.logger.debug("processedDataset " + self.processedDataset)
                common.logger.debug("len_processedDataset = " + str(len_processedDataset))
                print "len_processedDataset = ", len_processedDataset

                if (self.datasetPath != None ):
                   len_primary = len(self.primaryDataset)
                   common.logger.debug("primaryDataset = " + self.primaryDataset)
                   common.logger.debug("len_primary = " + str(len_primary))
                   if (len_primary > 100):
                       raise CrabException("Warning: primary datasetname has to be < 100 characters")
                                               #500 - len_user_name - len_primary - 32 - 9 - 7 - output
                   #if (len_processedDataset > (59 - len_user_name - len_primary)):
                   if (len_processedDataset > ( 450 - len_user_name - len_primary - len_file)):
                      #raise CrabException("Warning: publication name too long. USER.publish_data_name has to be < " + str(59 - len_user_name - len_primary) + " characters")
                      raise CrabException("Warning: publication name too long. USER.publish_data_name has to be < " + str(450 - len_user_name - len_primary -len_file) + " characters")
                else:
                   #if (len_processedDataset > (59 - len_user_name) / 2):
                   #    raise CrabException("Warning: publication name too long. USER.publish_data_name has to be < " + str((59 - len_user_name) / 2) + " characters")
                   if (len_processedDataset > (450 - len_user_name -len_file) / 2):
                       raise CrabException("Warning: publication name too long. USER.publish_data_name has to be < " + str((450 - len_user_name - len_file) / 2) + " characters")
        """

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
#wmbs
        self.automation = int(self.cfg_params.get('WMBS.automation',0))
        if self.automation == 0:
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
            elif self.ads or self.lumiMask:
                self.algo = 'LumiBased'
            elif splitByRun ==1:
                self.algo = 'RunBased'
            else:
                self.algo = 'EventBased'
            common.logger.debug("Job splitting method: %s" % self.algo)

            splitter = JobSplitter(self.cfg_params,self.conf)
            self.dict = splitter.Algos()[self.algo]()

        self.argsFile= '%s/arguments.xml'%common.work_space.shareDir()
        self.rootArgsFilename= 'arguments'
        # modify Pset only the first time
        if isNew:
            if self.pset != None: self.ModifyPset()

            ## Prepare inputSandbox TarBall (only the first time)
            self.tarNameWithPath = self.getTarBall(self.executable)


    def ModifyPset(self):
        import PsetManipulator as pp

        # If pycfg_params set, fake out the config script
        # to make it think it was called with those args
        pycfg_params = self.cfg_params.get('CMSSW.pycfg_params',None)
        if pycfg_params:
            trueArgv = sys.argv
            sys.argv = [self.pset]
            sys.argv.extend(pycfg_params.split(' '))
        PsetEdit = pp.PsetManipulator(self.pset)
        if pycfg_params: # Restore original sys.argv
            sys.argv = trueArgv

        try:
            # Add FrameworkJobReport to parameter-set, set max events.
            # Reset later for data jobs by writeCFG which does all modifications
            PsetEdit.maxEvent(1)
            PsetEdit.skipEvent(0)
            PsetEdit.psetWriter(self.configFilename())
            ## If present, add TFileService to output files
            if not int(self.cfg_params.get('CMSSW.skip_tfileservice_output',0)):
                tfsOutput = PsetEdit.getTFileService()
                if tfsOutput:
                    if tfsOutput in self.output_file:
                        common.logger.debug("Output from TFileService "+tfsOutput+" already in output files")
                    else:
                        outfileflag = True #output found
                        self.output_file.append(tfsOutput)
                        common.logger.info("Adding "+tfsOutput+" (from TFileService) to list of output files")
                    pass
                pass
            # If present and requested, add PoolOutputModule to output files
            edmOutput = PsetEdit.getPoolOutputModule()
            if int(self.cfg_params.get('CMSSW.get_edm_output',0)):
                if edmOutput:
                    for outputFile in edmOutput:
                        if outputFile in self.output_file:
                            common.logger.debug("Output from PoolOutputModule "+outputFile+" already in output files")
                        else:
                            self.output_file.append(outputFile)
                            common.logger.info("Adding "+outputFile+" (from PoolOutputModule) to list of output files")
            # not requested, check anyhow to avoid accidental T2 overload
            else:
                if edmOutput:
                    missedFiles = []
                    for outputFile in edmOutput:
                        if outputFile not in self.output_file:
                            missedFiles.append(outputFile)
                    if missedFiles:
                        msg  = "ERROR: PoolOutputModule(s) are present in your ParameteSet %s \n"%self.pset
                        msg += "    but the file(s) produced ( %s ) are not in the list of output files\n" % ', '.join(missedFiles)
                        msg += "WARNING: please remove them. If you want to keep them, add the file(s) to output_files or use CMSSW.get_edm_output = 1\n"
                        if int(self.cfg_params.get('CMSSW.ignore_edm_output',0)):
                            msg += "    CMSSW.ignore_edm_output==1 : Hope you know what you are doing...\n"
                            common.logger.info(msg)
                        else :
                            raise CrabException(msg)

            if (PsetEdit.getBadFilesSetting()):
                msg = "WARNING: You have set skipBadFiles to True. This will continue processing on some errors and you may not be notified."
                common.logger.info(msg)

        except CrabException, msg:
            common.logger.info(str(msg))
            msg='Error while manipulating ParameterSet (see previous message, if any): exiting...'
            raise CrabException(msg)


    def DataDiscoveryAndLocation(self, cfg_params):

        import DataDiscovery
        import DataLocation
        common.logger.log(10-1,"CMSSW::DataDiscoveryAndLocation()")

        datasetPath=self.datasetPath

        ## Contact the DBS
        common.logger.info("Contacting Data Discovery Services ...")
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
        sites = self.filesbyblock.fromkeys(self.filesbyblock,'')
        for lfn in self.filesbyblock.keys():
            if unsorted_sites.has_key(lfn):
                sites[lfn]=unsorted_sites[lfn]
            else:
                sites[lfn]=[]

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
        [allSites.append(it) for it in allSites if not allSites.count(it)]


        # screen output
        if self.ads or self.lumiMask:
            common.logger.info("Requested (A)DS %s has %s block(s)." %
                               (datasetPath, len(self.filesbyblock.keys())))
        else:
            common.logger.info("Requested dataset: " + datasetPath + \
                " has " + str(self.maxEvents) + " events in " + \
                str(len(self.filesbyblock.keys())) + " blocks.\n")

        return sites


    def split(self, jobParams,firstJobID):

        jobParams = self.dict['args']
        njobs = self.dict['njobs']
        self.jobDestination = self.dict['jobDestination']

        if njobs == 0:
            raise CrabException("Asked to split zero jobs: aborting")
        if not self.server and not self.local and njobs > 500:
            raise CrabException("The CRAB client will not submit more than 500 jobs. You must use the server mode.")

        # create the empty structure
        for i in range(njobs):
            jobParams.append("")

        listID=[]
        listField=[]
        listDictions=[]
        exist= os.path.exists(self.argsFile)
        for id in range(njobs):
            job = id + int(firstJobID)
            listID.append(job+1)
            job_ToSave ={}
            concString = ' '
            argu=''
            str_argu = str(job+1)
            if len(jobParams[id]):
                argu = {'JobID': job+1}
                for i in range(len(jobParams[id])):
                    argu[self.dict['params'][i]]=jobParams[id][i]
                    if len(jobParams[id])==1: self.NumEvents = jobParams[id][i]
                # just for debug
                str_argu += concString.join(jobParams[id])
            if argu != '': listDictions.append(argu)
            job_ToSave['arguments']= '%d %d'%( (job+1), 0)
            job_ToSave['dlsDestination']= self.jobDestination[id]
            listField.append(job_ToSave)
            from ProdCommon.SiteDB.CmsSiteMapper import CmsSEMap
            cms_se = CmsSEMap()
            msg="Job  %s  Arguments:  %s\n"%(str(job+1),str_argu)
            msg+="\t  Destination: %s "%(str(self.jobDestination[id]))
            SEDestination = [cms_se[dest] for dest in self.jobDestination[id]]
            msg+="\t  CMSDestination: %s "%(str(SEDestination))
            common.logger.log(10-1,msg)
        # write xml
        if len(listDictions):
            if exist==False: self.CreateXML()
            self.addEntry(listDictions)
           # self.zipXMLfile()
        common._db.updateJob_(listID,listField)
        return

   # def zipXMLfile(self):

    #    import tarfile
    #    try:
    #        tar = tarfile.open(self.tarNameWithPath, "a")
    #        tar.add(self.argsFile, os.path.basename(self.argsFile))
    #        tar.close()
    #    except IOError, exc:
    #        msg = 'Could not add %s to %s \n'%(self.argsFile,self.tarNameWithPath)
    #        msg += str(exc)
    #        raise CrabException(msg)
    #    except tarfile.TarError, exc:
    #        msg = 'Could not add %s to %s \n'%(self.argsFile,self.tarNameWithPath)
    #        msg += str(exc)
    #        raise CrabException(msg)

    def CreateXML(self):
        """
        """
        result = IMProvNode( self.rootArgsFilename )
        outfile = file( self.argsFile, 'w').write(str(result))
        return

    def addEntry(self, listDictions):
        """
        _addEntry_

        add an entry to the xml file
        """
        from IMProv.IMProvLoader import loadIMProvFile
        ## load xml
        improvDoc = loadIMProvFile(self.argsFile)
        entrname= 'Job'
        for dictions in listDictions:
           report = IMProvNode(entrname , None, **dictions)
           improvDoc.addNode(report)
        outfile = file( self.argsFile, 'w').write(str(improvDoc))
        return

    def numberOfJobs(self):
#wmbs
        if self.automation==0:
           return self.dict['njobs']
        else:
           return None

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
            common.logger.debug("swArea = "+swArea+" swReleaseTop ="+swReleaseTop)
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
                    common.logger.debug("Exe "+exeWithPath+" to be tarred")
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
            common.logger.debug("lib "+lib+" to be tarred")
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
                        common.logger.debug("data "+entry+" to be tarred")
                        tar.add(swArea+"/src/"+entry,"src/"+entry)
                    pass
                pass

            ### CMSSW ParameterSet
            if not self.pset is None:
                cfg_file = common.work_space.jobDir()+self.configFilename()
                tar.add(cfg_file,self.configFilename())

            try:
                crab_cfg_file = common.work_space.shareDir()+'/crab.cfg'
                tar.add(crab_cfg_file,'crab.cfg')
            except:
                pass

            ## Add ProdCommon dir to tar
            prodcommonDir = './'
            prodcommonPath = os.environ['CRABDIR'] + '/' + 'external/'
            neededStuff = ['ProdCommon/__init__.py','ProdCommon/FwkJobRep', 'ProdCommon/CMSConfigTools', \
                           'ProdCommon/Core', 'ProdCommon/MCPayloads', 'IMProv', 'ProdCommon/Storage', \
                           'WMCore/__init__.py','WMCore/Algorithms']
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
            common.logger.log(10-1,"Files in "+self.tgzNameWithPath+" : "+str(tar.getnames()))

            tar.close()
        except IOError, exc:
            msg = 'Could not create tar-ball %s \n'%self.tgzNameWithPath
            msg += str(exc)
            raise CrabException(msg)
        except tarfile.TarError, exc:
            msg = 'Could not create tar-ball %s \n'%self.tgzNameWithPath
            msg += str(exc)
            raise CrabException(msg)

        tarballinfo = os.stat(self.tgzNameWithPath)
        if ( tarballinfo.st_size > self.MaxTarBallSize*1024*1024 ) :
            msg  = 'Input sandbox size of ' + str(float(tarballinfo.st_size)/1024.0/1024.0) + ' MB is larger than the allowed ' + str(self.MaxTarBallSize) \
               +'MB input sandbox limit \n'
            msg += '      and not supported by the direct GRID submission system.\n'
            msg += '      Please use the CRAB server mode by setting server_name=<NAME> in section [CRAB] of your crab.cfg.\n'
            msg += '      For further infos please see https://twiki.cern.ch/twiki/bin/view/CMS/CrabServerForUsers#Server_available_for_users'
            raise CrabException(msg)

        ## create tar-ball with ML stuff

    def wsSetupEnvironment(self, nj=0):
        """
        Returns part of a job script which prepares
        the execution environment for the job 'nj'.
        """
        psetName = 'pset.py'

        # Prepare JobType-independent part
        txt = '\n#Written by cms_cmssw::wsSetupEnvironment\n'
        txt += 'echo ">>> setup environment"\n'
        txt += 'echo "set SCRAM ARCH to ' + self.executable_arch + '"\n'
        txt += 'export SCRAM_ARCH=' + self.executable_arch + '\n'
        txt += 'echo "SCRAM_ARCH = $SCRAM_ARCH"\n'
        txt += 'if [ $middleware == LCG ] || [ $middleware == CAF ] || [ $middleware == LSF ]; then \n'
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

        txt += 'elif [ $middleware == ARC ]; then\n'
        txt += self.wsSetupCMSLCGEnvironment_()

        #Setup PBS Environment
        txt += 'elif [ $middleware == PBS ]; then\n'
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
        txt += "## number of arguments (first argument always jobnumber, the second is the resubmission number)\n"
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
            #self.primaryDataset = self.datasetPath.split("/")[1]
            #DataTier = self.datasetPath.split("/")[2]
            txt += '\n'
            txt += 'DatasetPath='+self.datasetPath+'\n'

            txt += 'PrimaryDataset='+self.primaryDataset +'\n'
            txt += 'DataTier='+self.dataTier+'\n'
            txt += 'ApplicationFamily=cmsRun\n'

        else:
            #self.primaryDataset = 'null'
            txt += 'DatasetPath=MCDataTier\n'
            txt += 'PrimaryDataset=null\n'
            txt += 'DataTier=null\n'
            txt += 'ApplicationFamily=MCDataTier\n'
        if self.pset != None:
            pset = os.path.basename(job.configFilename())
            txt += '\n'
            txt += 'cp  $RUNTIME_AREA/'+pset+' .\n'

            txt += 'PreserveSeeds='  + ','.join(self.preserveSeeds)  + '; export PreserveSeeds\n'
            txt += 'IncrementSeeds=' + ','.join(self.incrementSeeds) + '; export IncrementSeeds\n'
            txt += 'echo "PreserveSeeds: <$PreserveSeeds>"\n'
            txt += 'echo "IncrementSeeds:<$IncrementSeeds>"\n'

            txt += 'mv -f ' + pset + ' ' + psetName + '\n'
        else:
            txt += '\n'
            if self.AdditionalArgs: txt += 'export AdditionalArgs=\"%s\"\n'%(self.AdditionalArgs)
            if int(self.NumEvents) != 0: txt += 'export MaxEvents=%s\n'%str(self.NumEvents)
        return txt

    def wsUntarSoftware(self, nj=0):
        """
        Put in the script the commands to build an executable
        or a library.
        """

        txt = '\n#Written by cms_cmssw::wsUntarSoftware\n'

        if os.path.isfile(self.tgzNameWithPath):
            txt += 'echo ">>> tar xzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+' :" \n'
            txt += 'tar zxvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+'\n'
            if  self.debug_wrapper==1 :
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

        if self.pset != None:
            psetName = 'pset.py'

            txt += '\n'
            if self.debug_wrapper == 1:
                txt += 'echo "***** cat ' + psetName + ' *********"\n'
                txt += 'cat ' + psetName + '\n'
                txt += 'echo "****** end ' + psetName + ' ********"\n'
                txt += '\n'
                txt += 'echo "***********************" \n'
                txt += 'which edmConfigHash \n'
                txt += 'echo "***********************" \n'
            txt += 'edmConfigHash ' + psetName + ' \n'
            txt += 'PSETHASH=`edmConfigHash ' + psetName + '` \n'
            txt += 'echo "PSETHASH = $PSETHASH" \n'
            #### FEDE temporary fix for noEdm files #####
            txt += 'if [ -z "$PSETHASH" ]; then \n'
            txt += '   export PSETHASH=null\n'
            txt += 'fi \n'
            #############################################
            txt += '\n'
        return txt


    def executableName(self):
        if self.scriptExe:
            return "sh "
        else:
            return self.executable

    def executableArgs(self):
        if self.scriptExe:
            return self.scriptExe + " $NJob $AdditionalArgs"
        else:
            return " -j $RUNTIME_AREA/crab_fjr_$NJob.xml -p pset.py"

    def inputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL input sandbox.
        """
        inp_box = []
        if os.path.isfile(self.tgzNameWithPath):
            inp_box.append(self.tgzNameWithPath)
        if os.path.isfile(self.argsFile):
            inp_box.append(self.argsFile)
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
            output_file_num = numberFile(fileWithSuffix, '$OutUniqueID')
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
             file_list.append(numberFile('$SOFTWARE_DIR/'+fileWithSuffix, '$OutUniqueID'))

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
        if ( common.scheduler.name() in ["glitecoll", "glite","glite_slc5"] ):
            ## 25-Jun-2009 SL: patch to use Cream enabled WMS
            if ( self.cfg_params.get('GRID.use_cream',None) ):
                req += ' && (other.GlueCEStateStatus == "Production" || other.GlueCEStateStatus == "Special")'
            else:
                req += ' && other.GlueCEStateStatus == "Production" '

        return req

    def configFilename(self):
        """ return the config filename """
        return self.name()+'.py'

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
        if (self.copy_data == 1):
            txt = '\n#Written by cms_cmssw::wsModifyReport\n'


            txt += 'if [ $StageOutExitStatus -eq 0 ] || [ $StageOutExitStatus -eq 60308 ] ; then\n'
            txt += '    FOR_LFN=$LFNBaseName\n'
            txt += 'else\n'
            txt += '    FOR_LFN=/copy_problems/ \n'
            txt += 'fi\n'

            txt += 'echo ">>> Modify Job Report:" \n'
            txt += 'chmod a+x $RUNTIME_AREA/ProdCommon/FwkJobRep/ModifyJobReport.py\n'
            txt += 'echo "SE = $SE"\n'
            #### FEDE changing SE_PATH with the endpoint
            txt += 'echo "endpoint = $endpoint"\n'
            txt += 'SE_PATH=$endpoint\n'
            txt += 'echo "SE_PATH = $endpoint"\n'
            txt += 'echo "FOR_LFN = $FOR_LFN" \n'
            txt += 'echo "CMSSW_VERSION = $CMSSW_VERSION"\n\n'


            args = 'fjr $RUNTIME_AREA/crab_fjr_$NJob.xml n_job $OutUniqueID for_lfn $FOR_LFN PrimaryDataset $PrimaryDataset  ApplicationFamily $ApplicationFamily ApplicationName $executable cmssw_version $CMSSW_VERSION psethash $PSETHASH se_name $SE se_path $SE_PATH file_list $file_list'
            if (self.publish_data == 1):
                txt += 'ProcessedDataset='+self.processedDataset+'\n'
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
        ## This cannot more work given the changes on the Job argumentsJob
        """
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
        """
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
            common.logger.info(msg)

        if (self.return_data == 1):
            for file in (self.output_file):
                listOutFiles.append(numberFile(file, '$OutUniqueID'))
        for file in (self.output_file_sandbox):
            listOutFiles.append(numberFile(file, '$NJob'))
        listOutFiles.append(stdout)
        listOutFiles.append(stderr)

        txt += 'echo "output files: '+string.join(listOutFiles,' ')+'"\n'
        txt += 'filesToCheck="'+string.join(listOutFiles,' ')+'"\n'
        txt += 'export filesToCheck\n'
        taskinfo={}
        taskinfo['outfileBasename'] = self.output_file
        common._db.updateTask_(taskinfo)

        if list : return self.output_file
        return txt
