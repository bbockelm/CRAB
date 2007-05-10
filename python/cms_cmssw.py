from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
import common
import PsetManipulator  
import DataDiscovery
import DataDiscovery_DBS2
import DataLocation
import Scram

import os, string, re, shutil, glob

class Cmssw(JobType):
    def __init__(self, cfg_params, ncjobs):
        JobType.__init__(self, 'CMSSW')
        common.logger.debug(3,'CMSSW::__init__')

        # Marco.
        self._params = {}
        self.cfg_params = cfg_params

        try:
            self.MaxTarBallSize = float(self.cfg_params['EDG.maxtarballsize'])
        except KeyError:
            self.MaxTarBallSize = 100.0

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
        self.pset = ''      #scrip use case Da   
        self.datasetPath = '' #scrip use case Da

        # set FJR file name
        self.fjrFileName = 'crab_fjr.xml'

        self.version = self.scram.getSWVersion()
        common.taskDB.setDict('codeVersion',self.version)
        self.setParam_('application', self.version)

        ### collect Data cards

        ## get DBS mode
        try:
            self.use_dbs_1 = int(self.cfg_params['CMSSW.use_dbs_1'])
        except KeyError:
            self.use_dbs_1 = 0
            
        try:
            tmp =  cfg_params['CMSSW.datasetpath']
            log.debug(6, "CMSSW::CMSSW(): datasetPath = "+tmp)
            if string.lower(tmp)=='none':
                self.datasetPath = None
                self.selectNoInput = 1
            else:
                self.datasetPath = tmp
                self.selectNoInput = 0
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
<<<<<<< cms_cmssw.py
            if self.use_dbs_1 == 1 :
                self.setParam_('dataset', datasetpath_split[1])
                self.setParam_('owner', datasetpath_split[-1])
            else:
                self.setParam_('dataset', datasetpath_split[1])
                self.setParam_('owner', datasetpath_split[2])
=======
            self.setParam_('dataset', datasetpath_split[1])
            self.setParam_('owner', datasetpath_split[-1])
>>>>>>> 1.79

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
            if self.pset.lower() != 'none' : 
                if (not os.path.exists(self.pset)):
                    raise CrabException("User defined PSet file "+self.pset+" does not exist")
            else:
                self.pset = None
        except KeyError:
            raise CrabException("PSet file missing. Cannot run cmsRun ")

        # output files
        ## stuff which must be returned always via sandbox
        self.output_file_sandbox = []

        # add fjr report by default via sandbox
        self.output_file_sandbox.append(self.fjrFileName)

        # other output files to be returned via sandbox or copied to SE
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
                log.message("No output file defined: only stdout/err and the CRAB Framework Job Report will be available")
                pass
            pass
        except KeyError:
            log.message("No output file defined: only stdout/err and the CRAB Framework Job Report will be available")
            pass

        # script_exe file as additional file in inputSandbox
        try:
            self.scriptExe = cfg_params['USER.script_exe']
            if self.scriptExe != '':
               if not os.path.isfile(self.scriptExe):
                  msg ="ERROR. file "+self.scriptExe+" not found"
                  raise CrabException(msg)
               self.additional_inbox_files.append(string.strip(self.scriptExe))
        except KeyError:
            self.scriptExe = ''

        #CarlosDaniele
        if self.datasetPath == None and self.pset == None and self.scriptExe == '' :
           msg ="Error. script_exe  not defined"
           raise CrabException(msg)

        ## additional input files
        try:
            tmpAddFiles = string.split(cfg_params['USER.additional_input_files'],',')
            for tmp in tmpAddFiles:
                tmp = string.strip(tmp)
                dirname = ''
                if not tmp[0]=="/": dirname = "."
                files = glob.glob(os.path.join(dirname, tmp))
                for file in files:
                    if not os.path.exists(file):
                        raise CrabException("Additional input file not found: "+file)
                    pass
                    storedFile = common.work_space.pathForTgz()+file
                    shutil.copyfile(file, storedFile)
                    self.additional_inbox_files.append(string.strip(storedFile))
                pass
            pass
            common.logger.debug(5,"Additional input files: "+str(self.additional_inbox_files))
        except KeyError:
            pass

        # files per job
        try:
            if (cfg_params['CMSSW.files_per_jobs']):
                raise CrabException("files_per_jobs no longer supported.  Quitting.")
        except KeyError:
            pass

        ## Events per job
        try:
            self.eventsPerJob =int( cfg_params['CMSSW.events_per_job'])
            self.selectEventsPerJob = 1
        except KeyError:
            self.eventsPerJob = -1
            self.selectEventsPerJob = 0
    
        ## number of jobs
        try:
            self.theNumberOfJobs =int( cfg_params['CMSSW.number_of_jobs'])
            self.selectNumberOfJobs = 1
        except KeyError:
            self.theNumberOfJobs = 0
            self.selectNumberOfJobs = 0

        try:
            self.total_number_of_events = int(cfg_params['CMSSW.total_number_of_events'])
            self.selectTotalNumberEvents = 1
        except KeyError:
            self.total_number_of_events = 0
            self.selectTotalNumberEvents = 0

        if self.pset != None: #CarlosDaniele 
             if ( (self.selectTotalNumberEvents + self.selectEventsPerJob + self.selectNumberOfJobs) != 2 ):
                 msg = 'Must define exactly two of total_number_of_events, events_per_job, or number_of_jobs.'
                 raise CrabException(msg)
        else:
             if (self.selectNumberOfJobs == 0):
                 msg = 'Must specify  number_of_jobs.'
                 raise CrabException(msg)

        ## source seed for pythia
        try:
            self.sourceSeed = int(cfg_params['CMSSW.pythia_seed'])
        except KeyError:
            self.sourceSeed = None
            common.logger.debug(5,"No seed given")

        try:
            self.sourceSeedVtx = int(cfg_params['CMSSW.vtx_seed'])
        except KeyError:
            self.sourceSeedVtx = None
            common.logger.debug(5,"No vertex seed given")
        try:
            self.firstRun = int(cfg_params['CMSSW.first_run'])
        except KeyError:
            self.firstRun = None
            common.logger.debug(5,"No first run given")
        if self.pset != None: #CarlosDaniele
            self.PsetEdit = PsetManipulator.PsetManipulator(self.pset) #Daniele Pset

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

        self.tgzNameWithPath = self.getTarBall(self.executable)
    
        ## Select Splitting
        if self.selectNoInput: 
            if self.pset == None: #CarlosDaniele
                self.jobSplittingForScript()
            else:
                self.jobSplittingNoInput()
        else: 
            self.jobSplittingByBlocks(blockSites)

        # modify Pset
        if self.pset != None: #CarlosDaniele
            try:
                if (self.datasetPath): # standard job
                    # allow to processa a fraction of events in a file
                    self.PsetEdit.inputModule("INPUT")
                    self.PsetEdit.maxEvent("INPUTMAXEVENTS")
                    self.PsetEdit.skipEvent("INPUTSKIPEVENTS")
                else:  # pythia like job
                    self.PsetEdit.maxEvent(self.eventsPerJob)
                    if (self.firstRun):
                        self.PsetEdit.pythiaFirstRun("INPUTFIRSTRUN")  #First Run
                    if (self.sourceSeed) :
                        self.PsetEdit.pythiaSeed("INPUT")
                        if (self.sourceSeedVtx) :
                            self.PsetEdit.pythiaSeedVtx("INPUTVTX")
                # add FrameworkJobReport to parameter-set
                self.PsetEdit.addCrabFJR(self.fjrFileName)
                self.PsetEdit.psetWriter(self.configFilename())
            except:
                msg='Error while manipuliating ParameterSet: exiting...'
                raise CrabException(msg)

    def DataDiscoveryAndLocation(self, cfg_params):

        common.logger.debug(10,"CMSSW::DataDiscoveryAndLocation()")

        datasetPath=self.datasetPath

        ## Contact the DBS
        common.logger.message("Contacting DBS...")
        try:

            if self.use_dbs_1 == 1 :
                self.pubdata=DataDiscovery.DataDiscovery(datasetPath, cfg_params)
            else :
                self.pubdata=DataDiscovery_DBS2.DataDiscovery_DBS2(datasetPath, cfg_params)
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
        except DataDiscovery_DBS2.NotExistingDatasetError_DBS2, ex :
            msg = 'ERROR ***: failed Data Discovery in DBS : %s'%ex.getErrorMessage()
            raise CrabException(msg)
        except DataDiscovery_DBS2.NoDataTierinProvenanceError_DBS2, ex :
            msg = 'ERROR ***: failed Data Discovery in DBS : %s'%ex.getErrorMessage()
            raise CrabException(msg)
        except DataDiscovery_DBS2.DataDiscoveryError_DBS2, ex:
            msg = 'ERROR ***: failed Data Discovery in DBS :  %s'%ex.getErrorMessage()
            raise CrabException(msg)

        ## get list of all required data in the form of dbs paths  (dbs path = /dataset/datatier/owner)
        common.logger.message("Required data are :"+self.datasetPath)

        self.filesbyblock=self.pubdata.getFiles()
        self.eventsbyblock=self.pubdata.getEventsPerBlock()
        self.eventsbyfile=self.pubdata.getEventsPerFile()

        ## get max number of events
        self.maxEvents=self.pubdata.getMaxEvents() ##  self.maxEvents used in Creator.py 
        common.logger.message("The number of available events is %s\n"%self.maxEvents)

        common.logger.message("Contacting DLS...")
        ## Contact the DLS and build a list of sites hosting the fileblocks
        try:
            dataloc=DataLocation.DataLocation(self.filesbyblock.keys(),cfg_params)
            dataloc.fetchDLSInfo()
        except DataLocation.DataLocationError , ex:
            msg = 'ERROR ***: failed Data Location in DLS \n %s '%ex.getErrorMessage()
            raise CrabException(msg)
        

        sites = dataloc.getSites()
        allSites = []
        listSites = sites.values()
        for listSite in listSites:
            for oneSite in listSite:
                allSites.append(oneSite)
        allSites = self.uniquelist(allSites)

        common.logger.message("Sites ("+str(len(allSites))+") hosting part/all of dataset: "+str(allSites)) 
        common.logger.debug(6, "List of Sites: "+str(allSites))
        return sites
    
    def jobSplittingByBlocks(self, blockSites):
        """
        Perform job splitting. Jobs run over an integer number of files
        and no more than one block.
        ARGUMENT: blockSites: dictionary with blocks as keys and list of host sites as values
        REQUIRES: self.selectTotalNumberEvents, self.selectEventsPerJob, self.selectNumberofJobs,
                  self.total_number_of_events, self.eventsPerJob, self.theNumberOfJobs,
                  self.maxEvents, self.filesbyblock
        SETS: self.jobDestination - Site destination(s) for each job (a list of lists)
              self.total_number_of_jobs - Total # of jobs
              self.list_of_args - File(s) job will run on (a list of lists)
        """

        # ---- Handle the possible job splitting configurations ---- #
        if (self.selectTotalNumberEvents):
            totalEventsRequested = self.total_number_of_events
        if (self.selectEventsPerJob):
            eventsPerJobRequested = self.eventsPerJob
            if (self.selectNumberOfJobs):
                totalEventsRequested = self.theNumberOfJobs * self.eventsPerJob

        # If user requested all the events in the dataset
        if (totalEventsRequested == -1):
            eventsRemaining=self.maxEvents
        # If user requested more events than are in the dataset
        elif (totalEventsRequested > self.maxEvents):
            eventsRemaining = self.maxEvents
            common.logger.message("Requested "+str(self.total_number_of_events)+ " events, but only "+str(self.maxEvents)+" events are available.")
        # If user requested less events than are in the dataset
        else:
            eventsRemaining = totalEventsRequested

        # If user requested more events per job than are in the dataset
        if (self.selectEventsPerJob and eventsPerJobRequested > self.maxEvents):
            eventsPerJobRequested = self.maxEvents

        # For user info at end
        totalEventCount = 0

        if (self.selectTotalNumberEvents and self.selectNumberOfJobs):
            eventsPerJobRequested = int(eventsRemaining/self.theNumberOfJobs)

        if (self.selectNumberOfJobs):
            common.logger.message("May not create the exact number_of_jobs requested.")

        if ( self.ncjobs == 'all' ) :
            totalNumberOfJobs = 999999999
        else :
            totalNumberOfJobs = self.ncjobs
            

        blocks = blockSites.keys()
        blockCount = 0
        # Backup variable in case self.maxEvents counted events in a non-included block
        numBlocksInDataset = len(blocks)

        jobCount = 0
        list_of_lists = []

        # ---- Iterate over the blocks in the dataset until ---- #
        # ---- we've met the requested total # of events    ---- #
        while ( (eventsRemaining > 0) and (blockCount < numBlocksInDataset) and (jobCount < totalNumberOfJobs)):
            block = blocks[blockCount]
            blockCount += 1
            
            if self.eventsbyblock.has_key(block) :
                numEventsInBlock = self.eventsbyblock[block]
                common.logger.debug(5,'Events in Block File '+str(numEventsInBlock))
            
                files = self.filesbyblock[block]
                numFilesInBlock = len(files)
                if (numFilesInBlock <= 0):
                    continue
                fileCount = 0

                # ---- New block => New job ---- #
                parString = "\\{"
                # counter for number of events in files currently worked on
                filesEventCount = 0
                # flag if next while loop should touch new file
                newFile = 1
                # job event counter
                jobSkipEventCount = 0
            
                # ---- Iterate over the files in the block until we've met the requested ---- #
                # ---- total # of events or we've gone over all the files in this block  ---- #
                while ( (eventsRemaining > 0) and (fileCount < numFilesInBlock) and (jobCount < totalNumberOfJobs) ):
                    file = files[fileCount]
                    if newFile :
                        try:
                            numEventsInFile = self.eventsbyfile[file]
                            common.logger.debug(6, "File "+str(file)+" has "+str(numEventsInFile)+" events")
                            # increase filesEventCount
                            filesEventCount += numEventsInFile
                            # Add file to current job
                            parString += '\\\"' + file + '\\\"\,'
                            newFile = 0
                        except KeyError:
                            common.logger.message("File "+str(file)+" has unknown number of events: skipping")
                        

                    # if less events in file remain than eventsPerJobRequested
                    if ( filesEventCount - jobSkipEventCount < eventsPerJobRequested ) :
                        # if last file in block
                        if ( fileCount == numFilesInBlock-1 ) :
                            # end job using last file, use remaining events in block
                            # close job and touch new file
                            fullString = parString[:-2]
                            fullString += '\\}'
                            list_of_lists.append([fullString,str(-1),str(jobSkipEventCount)])
                            common.logger.debug(3,"Job "+str(jobCount+1)+" can run over "+str(filesEventCount - jobSkipEventCount)+" events (last file in block).")
                            self.jobDestination.append(blockSites[block])
                            common.logger.debug(5,"Job "+str(jobCount+1)+" Destination: "+str(self.jobDestination[jobCount]))
                            # reset counter
                            jobCount = jobCount + 1
                            totalEventCount = totalEventCount + filesEventCount - jobSkipEventCount
                            eventsRemaining = eventsRemaining - filesEventCount + jobSkipEventCount
                            jobSkipEventCount = 0
                            # reset file
                            parString = "\\{"
                            filesEventCount = 0
                            newFile = 1
                            fileCount += 1
                        else :
                            # go to next file
                            newFile = 1
                            fileCount += 1
                    # if events in file equal to eventsPerJobRequested
                    elif ( filesEventCount - jobSkipEventCount == eventsPerJobRequested ) :
                        # close job and touch new file
                        fullString = parString[:-2]
                        fullString += '\\}'
                        list_of_lists.append([fullString,str(eventsPerJobRequested),str(jobSkipEventCount)])
                        common.logger.debug(3,"Job "+str(jobCount+1)+" can run over "+str(eventsPerJobRequested)+" events.")
                        self.jobDestination.append(blockSites[block])
                        common.logger.debug(5,"Job "+str(jobCount+1)+" Destination: "+str(self.jobDestination[jobCount]))
                        # reset counter
                        jobCount = jobCount + 1
                        totalEventCount = totalEventCount + eventsPerJobRequested
                        eventsRemaining = eventsRemaining - eventsPerJobRequested
                        jobSkipEventCount = 0
                        # reset file
                        parString = "\\{"
                        filesEventCount = 0
                        newFile = 1
                        fileCount += 1
                        
                    # if more events in file remain than eventsPerJobRequested
                    else :
                        # close job but don't touch new file
                        fullString = parString[:-2]
                        fullString += '\\}'
                        list_of_lists.append([fullString,str(eventsPerJobRequested),str(jobSkipEventCount)])
                        common.logger.debug(3,"Job "+str(jobCount+1)+" can run over "+str(eventsPerJobRequested)+" events.")
                        self.jobDestination.append(blockSites[block])
                        common.logger.debug(5,"Job "+str(jobCount+1)+" Destination: "+str(self.jobDestination[jobCount]))
                        # increase counter
                        jobCount = jobCount + 1
                        totalEventCount = totalEventCount + eventsPerJobRequested
                        eventsRemaining = eventsRemaining - eventsPerJobRequested
                        # calculate skip events for last file
                        # use filesEventCount (contains several files), jobSkipEventCount and eventsPerJobRequest
                        jobSkipEventCount = eventsPerJobRequested - (filesEventCount - jobSkipEventCount - self.eventsbyfile[file])
                        # remove all but the last file
                        filesEventCount = self.eventsbyfile[file]
                        parString = "\\{"
                        parString += '\\\"' + file + '\\\"\,'
                    pass # END if
                pass # END while (iterate over files in the block)
        pass # END while (iterate over blocks in the dataset)
        self.ncjobs = self.total_number_of_jobs = jobCount
        if (eventsRemaining > 0 and jobCount < totalNumberOfJobs ):
            common.logger.message("Could not run on all requested events because some blocks not hosted at allowed sites.")
        common.logger.message("\n"+str(jobCount)+" job(s) can run on "+str(totalEventCount)+" events.\n")
        
        self.list_of_args = list_of_lists
        return

    def jobSplittingNoInput(self):
        """
        Perform job splitting based on number of event per job
        """
        common.logger.debug(5,'Splitting per events')
        common.logger.message('Required '+str(self.eventsPerJob)+' events per job ')
        common.logger.message('Required '+str(self.theNumberOfJobs)+' jobs in total ')
        common.logger.message('Required '+str(self.total_number_of_events)+' events in total ')

        if (self.total_number_of_events < 0):
            msg='Cannot split jobs per Events with "-1" as total number of events'
            raise CrabException(msg)

        if (self.selectEventsPerJob):
            if (self.selectTotalNumberEvents):
                self.total_number_of_jobs = int(self.total_number_of_events/self.eventsPerJob)
            elif(self.selectNumberOfJobs) :  
                self.total_number_of_jobs =self.theNumberOfJobs
                self.total_number_of_events =int(self.theNumberOfJobs*self.eventsPerJob) 

        elif (self.selectNumberOfJobs) :
            self.total_number_of_jobs = self.theNumberOfJobs
            self.eventsPerJob = int(self.total_number_of_events/self.total_number_of_jobs)
 
        common.logger.debug(5,'N jobs  '+str(self.total_number_of_jobs))

        # is there any remainder?
        check = int(self.total_number_of_events) - (int(self.total_number_of_jobs)*self.eventsPerJob)

        common.logger.debug(5,'Check  '+str(check))

        common.logger.message(str(self.total_number_of_jobs)+' jobs can be created, each for '+str(self.eventsPerJob)+' for a total of '+str(self.total_number_of_jobs*self.eventsPerJob)+' events')
        if check > 0:
            common.logger.message('Warning: asked '+str(self.total_number_of_events)+' but can do only '+str(int(self.total_number_of_jobs)*self.eventsPerJob))

        # argument is seed number.$i
        self.list_of_args = []
        for i in range(self.total_number_of_jobs):
            ## Since there is no input, any site is good
           # self.jobDestination.append(["Any"])
            self.jobDestination.append([""]) #must be empty to write correctly the xml 
            args='' 
            if (self.firstRun):
                    ## pythia first run
                #self.list_of_args.append([(str(self.firstRun)+str(i))])
                args=args+(str(self.firstRun)+str(i))
            else:
                ## no first run
                #self.list_of_args.append([str(i)])
                args=args+str(i)
            if (self.sourceSeed):
                if (self.sourceSeedVtx):
                    ## pythia + vtx random seed
                    #self.list_of_args.append([
                    #                          str(self.sourceSeed)+str(i),
                    #                          str(self.sourceSeedVtx)+str(i)
                    #                          ])
                    args=args+str(',')+str(self.sourceSeed)+str(i)+str(',')+str(self.sourceSeedVtx)+str(i)
                else:
                    ## only pythia random seed
                    #self.list_of_args.append([(str(self.sourceSeed)+str(i))])
                    args=args +str(',')+str(self.sourceSeed)+str(i)
            else:
                ## no random seed
                if str(args)=='': args=args+(str(self.firstRun)+str(i))
            arguments=args.split(',')
            if len(arguments)==3:self.list_of_args.append([str(arguments[0]),str(arguments[1]),str(arguments[2])])
            elif len(arguments)==2:self.list_of_args.append([str(arguments[0]),str(arguments[1])])
            else :self.list_of_args.append([str(arguments[0])])
            
     #   print self.list_of_args

        return


    def jobSplittingForScript(self):#CarlosDaniele
        """
        Perform job splitting based on number of job
        """
        common.logger.debug(5,'Splitting per job')
        common.logger.message('Required '+str(self.theNumberOfJobs)+' jobs in total ')

        self.total_number_of_jobs = self.theNumberOfJobs

        common.logger.debug(5,'N jobs  '+str(self.total_number_of_jobs))

        common.logger.message(str(self.total_number_of_jobs)+' jobs can be created')

        # argument is seed number.$i
        self.list_of_args = []
        for i in range(self.total_number_of_jobs):
            ## Since there is no input, any site is good
           # self.jobDestination.append(["Any"])
            self.jobDestination.append([""])
            ## no random seed
            self.list_of_args.append([str(i)])
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
            jobParams[job] = arglist[job]
            # print str(arglist[job])
            # print jobParams[job]
            common.jobDB.setArguments(job, jobParams[job])
            common.logger.debug(5,"Job "+str(job)+" Destination: "+str(self.jobDestination[job]))
            common.jobDB.setDestination(job, self.jobDestination[job])

        common.jobDB.save()
        return
    
    def getJobTypeArguments(self, nj, sched):
        result = ''
        for i in common.jobDB.arguments(nj):
            result=result+str(i)+" "
        return result
  
    def numberOfJobs(self):
        # Fabio
        return self.total_number_of_jobs

    def getTarBall(self, exe):
        """
        Return the TarBall with lib and exe
        """
        
        # if it exist, just return it
        #
        # Marco. Let's start to use relative path for Boss XML files
        #
        self.tgzNameWithPath = common.work_space.pathForTgz()+'share/'+self.tgz_name
        if os.path.exists(self.tgzNameWithPath):
            return self.tgzNameWithPath

        # Prepare a tar gzipped file with user binaries.
        self.buildTar_(exe)

        return string.strip(self.tgzNameWithPath)

    def buildTar_(self, executable):

        # First of all declare the user Scram area
        swArea = self.scram.getSWArea_()
        #print "swArea = ", swArea
        # swVersion = self.scram.getSWVersion()
        # print "swVersion = ", swVersion
        swReleaseTop = self.scram.getReleaseTop_()
        #print "swReleaseTop = ", swReleaseTop
        
        ## check if working area is release top
        if swReleaseTop == '' or swArea == swReleaseTop:
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
                    exe = string.replace(exeWithPath, path,'')
                    tar.add(path+exe,exe)
                    pass
                else:
                    # the exe is from release, we'll find it on WN
                    pass
     
            ## Now get the libraries: only those in local working area
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

            ## Now check if any data dir(s) is present
            swAreaLen=len(swArea)
            for root, dirs, files in os.walk(swArea):
                if "data" in dirs:
                    common.logger.debug(5,"data "+root+"/data"+" to be tarred")
                    tar.add(root+"/data",root[swAreaLen:]+"/data")

            ## Add ProdAgent dir to tar
            paDir = 'ProdAgentApi'
            pa = os.environ['CRABDIR'] + '/' + 'ProdAgentApi'
            if os.path.isdir(pa):
                tar.add(pa,paDir)
        
            common.logger.debug(5,"Files added to "+self.tgzNameWithPath+" : "+str(tar.getnames()))
            tar.close()
        except :
            raise CrabException('Could not create tar-ball')

        ## check for tarball size
        tarballinfo = os.stat(self.tgzNameWithPath)
        if ( tarballinfo.st_size > self.MaxTarBallSize*1024*1024 ) :
            raise CrabException('Input sandbox size of ' + str(float(tarballinfo.st_size)/1024.0/1024.0) + ' MB is larger than the allowed ' + str(self.MaxTarBallSize) + ' MB input sandbox limit and not supported by the used GRID submission system. Please make sure that no unnecessary files are in all data directories in your local CMSSW project area as they are automatically packed into the input sandbox.')

        ## create tar-ball with ML stuff
        self.MLtgzfile =  common.work_space.pathForTgz()+'share/MLfiles.tgz' 
        try:
            tar = tarfile.open(self.MLtgzfile, "w:gz")
            path=os.environ['CRABDIR'] + '/python/'
            for file in ['report.py', 'DashboardAPI.py', 'Logger.py', 'ProcInfo.py', 'apmon.py', 'parseCrabFjr.py']:
                tar.add(path+file,file)
            common.logger.debug(5,"Files added to "+self.MLtgzfile+" : "+str(tar.getnames()))
            tar.close()
        except :
            raise CrabException('Could not create ML files tar-ball')
        
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
        txt += '    WORKING_DIR=`/bin/mktemp  -d $OSG_WN_TMP/cms_XXXXXXXXXXXX`\n'
        txt += '    echo "Created working directory: $WORKING_DIR"\n'
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
        txt += 'export SCRAM_ARCH='+self.executable_arch+'\n'
        txt += 'cd '+self.version+'\n'
        ### needed grep for bug in scramv1 ###
        txt += scram+' runtime -sh\n'
        txt += 'eval `'+scram+' runtime -sh | grep -v SCRAMRT_LSB_JOBNAME`\n'
        txt += 'echo $PATH\n'

        # Handle the arguments:
        txt += "\n"
        txt += "## number of arguments (first argument always jobnumber)\n"
        txt += "\n"
#        txt += "narg=$#\n"
        txt += "if [ $nargs -lt 2 ]\n"
        txt += "then\n"
        txt += "    echo 'SET_EXE_ENV 1 ==> ERROR Too few arguments' +$nargs+ \n"
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
        if self.pset != None: #CarlosDaniele
            pset = os.path.basename(job.configFilename())
            txt += '\n'
            if (self.datasetPath): # standard job
                #txt += 'InputFiles=$2\n'
                txt += 'InputFiles=${args[1]}\n'
                txt += 'MaxEvents=${args[2]}\n'
                txt += 'SkipEvents=${args[3]}\n'
                txt += 'echo "Inputfiles:<$InputFiles>"\n'
                txt += 'sed "s#{\'INPUT\'}#$InputFiles#" $RUNTIME_AREA/'+pset+' > pset_tmp_1.cfg\n'
                txt += 'echo "MaxEvents:<$MaxEvents>"\n'
                txt += 'sed "s#INPUTMAXEVENTS#$MaxEvents#" pset_tmp_1.cfg > pset_tmp_2.cfg\n'
                txt += 'echo "SkipEvents:<$SkipEvents>"\n'
                txt += 'sed "s#INPUTSKIPEVENTS#$SkipEvents#" pset_tmp_2.cfg > pset.cfg\n'
            else:  # pythia like job
                if (self.sourceSeed):
                    txt += 'FirstRun=${args[1]}\n'
                    txt += 'echo "FirstRun: <$FirstRun>"\n'
                    txt += 'sed "s#\<INPUTFIRSTRUN\>#$FirstRun#" $RUNTIME_AREA/'+pset+' > tmp_1.cfg\n'
                else:
                    txt += '# Copy untouched pset\n'
                    txt += 'cp $RUNTIME_AREA/'+pset+' tmp_1.cfg\n'
                if (self.sourceSeed):
#                    txt += 'Seed=$2\n'
                    txt += 'Seed=${args[2]}\n'
                    txt += 'echo "Seed: <$Seed>"\n'
                    txt += 'sed "s#\<INPUT\>#$Seed#" tmp_1.cfg > tmp_2.cfg\n'
                    if (self.sourceSeedVtx):
#                        txt += 'VtxSeed=$3\n'
                        txt += 'VtxSeed=${args[3]}\n'
                        txt += 'echo "VtxSeed: <$VtxSeed>"\n'
                        txt += 'sed "s#INPUTVTX#$VtxSeed#" tmp_2.cfg > pset.cfg\n'
                    else:
                        txt += 'mv tmp_2.cfg pset.cfg\n'
                else:
                    txt += 'mv tmp_1.cfg pset.cfg\n'
                   # txt += '# Copy untouched pset\n'
                   # txt += 'cp $RUNTIME_AREA/'+pset+' pset.cfg\n'


        if len(self.additional_inbox_files) > 0:
            for file in self.additional_inbox_files:
                relFile = file.split("/")[-1]
                txt += 'if [ -e $RUNTIME_AREA/'+relFile+' ] ; then\n'
                txt += '   cp $RUNTIME_AREA/'+relFile+' .\n'
                txt += '   chmod +x '+relFile+'\n'
                txt += 'fi\n'
            pass 

        if self.pset != None: #CarlosDaniele
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

    def wsBuildExe(self, nj=0):
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
            txt += '\n'
            txt += 'echo "Include ProdAgentApi in PYTHONPATH"\n'
            txt += 'if [ -z "$PYTHONPATH" ]; then\n'
            txt += '   export PYTHONPATH=ProdAgentApi\n'
            txt += 'else\n'
            txt += '   export PYTHONPATH=ProdAgentApi:${PYTHONPATH}\n'
            txt += 'fi\n'
            txt += '\n'

            pass
        
        return txt

    def modifySteeringCards(self, nj):
        """
        modify the card provided by the user, 
        writing a new card into share dir
        """
        
    def executableName(self):
        if self.scriptExe: #CarlosDaniele
            return "sh "
        else:
            return self.executable

    def executableArgs(self):
        if self.scriptExe:#CarlosDaniele
            return   self.scriptExe + " $NJob"
        else: 
            return " -p pset.cfg"

    def inputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL input sandbox.
        """
        inp_box = []
        # # dict added to delete duplicate from input sandbox file list
        # seen = {}
        ## code
        if os.path.isfile(self.tgzNameWithPath):
            inp_box.append(self.tgzNameWithPath)
        if os.path.isfile(self.MLtgzfile):
            inp_box.append(self.MLtgzfile)
        ## config
        if not self.pset is None:
            inp_box.append(common.work_space.pathForTgz() + 'job/' + self.configFilename())
        ## additional input files
        for file in self.additional_inbox_files:
            inp_box.append(file)
        return inp_box

    def outputSandbox(self, nj):
        """
        Returns a list of filenames to be put in JDL output sandbox.
        """
        out_box = []

        ## User Declared output files
        for out in (self.output_file+self.output_file_sandbox):
            n_out = nj + 1 
            out_box.append(self.numberFile_(out,str(n_out)))
        return out_box

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

        for fileWithSuffix in (self.output_file+self.output_file_sandbox):
            output_file_num = self.numberFile_(fileWithSuffix, '$NJob')
            txt += '\n'
            txt += '# check output file\n'
            txt += 'ls '+fileWithSuffix+'\n'
            txt += 'ls_result=$?\n'
            txt += 'if [ $ls_result -ne 0 ] ; then\n'
            txt += '   echo "ERROR: Problem with output file"\n'
            if common.scheduler.boss_scheduler_name == 'condor_g':
                txt += '    if [ $middleware == OSG ]; then \n'
                txt += '        echo "prepare dummy output file"\n'
                txt += '        echo "Processing of job output failed" > $RUNTIME_AREA/'+output_file_num+'\n'
                txt += '    fi \n'
            txt += 'else\n'
            txt += '   cp '+fileWithSuffix+' $RUNTIME_AREA/'+output_file_num+'\n'
            txt += 'fi\n'
       
        txt += 'cd $RUNTIME_AREA\n'
        txt += 'cd $RUNTIME_AREA\n'
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

        file_list = ''
        ## Add to filelist only files to be possibly copied to SE
        for fileWithSuffix in self.output_file:
            output_file_num = self.numberFile_(fileWithSuffix, '$NJob')
            file_list=file_list+output_file_num+' '
        file_list=file_list[:-1]
        txt += 'file_list="'+file_list+'"\n'

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
          result = name + '_' + txt + "." + ext
        else:
          result = name + '_' + txt
        
        return result

    def getRequirements(self, nj=[]):
        """
        return job requirements to add to jdl files 
        """
        req = ''
        if self.version:
            req='Member("VO-cms-' + \
                 self.version + \
                 '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'

        req = req + ' && (other.GlueHostNetworkAdapterOutboundIP)'

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
        txt += '   elif [ -f $OSG_APP/cmssoft/cms/cmsset_default.sh ] ;then\n'
        txt += '      # Use $OSG_APP/cmssoft/cms/cmsset_default.sh to setup cms software\n'
        txt += '       source $OSG_APP/cmssoft/cms/cmsset_default.sh '+self.version+'\n'
        txt += '   else\n'
        txt += '       echo "SET_CMS_ENV 10020 ==> ERROR $GRID3_APP_DIR/cmssoft/cmsset_default.sh and $OSG_APP/cmssoft/cms/cmsset_default.sh file not found"\n'
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
        txt += '	    echo "SET_CMS_ENV 10017 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after $GRID3_APP_DIR/cmssoft/cmsset_default.sh and $OSG_APP/cmssoft/cms/cmsset_default.sh file not found"\n' 
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

#######################################################################
    def uniquelist(self, old):
        """
        remove duplicates from a list
        """
        nd={}
        for e in old:
            nd[e]=0
        return nd.keys()
