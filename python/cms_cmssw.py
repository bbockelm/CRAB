from JobType import JobType
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from BlackWhiteListParser import BlackWhiteListParser
import common
import Scram

import os, string, glob

class Cmssw(JobType):
    def __init__(self, cfg_params, ncjobs):
        JobType.__init__(self, 'CMSSW')
        common.logger.debug(3,'CMSSW::__init__')

        self.argsList = []

        self._params = {}
        self.cfg_params = cfg_params
        # init BlackWhiteListParser
        self.blackWhiteListParser = BlackWhiteListParser(cfg_params)

        self.MaxTarBallSize = float(self.cfg_params.get('EDG.maxtarballsize',9.5))

        # number of jobs requested to be created, limit obj splitting
        self.ncjobs = ncjobs

        log = common.logger

        self.scram = Scram.Scram(cfg_params)
        self.additional_inbox_files = []
        self.scriptExe = ''
        self.executable = ''
        self.executable_arch = self.scram.getArch()
        self.tgz_name = 'default.tgz'
        self.additional_tgz_name = 'additional.tgz'
        self.scriptName = 'CMSSW.sh'
        self.pset = ''      #scrip use case Da
        self.datasetPath = '' #scrip use case Da

        # set FJR file name
        self.fjrFileName = 'crab_fjr.xml'

        self.version = self.scram.getSWVersion()

        #
        # Try to block creation in case of arch/version mismatch
        #

#        a = string.split(self.version, "_")
#
#        if int(a[1]) == 1 and (int(a[2]) < 5 and self.executable_arch.find('slc4') == 0):
#            msg = "Warning: You are using %s version of CMSSW  with %s architecture. \n--> Did you compile your libraries with SLC3? Otherwise you can find some problems running on SLC4 Grid nodes.\n"%(self.version, self.executable_arch)
#            common.logger.message(msg)
#        if int(a[1]) == 1 and (int(a[2]) >= 5 and self.executable_arch.find('slc3') == 0):
#            msg = "Error: CMS does not support %s with %s architecture"%(self.version, self.executable_arch)
#            raise CrabException(msg)
#

        self.setParam_('application', self.version)

        ### collect Data cards

        if not cfg_params.has_key('CMSSW.datasetpath'):
            msg = "Error: datasetpath not defined "
            raise CrabException(msg)
        tmp =  cfg_params['CMSSW.datasetpath']
        log.debug(6, "CMSSW::CMSSW(): datasetPath = "+tmp)
        if string.lower(tmp)=='none':
            self.datasetPath = None
            self.selectNoInput = 1
        else:
            self.datasetPath = tmp
            self.selectNoInput = 0

        # ML monitoring
        # split dataset path style: /PreProdR3Minbias/SIM/GEN-SIM
        if not self.datasetPath:
            self.setParam_('dataset', 'None')
            self.setParam_('owner', 'None')
        else:
            ## SL what is supposed to fail here?
            try:
                datasetpath_split = self.datasetPath.split("/")
                # standard style
                self.setParam_('datasetFull', self.datasetPath)
                self.setParam_('dataset', datasetpath_split[1])
                self.setParam_('owner', datasetpath_split[2])
            except:
                self.setParam_('dataset', self.datasetPath)
                self.setParam_('owner', self.datasetPath)

        self.setParam_('taskId', common._db.queryTask('name')) ## new BL--DS

        self.dataTiers = []

        ## now the application
        self.executable = cfg_params.get('CMSSW.executable','cmsRun')
        self.setParam_('exe', self.executable)
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
        self.output_file = []
        tmp = cfg_params.get('CMSSW.output_file',None)
        if tmp :
            tmpOutFiles = string.split(tmp,',')
            log.debug(7, 'cmssw::cmssw(): output files '+str(tmpOutFiles))
            for tmp in tmpOutFiles:
                tmp=string.strip(tmp)
                self.output_file.append(tmp)
                pass
        else:
            log.message("No output file defined: only stdout/err and the CRAB Framework Job Report will be available\n")
        pass

        # script_exe file as additional file in inputSandbox
        self.scriptExe = cfg_params.get('USER.script_exe',None)
        if self.scriptExe :
           if not os.path.isfile(self.scriptExe):
              msg ="ERROR. file "+self.scriptExe+" not found"
              raise CrabException(msg)
           self.additional_inbox_files.append(string.strip(self.scriptExe))

        #CarlosDaniele
        if self.datasetPath == None and self.pset == None and self.scriptExe == '' :
           msg ="Error. script_exe  not defined"
           raise CrabException(msg)

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
                    # fname = string.split(file, '/')[-1]
                    # storedFile = common.work_space.pathForTgz()+'share/'+fname
                    # shutil.copyfile(file, storedFile)
                    self.additional_inbox_files.append(string.strip(file))
                pass
            pass
            common.logger.debug(5,"Additional input files: "+str(self.additional_inbox_files))
        pass

        ## Events per job
        if cfg_params.has_key('CMSSW.events_per_job'):
            self.eventsPerJob =int( cfg_params['CMSSW.events_per_job'])
            self.selectEventsPerJob = 1
        else:
            self.eventsPerJob = -1
            self.selectEventsPerJob = 0

        ## number of jobs
        if cfg_params.has_key('CMSSW.number_of_jobs'):
            self.theNumberOfJobs =int( cfg_params['CMSSW.number_of_jobs'])
            self.selectNumberOfJobs = 1
        else:
            self.theNumberOfJobs = 0
            self.selectNumberOfJobs = 0

        if cfg_params.has_key('CMSSW.total_number_of_events'):
            self.total_number_of_events = int(cfg_params['CMSSW.total_number_of_events'])
            self.selectTotalNumberEvents = 1
        else:
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

        ## Old method of dealing with seeds
        ## FUTURE: This is for old CMSSW and old CRAB. Can throw exceptions after a couple of CRAB releases and then
        ## remove
        self.sourceSeed = cfg_params.get('CMSSW.pythia_seed',None)
        if self.sourceSeed:
          print "pythia_seed is a deprecated parameter. Use preserve_seeds or increment_seeds in the future.\n","Added to increment_seeds."
          self.incrementSeeds.append('sourceSeed')

        self.sourceSeedVtx = cfg_params.get('CMSSW.vtx_seed',None)
        if self.sourceSeedVtx:
          print "vtx_seed is a deprecated parameter. Use preserve_seeds or increment_seeds in the future.\n","Added to increment_seeds."
          self.incrementSeeds.append('VtxSmeared')

        self.sourceSeedG4 = cfg_params.get('CMSSW.g4_seed',None)
        if self.sourceSeedG4:
          print "g4_seed is a deprecated parameter. Use preserve_seeds or increment_seeds in the future.\n","Added to increment_seeds."
          self.incrementSeeds.append('g4SimHits')

        self.sourceSeedMix = cfg_params.get('CMSSW.mix_seed',None)
        if self.sourceSeedMix:
          print "mix_seed is a deprecated parameter. Use preserve_seeds or increment_seeds in the future.\n","Added to increment_seeds."
          self.incrementSeeds.append('mix')

        self.firstRun = cfg_params.get('CMSSW.first_run',None)

        if self.pset != None: #CarlosDaniele
            import PsetManipulator as pp
            PsetEdit = pp.PsetManipulator(self.pset) #Daniele Pset

        # Copy/return

        self.copy_data = int(cfg_params.get('USER.copy_data',0))
        self.return_data = int(cfg_params.get('USER.return_data',0))

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
                # Add FrameworkJobReport to parameter-set, set max events.
                # Reset later for data jobs by writeCFG which does all modifications
                PsetEdit.addCrabFJR(self.fjrFileName)
                PsetEdit.maxEvent(self.eventsPerJob)
                PsetEdit.psetWriter(self.configFilename())
            except:
                msg='Error while manipuliating ParameterSet: exiting...'
                raise CrabException(msg)

    def DataDiscoveryAndLocation(self, cfg_params):

        import DataDiscovery
        import DataLocation
        common.logger.debug(10,"CMSSW::DataDiscoveryAndLocation()")

        datasetPath=self.datasetPath

        ## Contact the DBS
        common.logger.message("Contacting Data Discovery Services ...")
        try:
            self.pubdata=DataDiscovery.DataDiscovery(datasetPath, cfg_params)
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
        self.eventsbyblock=self.pubdata.getEventsPerBlock()
        self.eventsbyfile=self.pubdata.getEventsPerFile()

        ## get max number of events
        self.maxEvents=self.pubdata.getMaxEvents() ##  self.maxEvents used in Creator.py

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

        # screen output
        common.logger.message("Requested dataset: " + datasetPath + " has " + str(self.maxEvents) + " events in " + str(len(self.filesbyblock.keys())) + " blocks.\n")

        return sites

  # to Be Removed  DS -- BL
  #  def setArgsList(self, argsList):
  #      self.argsList = argsList

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

        # list tracking which jobs are in which jobs belong to which block
        jobsOfBlock = {}

        # ---- Iterate over the blocks in the dataset until ---- #
        # ---- we've met the requested total # of events    ---- #
        while ( (eventsRemaining > 0) and (blockCount < numBlocksInDataset) and (jobCount < totalNumberOfJobs)):
            block = blocks[blockCount]
            blockCount += 1
            if block not in jobsOfBlock.keys() :
                jobsOfBlock[block] = []

            if self.eventsbyblock.has_key(block) :
                numEventsInBlock = self.eventsbyblock[block]
                common.logger.debug(5,'Events in Block File '+str(numEventsInBlock))

                files = self.filesbyblock[block]
                numFilesInBlock = len(files)
                if (numFilesInBlock <= 0):
                    continue
                fileCount = 0

                # ---- New block => New job ---- #
                parString = ""
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
                            list_of_lists.append([fullString,str(-1),str(jobSkipEventCount)])
                            common.logger.debug(3,"Job "+str(jobCount+1)+" can run over "+str(filesEventCount - jobSkipEventCount)+" events (last file in block).")
                            self.jobDestination.append(blockSites[block])
                            common.logger.debug(5,"Job "+str(jobCount+1)+" Destination: "+str(self.jobDestination[jobCount]))
                            # fill jobs of block dictionary
                            jobsOfBlock[block].append(jobCount+1)
                            # reset counter
                            jobCount = jobCount + 1
                            totalEventCount = totalEventCount + filesEventCount - jobSkipEventCount
                            eventsRemaining = eventsRemaining - filesEventCount + jobSkipEventCount
                            jobSkipEventCount = 0
                            # reset file
                            parString = ""
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
                        list_of_lists.append([fullString,str(eventsPerJobRequested),str(jobSkipEventCount)])
                        common.logger.debug(3,"Job "+str(jobCount+1)+" can run over "+str(eventsPerJobRequested)+" events.")
                        self.jobDestination.append(blockSites[block])
                        common.logger.debug(5,"Job "+str(jobCount+1)+" Destination: "+str(self.jobDestination[jobCount]))
                        jobsOfBlock[block].append(jobCount+1)
                        # reset counter
                        jobCount = jobCount + 1
                        totalEventCount = totalEventCount + eventsPerJobRequested
                        eventsRemaining = eventsRemaining - eventsPerJobRequested
                        jobSkipEventCount = 0
                        # reset file
                        parString = ""
                        filesEventCount = 0
                        newFile = 1
                        fileCount += 1

                    # if more events in file remain than eventsPerJobRequested
                    else :
                        # close job but don't touch new file
                        fullString = parString[:-2]
                        list_of_lists.append([fullString,str(eventsPerJobRequested),str(jobSkipEventCount)])
                        common.logger.debug(3,"Job "+str(jobCount+1)+" can run over "+str(eventsPerJobRequested)+" events.")
                        self.jobDestination.append(blockSites[block])
                        common.logger.debug(5,"Job "+str(jobCount+1)+" Destination: "+str(self.jobDestination[jobCount]))
                        jobsOfBlock[block].append(jobCount+1)
                        # increase counter
                        jobCount = jobCount + 1
                        totalEventCount = totalEventCount + eventsPerJobRequested
                        eventsRemaining = eventsRemaining - eventsPerJobRequested
                        # calculate skip events for last file
                        # use filesEventCount (contains several files), jobSkipEventCount and eventsPerJobRequest
                        jobSkipEventCount = eventsPerJobRequested - (filesEventCount - jobSkipEventCount - self.eventsbyfile[file])
                        # remove all but the last file
                        filesEventCount = self.eventsbyfile[file]
                        parString = '\\\"' + file + '\\\"\,'
                    pass # END if
                pass # END while (iterate over files in the block)
        pass # END while (iterate over blocks in the dataset)
        self.ncjobs = self.total_number_of_jobs = jobCount
        if (eventsRemaining > 0 and jobCount < totalNumberOfJobs ):
            common.logger.message("Could not run on all requested events because some blocks not hosted at allowed sites.")
        common.logger.message(str(jobCount)+" job(s) can run on "+str(totalEventCount)+" events.\n")

        # screen output
        screenOutput = "List of jobs and available destination sites:\n\n"

        # keep trace of block with no sites to print a warning at the end
        noSiteBlock = []
        bloskNoSite = []

        blockCounter = 0
        for block in blocks:
            if block in jobsOfBlock.keys() :
                blockCounter += 1
                screenOutput += "Block %5i: jobs %20s: sites: %s\n" % (blockCounter,spanRanges(jobsOfBlock[block]),','.join(self.blackWhiteListParser.checkWhiteList(self.blackWhiteListParser.checkBlackList(blockSites[block],block),block)))
                if len(self.blackWhiteListParser.checkWhiteList(self.blackWhiteListParser.checkBlackList(blockSites[block],block),block)) == 0:
                    noSiteBlock.append( spanRanges(jobsOfBlock[block]) )
                    bloskNoSite.append( blockCounter )

        common.logger.message(screenOutput)
        if len(noSiteBlock) > 0 and len(bloskNoSite) > 0:
            msg = 'WARNING: No sites are hosting any part of data for block:\n                '
            virgola = ""
            if len(bloskNoSite) > 1:
                virgola = ","
            for block in bloskNoSite:
                msg += ' ' + str(block) + virgola
            msg += '\n               Related jobs:\n                 '
            virgola = ""
            if len(noSiteBlock) > 1:
                virgola = ","
            for range_jobs in noSiteBlock:
                msg += str(range_jobs) + virgola
            msg += '\n               will not be submitted and this block of data can not be analyzed!\n'
            if self.cfg_params.has_key('EDG.se_white_list'):
                msg += 'WARNING: SE White List: '+self.cfg_params['EDG.se_white_list']+'\n'
                msg += '(Hint: By whitelisting you force the job to run at this particular site(s).\n'
                msg += 'Please check if the dataset is available at this site!)\n'
            if self.cfg_params.has_key('EDG.ce_white_list'):
                msg += 'WARNING: CE White List: '+self.cfg_params['EDG.ce_white_list']+'\n'
                msg += '(Hint: By whitelisting you force the job to run at this particular site(s).\n'
                msg += 'Please check if the dataset is available at this site!)\n'

            common.logger.message(msg)

        self.list_of_args = list_of_lists
        return

    def jobSplittingNoInput(self):
        """
        Perform job splitting based on number of event per job
        """
        common.logger.debug(5,'Splitting per events')

        if (self.selectEventsPerJob):
            common.logger.message('Required '+str(self.eventsPerJob)+' events per job ')
        if (self.selectNumberOfJobs):
            common.logger.message('Required '+str(self.theNumberOfJobs)+' jobs in total ')
        if (self.selectTotalNumberEvents):
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
            self.jobDestination.append([""]) #must be empty to write correctly the xml
            args=[]
            if (self.firstRun):
                ## pythia first run
                args.append(str(self.firstRun)+str(i))
            self.list_of_args.append(args)

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

        #### Fabio
        njobs = self.total_number_of_jobs
        arglist = self.list_of_args
        # create the empty structure
        for i in range(njobs):
            jobParams.append("")

        listID=[]
        listField=[]
        for job in range(njobs):
            jobParams[job] = arglist[job]
            listID.append(job+1)
            job_ToSave ={}
            concString = ' '
            argu=''
            if len(jobParams[job]):
                argu +=   concString.join(jobParams[job] )
            job_ToSave['arguments']= str(job+1)+' '+argu## new BL--DS
            job_ToSave['dlsDestination']= self.jobDestination[job]## new BL--DS
            #common._db.updateJob_(job,job_ToSave)## new BL--DS
            listField.append(job_ToSave)
            msg="Job "+str(job)+" Arguments:   "+str(job+1)+" "+argu+"\n"  \
            +"                     Destination: "+str(self.jobDestination[job])
            common.logger.debug(5,msg)
            #common.logger.debug(5,"Job "+str(job)+" Destination: "+str(self.jobDestination[job]))
        common._db.updateJob_(listID,listField)## new BL--DS
        ## Pay Attention Here....DS--BL
        self.argsList = (len(jobParams[1])+1)

        return
#
#    def getJobTypeArguments(self, nj, sched):
#        result = ''
#        jobs=[]
#        jobs.append(nj)
#        for i in common._db.queryJob('arguments',jobs):##  BL--DS
#            result=result+str(i)+" "
#        return result

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


            ## Add ProdCommon dir to tar
            prodcommonDir = 'ProdCommon'
            prodcommonPath = os.environ['CRABDIR'] + '/' + 'ProdCommon'
            if os.path.isdir(prodcommonPath):
                tar.add(prodcommonPath,prodcommonDir)

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
            #for file in ['report.py', 'DashboardAPI.py', 'Logger.py', 'ProcInfo.py', 'apmon.py', 'parseCrabFjr.py','writeCfg.py']:
            ### FEDE ####
            for file in ['report.py', 'DashboardAPI.py', 'Logger.py', 'ProcInfo.py', 'apmon.py', 'parseCrabFjr.py','writeCfg.py', 'JobReportErrorCode.py']:
            ###############
                tar.add(path+file,file)
            common.logger.debug(5,"Files added to "+self.MLtgzfile+" : "+str(tar.getnames()))
            tar.close()
        except :
            raise CrabException('Could not create ML files tar-ball')

        return

    def additionalInputFileTgz(self):
        """
        Put all additional files into a tar ball and return its name
        """
        import tarfile
        tarName=  common.work_space.pathForTgz()+'share/'+self.additional_tgz_name
        tar = tarfile.open(tarName, "w:gz")
        for file in self.additional_inbox_files:
            tar.add(file,string.split(file,'/')[-1])
        common.logger.debug(5,"Files added to "+self.additional_tgz_name+" : "+str(tar.getnames()))
        tar.close()
        return tarName

    def wsSetupEnvironment(self, nj=0):
        """
        Returns part of a job script which prepares
        the execution environment for the job 'nj'.
        """
        # Prepare JobType-independent part
        txt = '\n#Written by cms_cmssw::wsSetupEnvironment\n'
        txt += 'echo ">>> setup environment"\n'
        txt += 'if [ $middleware == LCG ]; then \n'
        txt += self.wsSetupCMSLCGEnvironment_()
        txt += 'elif [ $middleware == OSG ]; then\n'
        txt += '    WORKING_DIR=`/bin/mktemp  -d $OSG_WN_TMP/cms_XXXXXXXXXXXX`\n'
        txt += '    if [ ! $? == 0 ] ;then\n'
        #txt += '        echo "SET_CMS_ENV 10016 ==> OSG $WORKING_DIR could not be created on WN `hostname`"\n'
        #txt += '        echo "JOB_EXIT_STATUS = 10016"\n'
        #txt += '        echo "JobExitCode=10016" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '        dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '        exit 1\n'
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
        #txt += '    echo "### Set SCRAM ARCH to ' + self.executable_arch + ' ###"\n'
        #txt += '    export SCRAM_ARCH='+self.executable_arch+'\n'
        txt += 'fi\n'

        # Prepare JobType-specific part
        scram = self.scram.commandName()
        txt += '\n\n'
        txt += 'echo ">>> specific cmssw setup environment:"\n'
        txt += 'echo "CMSSW_VERSION =  '+self.version+'"\n'
        txt += scram+' project CMSSW '+self.version+'\n'
        txt += 'status=$?\n'
        txt += 'if [ $status != 0 ] ; then\n'
        #txt += '    echo "SET_EXE_ENV 10034 ==>ERROR CMSSW '+self.version+' not found on `hostname`" \n'
        #txt += '    echo "JOB_EXIT_STATUS = 10034"\n'
        #txt += '    echo "JobExitCode=10034" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    echo "ERROR ==> CMSSW '+self.version+' not found on `hostname`" \n'
        txt += '    job_exit_code=10034\n'
        #txt += '    if [ $middleware == OSG ]; then \n'
        #txt += '        cd $RUNTIME_AREA\n'
        #txt += '        echo ">>> current directory (RUNTIME_AREA): $RUNTIME_AREA"\n'
        #txt += '        echo ">>> Remove working directory: $WORKING_DIR"\n'
        #txt += '        /bin/rm -rf $WORKING_DIR\n'
        #txt += '        if [ -d $WORKING_DIR ] ;then\n'
        #txt += '            echo "SET_CMS_ENV 10018 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after CMSSW CMSSW_0_6_1 not found on `hostname`"\n'
        #txt += '            echo "JOB_EXIT_STATUS = 10018"\n'
        #txt += '            echo "JobExitCode=10018" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '            dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '            echo "ERROR ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after CMSSW CMSSW_0_6_1 not found on `hostname`"\n'
        #txt += '            job_exit_code=10017\n'
        #txt += '        fi\n'
        #txt += '    fi \n'
        #txt += '    exit 1 \n'
        txt += '    func_exit\n'
        txt += 'fi \n'
        txt += 'cd '+self.version+'\n'
        ########## FEDE FOR DBS2 ######################
        txt += 'SOFTWARE_DIR=`pwd`\n'
        txt += 'echo ">>> current directory (SOFTWARE_DIR): $SOFTWARE_DIR" \n'
        ###############################################
        ### needed grep for bug in scramv1 ###
        txt += 'eval `'+scram+' runtime -sh | grep -v SCRAMRT_LSB_JOBNAME`\n'
        # Handle the arguments:
        txt += "\n"
        txt += "## number of arguments (first argument always jobnumber)\n"
        txt += "\n"
       # txt += "if [ $nargs -lt "+str(len(self.argsList[nj].split()))+" ]\n"
        txt += "if [ $nargs -lt "+str(self.argsList)+" ]\n"
        txt += "then\n"
        #txt += "    echo 'SET_EXE_ENV 1 ==> ERROR Too few arguments' +$nargs+ \n"
        #txt += '    echo "JOB_EXIT_STATUS = 50113"\n'
        #txt += '    echo "JobExitCode=50113" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += "    echo 'ERROR ==> Too few arguments' +$nargs+ \n"
        txt += '    job_exit_code=50113\n'
        #txt += '    if [ $middleware == OSG ]; then \n'
        #txt += '        cd $RUNTIME_AREA\n'
        #txt += '        echo ">>> current directory (RUNTIME_AREA): $RUNTIME_AREA"\n'
        #txt += '        echo ">>> Remove working directory: $WORKING_DIR"\n'
        #txt += '        /bin/rm -rf $WORKING_DIR\n'
        #txt += '        if [ -d $WORKING_DIR ] ;then\n'
        #txt += '            echo "SET_EXE_ENV 50114 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after Too few arguments for CRAB job wrapper"\n'
        #txt += '            echo "JOB_EXIT_STATUS = 50114"\n'
        #txt += '            echo "JobExitCode=50114" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '            dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '            echo "ERROR ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after Too few arguments for CRAB job wrapper"\n'
        #txt += '            job_exit_code=10017\n'
        #txt += '        fi\n'
        #txt += '    fi\n'
        #txt += "    exit 1\n"
        txt += "    func_exit\n"
        txt += "fi\n"
        txt += "\n"

        # Prepare job-specific part
        job = common.job_list[nj]
        ### FEDE FOR DBS OUTPUT PUBLICATION
        if (self.datasetPath):
            txt += '\n'
            txt += 'DatasetPath='+self.datasetPath+'\n'

            datasetpath_split = self.datasetPath.split("/")

            txt += 'PrimaryDataset='+datasetpath_split[1]+'\n'
            txt += 'DataTier='+datasetpath_split[2]+'\n'
            txt += 'ApplicationFamily=cmsRun\n'

        else:
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
                txt += 'MaxEvents=${args[2]}; export MaxEvents\n'
                txt += 'SkipEvents=${args[3]}; export SkipEvents\n'
                txt += 'echo "Inputfiles:<$InputFiles>"\n'
                txt += 'echo "MaxEvents:<$MaxEvents>"\n'
                txt += 'echo "SkipEvents:<$SkipEvents>"\n'
            else:  # pythia like job
                txt += 'PreserveSeeds='  + ','.join(self.preserveSeeds)  + '; export PreserveSeeds\n'
                txt += 'IncrementSeeds=' + ','.join(self.incrementSeeds) + '; export IncrementSeeds\n'
                txt += 'echo "PreserveSeeds: <$PreserveSeeds>"\n'
                txt += 'echo "IncrementSeeds:<$IncrementSeeds>"\n'
                if (self.firstRun):
                    txt += 'FirstRun=${args[1]}; export FirstRun\n'
                    txt += 'echo "FirstRun: <$FirstRun>"\n'

            txt += 'mv -f '+pset+' pset.cfg\n'

        if len(self.additional_inbox_files) > 0:
            txt += 'if [ -e $RUNTIME_AREA/'+self.additional_tgz_name+' ] ; then\n'
            txt += '  tar xzvf $RUNTIME_AREA/'+self.additional_tgz_name+'\n'
            txt += 'fi\n'
            pass

        if self.pset != None:
            txt += '\n'
            txt += 'echo "***** cat pset.cfg *********"\n'
            txt += 'cat pset.cfg\n'
            txt += 'echo "****** end pset.cfg ********"\n'
            txt += '\n'
            txt += 'PSETHASH=`EdmConfigHash < pset.cfg` \n'
            txt += 'echo "PSETHASH = $PSETHASH" \n'
            txt += '\n'
        return txt
    #### FEDE #####
    def wsUntarSoftware(self, nj=0):
        """
        Put in the script the commands to build an executable
        or a library.
        """

        txt = '\n#Written by cms_cmssw::wsUntarSoftware\n'

        if os.path.isfile(self.tgzNameWithPath):
            txt += 'echo ">>> tar xzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+' :" \n'
            txt += 'tar xzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+'\n'
            txt += 'untar_status=$? \n'
            txt += 'if [ $untar_status -ne 0 ]; then \n'
            txt += '   echo "ERROR ==> Untarring .tgz file failed"\n'
            txt += '   job_exit_code=$untar_status\n'
            txt += '   func_exit\n'
            txt += 'else \n'
            txt += '   echo "Successful untar" \n'
            txt += 'fi \n'
            txt += '\n'
            txt += 'echo ">>> Include ProdCommon in PYTHONPATH:"\n'
            txt += 'if [ -z "$PYTHONPATH" ]; then\n'
            txt += '   export PYTHONPATH=$RUNTIME_AREA/ProdCommon\n'
            txt += 'else\n'
            txt += '   export PYTHONPATH=$RUNTIME_AREA/ProdCommon:${PYTHONPATH}\n'
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
        txt += 'mv $RUNTIME_AREA/ProdCommon/ . \n'


        #if os.path.isfile(self.tgzNameWithPath):
        #    txt += 'echo ">>> tar xzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+' :" \n'
        #    txt += 'tar xzvf $RUNTIME_AREA/'+os.path.basename(self.tgzNameWithPath)+'\n'
        #    txt += 'untar_status=$? \n'
        #    txt += 'if [ $untar_status -ne 0 ]; then \n'
        #    txt += '   echo "ERROR ==> Untarring .tgz file failed"\n'
        #    txt += '   job_exit_code=$untar_status\n'
        #    txt += '   func_exit\n'
        #    txt += 'else \n'
        #    txt += '   echo "Successful untar" \n'
        #    txt += 'fi \n'
        #    txt += '\n'
        #    txt += 'echo ">>> Include ProdCommon in PYTHONPATH:"\n'
        txt += 'if [ -z "$PYTHONPATH" ]; then\n'
        txt += '   export PYTHONPATH=$SOFTWARE_DIR/ProdCommon\n'
        txt += 'else\n'
        txt += '   export PYTHONPATH=$SOFTWARE_DIR/ProdCommon:${PYTHONPATH}\n'
        txt += 'echo "PYTHONPATH=$PYTHONPATH"\n'
        txt += 'fi\n'
        txt += '\n'

        return txt
    ############################################################################

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
        # FUTURE: This function tests the CMSSW version. Can be simplified as we drop support for old versions
        if self.scriptExe:#CarlosDaniele
            return   self.scriptExe + " $NJob"
        else:
            version_array = self.scram.getSWVersion().split('_')
            major = 0
            minor = 0
            try:
                major = int(version_array[1])
                minor = int(version_array[2])
            except:
                msg = "Cannot parse CMSSW version string: " + "_".join(version_array) + " for major and minor release number!"
                raise CrabException(msg)

            ex_args = ""

            # Framework job report
            if major >= 1 and minor >= 5 :
                #ex_args += " -j " + self.fjrFileName
            ### FEDE it could be improved!!! ####
                ex_args += " -j $RUNTIME_AREA/crab_fjr_$NJob.xml"
            #######################################
            # Type of cfg file
            if major >= 2 :
                ex_args += " -p pset.pycfg"
            else:
                ex_args += " -p pset.cfg"
            return ex_args

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
        tgz = self.additionalInputFileTgz()
        inp_box.append(tgz)
        ## executable
        wrapper = os.path.basename(str(common._db.queryTask('scriptName')))
        inp_box.append(common.work_space.pathForTgz() +'job/'+ wrapper)
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

        txt = '\n#Written by cms_cmssw::wsRenameOutput\n'
        txt += 'echo ">>> current directory (SOFTWARE_DIR): $SOFTWARE_DIR" \n'
        txt += 'echo ">>> current directory content:"\n'
        txt += 'ls \n'
        txt += '\n'

        #txt += 'output_exit_status=0\n'

        ### FEDE #######
        #for fileWithSuffix in (self.output_file_sandbox):
        #    output_file_num = self.numberFile_(fileWithSuffix, '$NJob')
        #    txt += '\n'
        #    txt += '# check output file\n'
        #    txt += 'if [ -e ./'+fileWithSuffix+' ] ; then\n'
        #    txt += '    mv '+fileWithSuffix+' $RUNTIME_AREA/'+output_file_num+'\n'
        #    txt += '    ln -s $RUNTIME_AREA/'+output_file_num+' $RUNTIME_AREA/'+fileWithSuffix+'\n'
        #    txt += 'else\n'
        #    txt += '    echo "WARNING: Output file '+fileWithSuffix+' not found"\n'
        #    txt += '    job_exit_code=60302\n'
        #    if common.scheduler.name().upper() == 'CONDOR_G':
        #        txt += '    if [ $middleware == OSG ]; then \n'
        #        txt += '        echo "prepare dummy output file"\n'
        #        txt += '        echo "Processing of job output failed" > $RUNTIME_AREA/'+output_file_num+'\n'
        #        txt += '    fi \n'
        #    txt += 'fi\n'

        for fileWithSuffix in (self.output_file):
            output_file_num = self.numberFile_(fileWithSuffix, '$NJob')
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
            #txt += '    exit_status=60302\n'
            #txt += '    echo "ERROR: Output file '+fileWithSuffix+' not found"\n'
            #txt += '    echo "JOB_EXIT_STATUS = $exit_status"\n'
            #txt += '    output_exit_status=$exit_status\n'
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
             file_list.append(self.numberFile_(fileWithSuffix, '$NJob'))

        txt += 'file_list="'+string.join(file_list,' ')+'"\n'
        txt += '\n'
        txt += 'echo ">>> current directory (SOFTWARE_DIR): $SOFTWARE_DIR" \n'
        txt += 'echo ">>> current directory content:"\n'
        txt += 'ls \n'
        txt += '\n'
        txt += 'cd $RUNTIME_AREA\n'
        txt += 'echo ">>> current directory (RUNTIME_AREA):  $RUNTIME_AREA"\n'
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
        ## SL add requirement for OS version only if SL4
        #reSL4 = re.compile( r'slc4' )
        if self.executable_arch: # and reSL4.search(self.executable_arch):
            req+=' && Member("VO-cms-' + \
                 self.executable_arch + \
                 '", other.GlueHostApplicationSoftwareRunTimeEnvironment)'

        req = req + ' && (other.GlueHostNetworkAdapterOutboundIP)'
        if common.scheduler.name() == "glitecoll":
            req += ' && other.GlueCEStateStatus == "Production" '

        return req

    def configFilename(self):
        """ return the config filename """
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
        #txt += '        echo "SET_CMS_ENV 10020 ==> ERROR $OSG_APP/cmssoft/cms/cmsset_default.sh file not found"\n'
        #txt += '        echo "JOB_EXIT_STATUS = 10020"\n'
        #txt += '        echo "JobExitCode=10020" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '        dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '        echo "ERROR ==> $OSG_APP/cmssoft/cms/cmsset_default.sh file not found"\n'
        txt += '        job_exit_code=10020\n'
        #txt += '        cd $RUNTIME_AREA\n'
        #txt += '        echo ">>> current directory (RUNTIME_AREA): $RUNTIME_AREA"\n'
        #txt += '        echo ">>> Remove working directory: $WORKING_DIR"\n'
        #txt += '        /bin/rm -rf $WORKING_DIR\n'
        #txt += '        if [ -d $WORKING_DIR ] ;then\n'
        #txt += '            echo "SET_CMS_ENV 10017 ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after $OSG_APP/cmssoft/cms/cmsset_default.sh file not found"\n'
        #txt += '            echo "JOB_EXIT_STATUS = 10017"\n'
        #txt += '            echo "JobExitCode=10017" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '            dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '            echo "ERROR ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after $OSG_APP/cmssoft/cms/cmsset_default.sh file not found"\n'
        #txt += '            job_exit_code=10017\n'
        #txt += '        fi\n'
        txt += '\n'
        #txt += '        exit 1\n'
        txt += '        func_exit\n'
        txt += '    fi\n'
        txt += '\n'
        txt += '    echo "==> setup cms environment ok"\n'
        txt += '    echo "SCRAM_ARCH = $SCRAM_ARCH"\n'

        return txt

    ### OLI_DANIELE
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
        #txt += '        echo "SET_CMS_ENV 10031 ==> ERROR CMS software dir not found on WN `hostname`"\n'
        #txt += '        echo "JOB_EXIT_STATUS = 10031" \n'
        #txt += '        echo "JobExitCode=10031" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '        dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '        exit 1\n'
        txt += '        echo "ERROR ==> CMS software dir not found on WN `hostname`"\n'
        txt += '        job_exit_code=10031\n'
        txt += '        func_exit\n'
        txt += '    else\n'
        txt += '        echo "Sourcing environment... "\n'
        txt += '        if [ ! -s $VO_CMS_SW_DIR/cmsset_default.sh ] ;then\n'
        #txt += '            echo "SET_CMS_ENV 10020 ==> ERROR cmsset_default.sh file not found into dir $VO_CMS_SW_DIR"\n'
        #txt += '            echo "JOB_EXIT_STATUS = 10020"\n'
        #txt += '            echo "JobExitCode=10020" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '            dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '            exit 1\n'
        txt += '            echo "ERROR ==> cmsset_default.sh file not found into dir $VO_CMS_SW_DIR"\n'
        txt += '            job_exit_code=10020\n'
        txt += '            func_exit\n'
        txt += '        fi\n'
        txt += '        echo "sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '        source $VO_CMS_SW_DIR/cmsset_default.sh\n'
        txt += '        result=$?\n'
        txt += '        if [ $result -ne 0 ]; then\n'
        #txt += '            echo "SET_CMS_ENV 10032 ==> ERROR problem sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        #txt += '            echo "JOB_EXIT_STATUS = 10032"\n'
        #txt += '            echo "JobExitCode=10032" | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '            dumpStatus $RUNTIME_AREA/$repo\n'
        #txt += '            exit 1\n'
        txt += '            echo "ERROR ==> problem sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '            job_exit_code=10032\n'
        txt += '            func_exit\n'
        txt += '        fi\n'
        txt += '    fi\n'
        txt += '    \n'
        txt += '    echo "==> setup cms environment ok"\n'
        return txt

    ### FEDE FOR DBS OUTPUT PUBLICATION
    def modifyReport(self, nj):
        """
        insert the part of the script that modifies the FrameworkJob Report
        """

        txt = '\n#Written by cms_cmssw::modifyReport\n'
        try:
            publish_data = int(self.cfg_params['USER.publish_data'])
        except KeyError:
            publish_data = 0
        if (publish_data == 1):

            txt += 'if [ $copy_exit_status -eq 0 ]; then\n'
            txt += '    echo ">>> Modify Job Report:" \n'
            txt += '    chmod a+x $SOFTWARE_DIR/ProdCommon/ProdCommon/FwkJobRep/ModifyJobReport.py\n'
            #txt += '    if [ -z "$SE" ]; then\n'
            #txt += '        SE="" \n'
            #txt += '    fi \n'
            #txt += '    if [ -z "$SE_PATH" ]; then\n'
            #txt += '        SE_PATH="" \n'
            #txt += '    fi \n'
            txt += '    echo "SE = $SE"\n'
            txt += '    echo "SE_PATH = $SE_PATH"\n'

            processedDataset = self.cfg_params['USER.publish_data_name']
            txt += '    ProcessedDataset='+processedDataset+'\n'
            #txt += '    if [ "$SE_PATH" == "" ]; then\n'
            #txt += '        FOR_LFN=/copy_problems/ \n'
            #txt += '    else \n'
            #txt += '        tmp=`echo $SE_PATH | awk -F \'store\' \'{print$2}\'` \n'
            #txt += '        FOR_LFN=/store$tmp \n'
            #txt += '    fi \n'
            txt += '    tmp=`echo $SE_PATH | awk -F \'store\' \'{print$2}\'` \n'
            txt += '    FOR_LFN=/store$tmp \n'
            txt += '    echo "ProcessedDataset = $ProcessedDataset"\n'
            txt += '    echo "FOR_LFN = $FOR_LFN" \n'
            txt += '    echo "CMSSW_VERSION = $CMSSW_VERSION"\n\n'
            #txt += '    echo "$SOFTWARE_DIR/ProdCommon/ProdCommon/FwkJobRep/ModifyJobReport.py crab_fjr_$NJob.xml $NJob $FOR_LFN $PrimaryDataset $DataTier $ProcessedDataset $ApplicationFamily $executable $CMSSW_VERSION $PSETHASH $SE $SE_PATH"\n'
            #txt += '    $SOFTWARE_DIR/ProdCommon/ProdCommon/FwkJobRep/ModifyJobReport.py crab_fjr_$NJob.xml $NJob $FOR_LFN $PrimaryDataset $DataTier $ProcessedDataset $ApplicationFamily $executable $CMSSW_VERSION $PSETHASH $SE $SE_PATH\n'
            ### FEDE ####
            txt += '    echo "$SOFTWARE_DIR/ProdCommon/ProdCommon/FwkJobRep/ModifyJobReport.py $RUNTIME_AREA/crab_fjr_$NJob.xml $NJob $FOR_LFN $PrimaryDataset $DataTier $ProcessedDataset $ApplicationFamily $executable $CMSSW_VERSION $PSETHASH $SE $SE_PATH"\n'
            txt += '    $SOFTWARE_DIR/ProdCommon/ProdCommon/FwkJobRep/ModifyJobReport.py $RUNTIME_AREA/crab_fjr_$NJob.xml $NJob $FOR_LFN $PrimaryDataset $DataTier $ProcessedDataset $ApplicationFamily $executable $CMSSW_VERSION $PSETHASH $SE $SE_PATH\n'
            ####################################
            txt += '    modifyReport_result=$?\n'
            txt += '    if [ $modifyReport_result -ne 0 ]; then\n'
            txt += '        modifyReport_result=70500\n'
            txt += '        job_exit_code=$modifyReport_result\n'
            txt += '        echo "ModifyReportResult=$modifyReport_result" | tee -a $RUNTIME_AREA/$repo\n'
            txt += '        echo "WARNING: Problem with ModifyJobReport"\n'
            txt += '    else\n'
            ### FEDE #####
            #txt += '        mv NewFrameworkJobReport.xml crab_fjr_$NJob.xml\n'
            #######################
            txt += '        mv NewFrameworkJobReport.xml $RUNTIME_AREA/crab_fjr_$NJob.xml\n'
            txt += '    fi\n'
            txt += 'fi\n'
        return txt

    def cleanEnv(self):
        txt = '\n#Written by cms_cmssw::cleanEnv\n'
        txt += 'if [ $middleware == OSG ]; then\n'
        txt += '    cd $RUNTIME_AREA\n'
        txt += '    echo ">>> current directory (RUNTIME_AREA): $RUNTIME_AREA"\n'
        txt += '    echo ">>> Remove working directory: $WORKING_DIR"\n'
        txt += '    /bin/rm -rf $WORKING_DIR\n'
        txt += '    if [ -d $WORKING_DIR ] ;then\n'
        txt += '        echo "ERROR ==> OSG $WORKING_DIR could not be deleted on WN `hostname` after cleanup of WN"\n'
        txt += '        job_exit_code=10017\n'
        txt += '        func_exit\n'
        txt += '    fi\n'
        txt += 'fi\n'
        txt += '\n'
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

    def outList(self):
        """
        check the dimension of the output files
        """
        txt = ''
        txt += 'echo ">>> list of expected files on output sandbox"\n'
        listOutFiles = []
        stdout = 'CMSSW_$NJob.stdout'
        stderr = 'CMSSW_$NJob.stderr'
        if (self.return_data == 1):
            for file in (self.output_file+self.output_file_sandbox):
                listOutFiles.append(self.numberFile_(file, '$NJob'))
            listOutFiles.append(stdout)
            listOutFiles.append(stderr)
        else:
            for file in (self.output_file_sandbox):
                listOutFiles.append(self.numberFile_(file, '$NJob'))
            listOutFiles.append(stdout)
            listOutFiles.append(stderr)
        txt += 'echo "output files: '+string.join(listOutFiles,' ')+'"\n'
        txt += 'filesToCheck="'+string.join(listOutFiles,' ')+'"\n'
        txt += 'export filesToCheck\n'
        return txt
