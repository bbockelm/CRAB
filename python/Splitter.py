import common
from crab_exceptions import *
from crab_util import *
from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser

class JobSplitter:
    def __init__( self, cfg_params,  args ):
        self.cfg_params = cfg_params
        self.args=args
        #self.maxEvents
        # init BlackWhiteListParser
        seWhiteList = cfg_params.get('GRID.se_white_list',[])
        seBlackList = cfg_params.get('GRID.se_black_list',[])
        self.blackWhiteListParser = SEBlackWhiteListParser(seWhiteList, seBlackList, common.logger())


    def checkUserSettings(self):
        ## Events per job
        if self.cfg_params.has_key('CMSSW.events_per_job'):
            self.eventsPerJob =int( self.cfg_params['CMSSW.events_per_job'])
            self.selectEventsPerJob = 1
        else:
            self.eventsPerJob = -1
            self.selectEventsPerJob = 0

        ## number of jobs
        if self.cfg_params.has_key('CMSSW.number_of_jobs'):
            self.theNumberOfJobs =int( self.cfg_params['CMSSW.number_of_jobs'])
            self.selectNumberOfJobs = 1
        else:
            self.theNumberOfJobs = 0
            self.selectNumberOfJobs = 0

        if self.cfg_params.has_key('CMSSW.total_number_of_events'):
            self.total_number_of_events = int(self.cfg_params['CMSSW.total_number_of_events'])
            self.selectTotalNumberEvents = 1
            if self.selectNumberOfJobs  == 1:
                if (self.total_number_of_events != -1) and int(self.total_number_of_events) < int(self.theNumberOfJobs):
                    msg = 'Must specify at least one event per job. total_number_of_events > number_of_jobs '
                    raise CrabException(msg)
        else:
            self.total_number_of_events = 0
            self.selectTotalNumberEvents = 0


########################################################################
    def jobSplittingByEvent( self ):
        """
        Perform job splitting. Jobs run over an integer number of files
        and no more than one block.
        ARGUMENT: blockSites: dictionary with blocks as keys and list of host sites as values
        REQUIRES: self.selectTotalNumberEvents, self.selectEventsPerJob, self.selectNumberofJobs,
                  self.total_number_of_events, self.eventsPerJob, self.theNumberOfJobs,
                  self.maxEvents, self.filesbyblock
        SETS: jobDestination - Site destination(s) for each job (a list of lists)
              self.total_number_of_jobs - Total # of jobs
              self.list_of_args - File(s) job will run on (a list of lists)
        """

        jobDestination=[]  
        self.checkUserSettings()
        if ( (self.selectTotalNumberEvents + self.selectEventsPerJob + self.selectNumberOfJobs) != 2 ):
            msg = 'Must define exactly two of total_number_of_events, events_per_job, or number_of_jobs.'
            raise CrabException(msg)
 
        blockSites = self.args['blockSites'] 
        pubdata = self.args['pubdata']
        filesbyblock=pubdata.getFiles()

        self.eventsbyblock=pubdata.getEventsPerBlock()
        self.eventsbyfile=pubdata.getEventsPerFile()
        self.parentFiles=pubdata.getParent()

        ## get max number of events
        self.maxEvents=pubdata.getMaxEvents()

        self.useParent = int(self.cfg_params.get('CMSSW.use_parent',0))
        noBboundary = int(self.cfg_params.get('CMSSW.no_block_boundary',0))

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
            common.logger.info("Requested "+str(self.total_number_of_events)+ " events, but only "+str(self.maxEvents)+" events are available.")
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
            common.logger.info("May not create the exact number_of_jobs requested.")

        # old... to remove Daniele
        totalNumberOfJobs = 999999999

        blocks = blockSites.keys()
        blockCount = 0
        # Backup variable in case self.maxEvents counted events in a non-included block
        numBlocksInDataset = len(blocks)

        jobCount = 0
        list_of_lists = []

        # list tracking which jobs are in which jobs belong to which block
        jobsOfBlock = {}

        parString = ""
        pString = ""
        filesEventCount = 0

        # ---- Iterate over the blocks in the dataset until ---- #
        # ---- we've met the requested total # of events    ---- #
        while ( (eventsRemaining > 0) and (blockCount < numBlocksInDataset) and (jobCount < totalNumberOfJobs)):
            block = blocks[blockCount]
            blockCount += 1
            if block not in jobsOfBlock.keys() :
                jobsOfBlock[block] = []

            if self.eventsbyblock.has_key(block) :
                numEventsInBlock = self.eventsbyblock[block]
                common.logger.debug('Events in Block File '+str(numEventsInBlock))

                files = filesbyblock[block]
                numFilesInBlock = len(files)
                if (numFilesInBlock <= 0):
                    continue
                fileCount = 0
                if noBboundary == 0: # DD
                    # ---- New block => New job ---- #
                    parString = ""
                    pString=""
                    # counter for number of events in files currently worked on
                    filesEventCount = 0
                # flag if next while loop should touch new file
                newFile = 1
                # job event counter
                jobSkipEventCount = 0

                # ---- Iterate over the files in the block until we've met the requested ---- #
                # ---- total # of events or we've gone over all the files in this block  ---- #
                msg='\n'
                while ( (eventsRemaining > 0) and (fileCount < numFilesInBlock) and (jobCount < totalNumberOfJobs) ):
                    file = files[fileCount]
                    if self.useParent==1:
                        parent = self.parentFiles[file]
                        common.logger.log(10-1, "File "+str(file)+" has the following parents: "+str(parent))
                    if newFile :
                        try:
                            numEventsInFile = self.eventsbyfile[file]
                            common.logger.log(10-1, "File "+str(file)+" has "+str(numEventsInFile)+" events")
                            # increase filesEventCount
                            filesEventCount += numEventsInFile
                            # Add file to current job
                            parString +=  file + ','
                            if self.useParent==1:
                                for f in parent :
                                    pString += f  + ','
                            newFile = 0
                        except KeyError:
                            common.logger.info("File "+str(file)+" has unknown number of events: skipping")

                    eventsPerJobRequested = min(eventsPerJobRequested, eventsRemaining)
                    # if less events in file remain than eventsPerJobRequested
                    if ( filesEventCount - jobSkipEventCount < eventsPerJobRequested):
                        if noBboundary == 1: ## DD
                            newFile = 1
                            fileCount += 1
                        else:
                            # if last file in block
                            if ( fileCount == numFilesInBlock-1 ) :
                                # end job using last file, use remaining events in block
                                # close job and touch new file
                                fullString = parString[:-1]
                                if self.useParent==1:
                                    fullParentString = pString[:-1]
                                    list_of_lists.append([fullString,fullParentString,str(-1),str(jobSkipEventCount)])
                                else:
                                    list_of_lists.append([fullString,str(-1),str(jobSkipEventCount)])
                                msg += "Job %s can run over %s  events (last file in block).\n"%(str(jobCount+1), str(filesEventCount - jobSkipEventCount))
                                jobDestination.append(blockSites[block])
                                msg += "Job %s Destination: %s\n"%(str(jobCount+1),str(jobDestination[jobCount]))
                                # fill jobs of block dictionary
                                jobsOfBlock[block].append(jobCount+1)
                                # reset counter
                                jobCount = jobCount + 1
                                totalEventCount = totalEventCount + filesEventCount - jobSkipEventCount
                                eventsRemaining = eventsRemaining - filesEventCount + jobSkipEventCount
                                jobSkipEventCount = 0
                                # reset file
                                pString = ""
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
                        fullString = parString[:-1]
                        if self.useParent==1:
                            fullParentString = pString[:-1]
                            list_of_lists.append([fullString,fullParentString,str(eventsPerJobRequested),str(jobSkipEventCount)])
                        else:
                            list_of_lists.append([fullString,str(eventsPerJobRequested),str(jobSkipEventCount)])
                        msg += "Job %s can run over %s events.\n"%(str(jobCount+1),str(eventsPerJobRequested))
                        jobDestination.append(blockSites[block])
                        msg+= "Job %s Destination: %s\n"%(str(jobCount+1),str(jobDestination[jobCount]))
                        jobsOfBlock[block].append(jobCount+1)
                        # reset counter
                        jobCount = jobCount + 1
                        totalEventCount = totalEventCount + eventsPerJobRequested
                        eventsRemaining = eventsRemaining - eventsPerJobRequested
                        jobSkipEventCount = 0
                        # reset file
                        pString = ""
                        parString = ""
                        filesEventCount = 0
                        newFile = 1
                        fileCount += 1

                    # if more events in file remain than eventsPerJobRequested
                    else :
                        # close job but don't touch new file
                        fullString = parString[:-1]
                        if self.useParent==1:
                            fullParentString = pString[:-1]
                            list_of_lists.append([fullString,fullParentString,str(eventsPerJobRequested),str(jobSkipEventCount)])
                        else:
                            list_of_lists.append([fullString,str(eventsPerJobRequested),str(jobSkipEventCount)])
                        msg += "Job %s can run over %s events.\n"%(str(jobCount+1),str(eventsPerJobRequested))
                        jobDestination.append(blockSites[block])
                        msg+= "Job %s Destination: %s\n"%(str(jobCount+1),str(jobDestination[jobCount]))
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
                        pString_tmp=''
                        if self.useParent==1:
                            for f in parent : pString_tmp +=  f + ','
                        pString =  pString_tmp 
                        parString =  file + ','
                    pass # END if
                pass # END while (iterate over files in the block)
        pass # END while (iterate over blocks in the dataset)
        common.logger.debug(msg)
        self.ncjobs = self.total_number_of_jobs = jobCount
        if (eventsRemaining > 0 and jobCount < totalNumberOfJobs ):
            common.logger.info("Could not run on all requested events because some blocks not hosted at allowed sites.")
        common.logger.info(str(jobCount)+" job(s) can run on "+str(totalEventCount)+" events.\n")
 
        # skip check on  block with no sites  DD
        if noBboundary == 0 : self.checkBlockNoSite(blocks,jobsOfBlock,blockSites)

       # prepare dict output
        dictOut = {}
        dictOut['params']= ['InputFiles','MaxEvents','SkipEvents']
        if self.useParent: dictOut['params']= ['InputFiles','ParentFiles','MaxEvents','SkipEvents']
        dictOut['args'] = list_of_lists
        dictOut['jobDestination'] = jobDestination
        dictOut['njobs']=self.total_number_of_jobs

        return dictOut

        # keep trace of block with no sites to print a warning at the end

    def checkBlockNoSite(self,blocks,jobsOfBlock,blockSites):   
        # screen output
        screenOutput = "List of jobs and available destination sites:\n\n"
        noSiteBlock = []
        bloskNoSite = []
        allBlock = []

        blockCounter = 0
        for block in blocks:
            if block in jobsOfBlock.keys() :
                blockCounter += 1
                allBlock.append( blockCounter )
                screenOutput += "Block %5i: jobs %20s: sites: %s\n" % (blockCounter,spanRanges(jobsOfBlock[block]),
                    ','.join(self.blackWhiteListParser.checkWhiteList(self.blackWhiteListParser.checkBlackList(blockSites[block],[block]),[block])))
                if len(self.blackWhiteListParser.checkWhiteList(self.blackWhiteListParser.checkBlackList(blockSites[block],[block]),[block])) == 0:
                    noSiteBlock.append( spanRanges(jobsOfBlock[block]) )
                    bloskNoSite.append( blockCounter )

        common.logger.info(screenOutput)
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
            if self.cfg_params.has_key('GRID.se_white_list'):
                msg += 'WARNING: SE White List: '+self.cfg_params['GRID.se_white_list']+'\n'
                msg += '(Hint: By whitelisting you force the job to run at this particular site(s).\n'
                msg += 'Please check if the dataset is available at this site!)\n'
            if self.cfg_params.has_key('GRID.ce_white_list'):
                msg += 'WARNING: CE White List: '+self.cfg_params['GRID.ce_white_list']+'\n'
                msg += '(Hint: By whitelisting you force the job to run at this particular site(s).\n'
                msg += 'Please check if the dataset is available at this site!)\n'

            common.logger.info(msg)

        if bloskNoSite == allBlock:
            raise CrabException('No jobs created') 

        return


########################################################################
    def jobSplittingByRun(self): 
        """
        """
        from sets import Set  
        from WMCore.JobSplitting.RunBased import RunBased
        from WMCore.DataStructs.Workflow import Workflow
        from WMCore.DataStructs.File import File
        from WMCore.DataStructs.Fileset import Fileset
        from WMCore.DataStructs.Subscription import Subscription
        from WMCore.JobSplitting.SplitterFactory import SplitterFactory
        from WMCore.DataStructs.Run import Run 

        self.checkUserSettings()
        blockSites = self.args['blockSites'] 
        pubdata = self.args['pubdata']

        if self.selectNumberOfJobs == 0 :
            self.theNumberOfJobs = 9999999
        blocks = {}
        runList = [] 
        thefiles = Fileset(name='FilesToSplit')
        fileList = pubdata.getListFiles()
        for f in fileList:
            block = f['Block']['Name']
            try: 
                f['Block']['StorageElementList'].extend(blockSites[block])
            except:
                continue
            wmbsFile = File(f['LogicalFileName'])
            [ wmbsFile['locations'].add(x) for x in blockSites[block] ]
            wmbsFile['block'] = block
            runNum = f['RunsList'][0]['RunNumber']
            runList.append(runNum) 
            myRun = Run(runNumber=runNum)
            wmbsFile.addRun( myRun )
            thefiles.addFile(
                wmbsFile
                )
 
        work = Workflow()
        subs = Subscription(
        fileset = thefiles,
        workflow = work,
        split_algo = 'RunBased',
        type = "Processing")
        splitter = SplitterFactory()
        jobfactory = splitter(subs)
        
        #loop over all runs 
        set = Set(runList)
        list_of_lists = []
        jobDestination = []
        count = 0
        for jobGroup in  jobfactory():
            if count <  self.theNumberOfJobs:
                res = self.getJobInfo(jobGroup)
                parString = '' 
                for file in res['lfns']:
                    parString += file + ','
                fullString = parString[:-1]
                list_of_lists.append([fullString,str(-1),str(0)])     
                #need to check single file location
                jobDestination.append(res['locations'])   
                count +=1
       # prepare dict output
        dictOut = {}
        dictOut['params']= ['InputFiles','MaxEvents','SkipEvents']
        dictOut['args'] = list_of_lists
        dictOut['jobDestination'] = jobDestination
        dictOut['njobs']=count

        return dictOut

    def getJobInfo( self,jobGroup ):
        res = {}
        lfns = []         
        locations = []         
        tmp_check=0
        for job in jobGroup.jobs:
            for file in job.getFiles():
                lfns.append(file['lfn']) 
                for loc in file['locations']:
                    if tmp_check < 1 :
                        locations.append(loc)
                tmp_check = tmp_check + 1 
                ### qui va messo il check per la locations 
        res['lfns'] = lfns 
        res['locations'] = locations 
        return res                
       
########################################################################
    def jobSplittingNoInput(self):
        """
        Perform job splitting based on number of event per job
        """
        common.logger.debug('Splitting per events')
        self.checkUserSettings()
        jobDestination=[]
        if ( (self.selectTotalNumberEvents + self.selectEventsPerJob + self.selectNumberOfJobs) != 2 ):
            msg = 'Must define exactly two of total_number_of_events, events_per_job, or number_of_jobs.'
            raise CrabException(msg)

        managedGenerators =self.args['managedGenerators']
        generator = self.args['generator']
        firstRun = self.cfg_params.get('CMSSW.first_run',None)

        if (self.selectEventsPerJob):
            common.logger.info('Required '+str(self.eventsPerJob)+' events per job ')
        if (self.selectNumberOfJobs):
            common.logger.info('Required '+str(self.theNumberOfJobs)+' jobs in total ')
        if (self.selectTotalNumberEvents):
            common.logger.info('Required '+str(self.total_number_of_events)+' events in total ')

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

        common.logger.debug('N jobs  '+str(self.total_number_of_jobs))

        # is there any remainder?
        check = int(self.total_number_of_events) - (int(self.total_number_of_jobs)*self.eventsPerJob)

        common.logger.debug('Check  '+str(check))

        common.logger.info(str(self.total_number_of_jobs)+' jobs can be created, each for '+str(self.eventsPerJob)+' for a total of '+str(self.total_number_of_jobs*self.eventsPerJob)+' events')
        if check > 0:
            common.logger.info('Warning: asked '+str(self.total_number_of_events)+' but can do only '+str(int(self.total_number_of_jobs)*self.eventsPerJob))

        # argument is seed number.$i
        self.list_of_args = []
        for i in range(self.total_number_of_jobs):
            ## Since there is no input, any site is good
            jobDestination.append([""]) #must be empty to write correctly the xml
            args=[]
            if (firstRun):
                ## pythia first run
                args.append(str(firstRun)+str(i))
            if (generator in managedGenerators):
                if (generator == 'comphep' and i == 0):
                    # COMPHEP is brain-dead and wants event #'s like 1,100,200,300
                    args.append('1')
                else:
                    args.append(str(i*self.eventsPerJob))
            args.append(str(self.eventsPerJob))
            self.list_of_args.append(args)
       # prepare dict output

        dictOut = {}
        dictOut['params'] = ['MaxEvents']
        if (firstRun):
            dictOut['params'] = ['FirstRun','MaxEvents']
            if ( generator in managedGenerators ) : dictOut['params'] = ['FirstRun', 'FirstEvent', 'MaxEvents'] 
        else:  
            if (generator in managedGenerators) : dictOut['params'] = ['FirstEvent', 'MaxEvents']
        dictOut['args'] = self.list_of_args
        dictOut['jobDestination'] = jobDestination
        dictOut['njobs']=self.total_number_of_jobs

        return dictOut


    def jobSplittingForScript(self):
        """
        Perform job splitting based on number of job
        """
        self.checkUserSettings()
        if (self.selectNumberOfJobs == 0):
            msg = 'must specify  number_of_jobs.'
            raise crabexception(msg)
        jobDestination = []
        common.logger.debug('Splitting per job')
        common.logger.info('Required '+str(self.theNumberOfJobs)+' jobs in total ')

        self.total_number_of_jobs = self.theNumberOfJobs

        common.logger.debug('N jobs  '+str(self.total_number_of_jobs))

        common.logger.info(str(self.total_number_of_jobs)+' jobs can be created')

        # argument is seed number.$i
        #self.list_of_args = []
        for i in range(self.total_number_of_jobs):
            jobDestination.append([""])
        #   self.list_of_args.append([str(i)])

       # prepare dict output
        dictOut = {}
        dictOut['args'] = [] # self.list_of_args
        dictOut['jobDestination'] = jobDestination
        dictOut['njobs']=self.total_number_of_jobs
        return dictOut
 

    def jobSplittingByLumi(self): 
        """
        """
        return
    def Algos(self):
        """
        Define key splittingType matrix
        """
        SplitAlogs = { 
                     'EventBased'           : self.jobSplittingByEvent, 
                     'RunBased'             : self.jobSplittingByRun,
                     'LumiBased'            : self.jobSplittingByLumi, 
                     'NoInput'              : self.jobSplittingNoInput, 
                     'ForScript'            : self.jobSplittingForScript
                     }  
        return SplitAlogs

