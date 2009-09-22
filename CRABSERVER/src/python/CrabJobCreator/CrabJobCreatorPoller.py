#!/usr/bin/env python
#pylint: disable-msg=W0613,W0703,E1101
"""
The actual wmbsDB poll / Subscription split / Task Extension algorithm.
It will be parallelized if needed through WorkQueue using a new class.
"""
__all__ = []
__revision__ = "$Id: CrabJobCreatorPoller.py,v 0 \
            2009/09/22 23:21:44 riahi Exp $"
__version__ = "$Revision: 0 $"


import logging
import os
from xml.dom import minidom
from IMProv.IMProvNode import IMProvNode
import traceback

# WMCORE
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory
from WMCore.WMFactory import WMFactory
from WMCore.WMBS.Subscription import Subscription
from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

# Task and job objects
from ProdCommon.BossLite.DbObjects.Job import Job

# CrabServer
from CrabServerWorker.CrabWorkerAPI import CrabWorkerAPI

# thread
import threading

# Subscriptions tracking
SUBSWATCH = {}

class CrabJobCreatorPoller(BaseWorkerThread):
    """
    Regular worker for the CrabJobCreator. Check wmbsDB for subscription, if matching  
    split it and extend task.
    """

    def __init__(self, config, threads=5):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        # BossLite attributes
        self.blDBsession = BossLiteAPI('MySQL', dbConfig, makePool=True)
        self.sessionPool = self.blDBsession.bossLiteDB.getPool()

        # Server configuration attributes
        self.config = config 

        # Tasks attributes
        self.cmdRng = "[]"
        self.taskToComplete = None 
        self.listArguments = [] 

        # DB session 
        self.blSession = BossLiteAPI('MySQL', pool=self.sessionPool)
        self.cwdb = CrabWorkerAPI( self.blSession.bossLiteDB )
 
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "CrabJobCreator.Database",
                                logger = self.logger,
                                dbinterface = myThread.dbi)

        self.getSubscription = daofactory(classname = "ListToSplit")
        self.getTaskByJobGroup = daofactory(classname = "LoadTaskNameByJgId")

        factory = WMFactory("msgService", "WMCore.MsgService."+ \
                             myThread.dialect)
        self.newMsgService = myThread.factory['msgService'].\
                             loadObject("MsgService")



    def taskExtension(self, jobGroups):
        """
        Extension work
        """
        dlsDest = []

        for dest in jobGroups:
            dlsDest.extend(dest.getLocationsForJobs())


        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Load task name from wmcore db by jobgroup id
        taskName = self.getTaskByJobGroup.execute(jobGroups[0].id)[0]

        myThread.transaction.commit()

        try:
            self.taskToComplete = self.blSession.loadTaskByName( taskName )
        except Exception, ex:
            logging.info("Unable to load task [%s]."%(taskName))
            logging.info( "Exception raised: " + str(ex) )
            logging.info( str(traceback.format_exc()) )
            return 

        # Build job dict.s
        jobs = []
        rjAttrs = {}
        attrtemp = {'processStatus': 'created', 'status': 'C', 'taskId': '1' , \
          'submission': '1', 'statusScheduler': 'Created', 'jobId': '', \
          'state': 'SubRequested', 'closed': 'N'}
        tempDict = {'executable': 'CMSSW.sh', 'outputFiles': [], 'name': '', \
          'standardError': '', 'submissionNumber': '1', 'standardOutput': '', \
          'jobId': '', 'arguments': '', 'taskId':'1' , \
          'dlsDestination': dlsDest , 'closed' :'N'} 

        jobCounter = 0
        for jI in jobGroups[0].jobs:

            jobCounter += 1
            tempDict["outputFiles"] = []
            tempDict["outputFiles"].extend(['out_files_'+\
                   str((jobGroups[0].jobs).index(jI)+1)+'.tgz','crab_fjr_'+\
                   str((jobGroups[0].jobs).index(jI)+1)+'.xml','.BrokerInfo'])
            tempDict["name"] = self.taskToComplete['name']+ \
                   '_job'+str((jobGroups[0].jobs).index(jI)+1)
            tempDict["standardError"] = 'CMSSW_'+ str((jobGroups[0].jobs).\
                    index(jI)+1)+'.stderr'
            tempDict["standardOutput"] = 'CMSSW_'+str((jobGroups[0].jobs).\
                    index(jI)+1)+'.stdout'
            tempDict["jobId"] = (jobGroups[0].jobs).index(jI) + 1
            tempDict['arguments'] = (jobGroups[0].jobs).index(jI) + 1
            tempDict['taskId'] =  self.taskToComplete['id']
            rjAttrs[tempDict["name"]] = attrtemp
            rjAttrs[tempDict["name"]]['jobId'] = \
              (jobGroups[0].jobs).index(jI) + 1

            job = Job( tempDict )
            subn = int( job['submissionNumber'] )
            if subn > 0 :
                job['submissionNumber'] = subn - 1
            else :
                job['submissionNumber'] = subn
            jobs.append(job)

        self.taskToComplete.addJobs(jobs)

        # Fill running job information
        for job in self.taskToComplete.jobs:
            
            attrs = rjAttrs[ str(job['name']) ]

            try:
                self.blSession.getRunningInstance( job, attrs )
            except Exception, e:
                logMsg = ("Problem generating RunningJob %s.%s."%( \
                           self.taskToComplete['name'], job['name']) )
                logMsg += str(e)
                logging.info( logMsg )
                logging.debug( traceback.format_exc() )
                return

            try:                
                self.blSession.updateDB( job )
            except Exception, e:
                logMsg = ("Problem generating updating bosslite DB with %s"\
                         %job['name']) 
                logMsg += str(e) 
                logging.info( logMsg )
                logging.debug( traceback.format_exc() )
                return

        # Argument.xml creation
        self.listArguments = []
        tempArg = {}

        for jI in jobGroups[0].jobs:

            tempArg['MaxEvents'] = jI['input_files'][0]['events']
            tempArg['JobID'] = (jobGroups[0].jobs).index(jI) + 1
            tempArg['InputFiles'] = jI['input_files'][0]['lfn']
            tempArg['SkipEvents'] = jI['mask']['FirstEvent']
            self.listArguments.append(tempArg)
            tempArg = {}

        # reconstruct command structures
        if not self.parseCommandXML() == 0:
            return

        try:

            self.createXML()
            self.addEntry()

        except:
            logMsg = ("Problem creating argument.xml for %s"\
                   %self.taskToComplete['name'])
            logging.info( logMsg )
            logging.debug( traceback.format_exc() )
            logging.debug("Argument.xml creation failed")

        # Create jobs in CrabWorkerDB
        if not self.registerJobs() == 0:
            return

        # Payload will be received by CW and TT 
        payload = self.taskToComplete['name'] +"::"+ \
          str(self.config.CrabJobCreator.maxRetries) +"::"+ \
          str(self.cmdRng)

        # Send message 
        myThread.transaction.begin()
        self.newMsgService.registerAs("CrabJobCreator")
        msg = {'name':'CrabJobCreatorComponent:NewTaskRegistered'\
              , 'payload':payload}
        self.newMsgService.publish(msg)
        myThread.transaction.commit()
        self.newMsgService.finish()

        return 0 

    def parseCommandXML(self):
        """
        Parse configuration files
        """
        status = 0
        cmdSpecFile = os.path.join(\
          self.config.CrabJobCreator.wdir, \
          self.taskToComplete['name']  + \
          '_spec/cmd.xml' )
        
        try:
        
            doc = minidom.parse(cmdSpecFile)
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]
            self.cmdRng =  str( cmdXML.getAttribute('Range') )
        
        except Exception, e:
            status = 6       
            reason = "Error while parsing command XML for task %s, \
             it will not be processed" % self.taskToComplete['name'] 
            reason += str(e)
            logging.info( reason )
            logging.info( traceback.format_exc() )
        return status

    def createXML(self):
        """
        Create xml file
        """
        result = IMProvNode( 'arguments' )
        file( self.config.CrabJobCreator.wdir+'/'+\
           self.taskToComplete['name']+'_spec/arguments.xml' , 'w')\
           .write(str(result))
        return

    def addEntry(self):
        """
        _addEntry_

        add an entry to the xml file
        """
        from IMProv.IMProvLoader import loadIMProvFile
        improvDoc = loadIMProvFile( \
           self.config.CrabJobCreator.wdir+'/'+\
           self.taskToComplete['name']+'_spec/arguments.xml' )

        entrname = 'Job'
        for dictions in self.listArguments:
            report = IMProvNode(entrname , None, **dictions)
            improvDoc.addNode(report) 
        file( self.config.CrabJobCreator.wdir+'/'+\
          self.taskToComplete['name']+\
          '_spec/arguments.xml' , 'w').write(str(improvDoc))
        return


    def registerJobs(self):
        """
        Register taskToComplete jobs
        """

        ranges = eval(self.cmdRng)

        for job in self.taskToComplete.jobs:
            jobName = job['name']
            cacheArea = os.path.join( \
     self.config.CrabJobCreator.wdir, \
     self.taskToComplete['name'] + '_spec', jobName )
            weStatus = 'create'
            if job['jobId'] in ranges :
                weStatus = 'Submitting'


            jobDetails = {
                          'id':jobName, 'job_type':\
                     'Processing', 'cache':cacheArea, \
                          'owner':self.taskToComplete['name']\
                          , 'status': weStatus, \
                          'max_retries': self.config.CrabJobCreator.maxRetries\
                          , 'max_racers':1 \
                         }
            jobAlreadyRegistered = False

            try:
                jobAlreadyRegistered = self.cwdb.existsWEJob(jobName)
            except Exception, e:
                logMsg = ('Error while checking job registration: \
                  assuming %s as not registered' % jobName)
                logMsg +=  str(e)
                logging.info ( logMsg )
                logging.debug( traceback.format_exc() )
                jobAlreadyRegistered = False

            if jobAlreadyRegistered == True:
                continue

            logging.debug('Registering %s'%jobName)
            try:
                self.cwdb.registerWEJob(jobDetails)
            except Exception, e:
                logMsg = 'Error while registering job for JT: %s' % jobName
                logMsg +=  traceback.format_exc()
                logging.info (logMsg)
                return 1
            logging.debug('Registration for %s performed'%jobName)
        return 0

    
    def databaseWork(self):
        """
        Queries DB for all subscriptions, if matching calls
        extension process
        """
        logging.info("starting DB Pool now...")

        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Only subscriptions for closed filesets and not splitted yet are got
        pickSub = self.getSubscription.execute()

        myThread.transaction.commit()

        logging.info('I found these new subscriptions %s to split'%pickSub)

        for sub in pickSub:

            # if subscription is found in SUBSWATCH means 
            # that it is still processed by the last thread 
            # OR the extension failed before and the splitting 
            # work is already done
            if sub not in SUBSWATCH:            

                # Load the subscrition to split 
                subscriptionToSplit = Subscription(id = sub)

                try:
                    subscriptionToSplit.load()
                except Exception, e:
                    logMsg = ("Subscription %s can't be loaded" %str(sub))
                    logMsg += str(e)
                    logging.info( logMsg )
                    logging.debug( traceback.format_exc() )
                    continue 

                # SplitterFactory object 
                splitter = SplitterFactory()
                jobFactory = splitter(package = "WMCore.WMBS",
                                  subscription = subscriptionToSplit)
                try:
                    # Get splitting algo for a WF from wmbs DB
                    # files_per_job will be moved to the client 
                    jobGroups = jobFactory(files_per_job = 100)
                except Exception, e:
                    logMsg = ("Problem when splitting %s" \
                              %str(sub))              
                    logMsg += str(e) 
                    logging.info( logMsg )
                    logging.debug( traceback.format_exc() )
                    continue 

                # Number of job 0 - exit and try again next time 
                if len(jobGroups[0].jobs)==0:
                    continue 

                # Add jobgroup to the list of subscriptions to process  
                SUBSWATCH[sub] = jobGroups
         
        # Loop on all subscription recently found 
        # and the ones for which the extension failed previously 
        for subId in SUBSWATCH.keys():
 
            # To parallelize this work if needed
            if self.taskExtension(SUBSWATCH[subId]) == 0:
                del SUBSWATCH[subId]

        # Release bossLite session        
        self.blSession.bossLiteDB.close()
        del self.blSession 

        logging.info('databases work ends')
       
    def algorithm(self, parameters):
        """
        Queries DB for all watched subscription, if matching split subscription and extend task information. 
        """
        logging.debug("Running pool / split / extend algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.databaseWork()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise

