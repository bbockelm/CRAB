#!/usr/bin/env python
#pylint: disable-msg=W0613,W0703,E1101
"""
The actual wmbsDB poll / Subscription split / Task Extension algorithm.
It will be parallelized if needed through WorkQueue using a new class.
"""
__all__ = []
__revision__ = "$Id: CrabJobCreatorPoller.py,v 1.3 \
            2009/11/06 12:21:44 hriahi Exp $"
__version__ = "$Revision: 1.0 $"

import logging
import os
import sha
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
from ProdCommon.Credential.CredentialAPI import CredentialAPI

from ProdCommon.Storage.SEAPI.SElement import SElement
from ProdCommon.Storage.SEAPI.SBinterface import SBinterface
from ProdCommon.Storage.SEAPI.Exceptions import *

# CrabServer
from CrabServerWorker.CrabWorkerAPI import CrabWorkerAPI

# thread
import threading

# Subscriptions tracking
SUBSWATCH = {}

class CrabJobCreatorPoller(BaseWorkerThread):
    """
    Regular worker for the CrabJobCreatorPoller. Check wmbsDB for subscription, if matching  
    split it and extend task.
    """

    def __init__(self, config, threads=5):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config 

        # BossLite attributes
        self.blDBsession = BossLiteAPI('MySQL', pool=self.config['blSessionPool'])
        self.cwdb = CrabWorkerAPI( self.blDBsession.bossLiteDB )

        # Transfer parameter
        self.copyTout = self.config['copyTout']

        # Tasks attributes
        self.cmdRng = "[]"
        self.taskToComplete = None 
        self.listArguments = [] 
        self.cfgParams = {}     
        self.owner = None
        self.credential = None

        # Arguments.xml path
        self.pathFile = None

        # Credential attributes
        self.credAPI = CredentialAPI(\
{'credential':self.config['credentialType']})
        self.newMsgService = self.config['messageService']
  
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "CrabJobCreator.Database",
                                logger = self.logger,
                                dbinterface = myThread.dbi)

        self.getTaskByJobGroup = daofactory(classname = "LoadTaskNameByJgId")
        self.getTaskBySubscription = daofactory(classname = "LoadTaskNameBySubId")

        WMBSdao = DAOFactory(package = "WMCore.WMBS",
                                logger = self.logger,
                                dbinterface = myThread.dbi)
        
        self.getListSubscription = WMBSdao(classname = "Subscriptions.ListIncomplete")

    def taskExtension(self, jobGroups):
        """
        Extension work
        """

        dlsDest = []

        for dest in jobGroups:
            dlsDest.extend(dest.getLocationsForJobs())

        logging.info("DEST GOT %s" %dlsDest)

        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Load task name for wmcore db by jobgroup id
        taskName = self.getTaskByJobGroup.execute(jobGroups[0].id)[0]

        myThread.transaction.commit()
        logging.info("Task To Load %s" %taskName)

        try:
            self.taskToComplete = self.blDBsession.loadTaskByName( taskName )
        except Exception, ex:
            logging.info("Unable to load task [%s]." %(taskName))
            logging.info( "Exception raised: " + str(ex) )
            logging.info( str(traceback.format_exc()) )
            self.markTaskAsNotSubmitted( 'all' )
            return 

        logging.info("Task loaded [%s]" %self.taskToComplete['name'])

        # Build job dict.s
        rjAttrs = {}
        attrtemp = {'processStatus': 'created', 'status': 'C', 'taskId': '1' , \
          'submission': '1', 'statusScheduler': 'Created', 'jobId': '', \
          'state': 'Created', 'closed': 'N'}
        tempDict = {'executable': 'CMSSW.sh', 'outputFiles': [], 'name': '', \
          'standardError': '', 'submissionNumber': '1', 'standardOutput': '', \
          'jobId': '', 'wmbsJobId': '', 'arguments': '', 'taskId':'1' , \
          'dlsDestination': dlsDest , 'closed' :'N'} 

        jobCounter = 0
        jobNumber = len(self.taskToComplete.getJobs())
        logging.info(" This task has already %s jobs" %jobNumber)

        for jI in jobGroups[0].jobs:

            jobCounter += 1

            tempDict["outputFiles"] = []
            tempDict["outputFiles"].extend(['out_files_'+\
                   str((jobGroups[0].jobs).index(jI)+1+jobNumber)+'.tgz','crab_fjr_'+\
                   str((jobGroups[0].jobs).index(jI)+1+jobNumber)+'.xml','.BrokerInfo'])
            tempDict["name"] = self.taskToComplete['name']+ \
                   '_job'+str((jobGroups[0].jobs).index(jI)+1+jobNumber)
            tempDict["standardError"] = 'CMSSW_'+ str((jobGroups[0].jobs).\
                    index(jI)+1)+'.stderr'
            tempDict["standardOutput"] = 'CMSSW_'+str((jobGroups[0].jobs).\
                    index(jI)+1)+'.stdout'
            tempDict["jobId"] = (jobGroups[0].jobs).index(jI) + 1 + jobNumber
            tempDict["wmbsJobId"] = jI['id']
            tempDict['arguments'] = (jobGroups[0].jobs).index(jI) + 1 + jobNumber
            tempDict['taskId'] =  self.taskToComplete['id']
            rjAttrs[tempDict["name"]] = attrtemp
            rjAttrs[tempDict["name"]]['jobId'] = \
              (jobGroups[0].jobs).index(jI) + 1 + jobNumber

            logging.debug("THE JOB DICT is %s" %tempDict)
 
            job = Job( tempDict )
            subn = int( job['submissionNumber'] )
            if subn > 0 :
                job['submissionNumber'] = subn - 1
            else :
                job['submissionNumber'] = subn

            if jobNumber > 0 :

                self.taskToComplete.appendJob(job)

            else:

                self.taskToComplete.addJob(job)

        # Fill running job information
        for job in self.taskToComplete.jobs:

            if str(job['name']) in rjAttrs:

                attrs = rjAttrs[ str(job['name']) ]

                try:
                    self.blDBsession.getRunningInstance( job, attrs )
                except Exception, e:
                    logMsg = ("Problem generating RunningJob %s.%s.%s."%( \
                               self.taskToComplete['name'], job['name']\
                                   , job['jobId']) )
                    logMsg += str(e)
                    logging.info( logMsg )
                    logging.info( traceback.format_exc() )
                    self.markTaskAsNotSubmitted(\
                             job['jobId'])
                    continue 

                try:                
                    self.blDBsession.updateDB( job )
                except Exception, e:
                    logMsg = ("Problem updating bosslite DB with %s of id %s"\
                             %(job['name'], job['jobId'])) 
                    logMsg += str(e) 
                    logging.info( logMsg )
                    logging.info( traceback.format_exc() )
                    self.markTaskAsNotSubmitted(\
                          job['jobId'])
                    continue 

        logging.info("Bosslite DB Update Ends ")
 
        # Argument.xml creation
        self.listArguments = []
        tempArg = {}

        for jI in jobGroups[0].jobs:

            tempArg['MaxEvents'] = jI['input_files'][0]['events']
            tempArg['JobID'] = (jobGroups[0].jobs).index(jI) + 1 + jobNumber
            tempArg['InputFiles'] = jI['input_files'][0]['lfn']
            tempArg['SkipEvents'] = 0
            if jI['mask']['FirstEvent']:tempArg['SkipEvents'] = jI['mask']['FirstEvent']
            self.listArguments.append(tempArg)
            tempArg = {}

        # Reconstruct command structures
        logging.info("Parsing XML")

        if not self.parseCommandXML() == 0:
            self.markTaskAsNotSubmitted( 'all' )
            return

        self.pathFile = self.config['CacheDir'] \
      + '/'+ self.taskToComplete['name']+'_spec/arguments.xml'

        logging.info("Creating XML")

        # Create argument.xml
        try:

            file( self.pathFile )

        except IOError, e:

            logMsg = ("File doesn't exist...")
            logging.info( logMsg )
            logging.info( traceback.format_exc() ) 
            result = IMProvNode( 'arguments' )
            file( self.pathFile , 'w').write(str(result))


        except:
            logMsg = ("Problem creating argument.xml for %s"\
                   %self.taskToComplete['name'])
            logging.info( logMsg )
            logging.info( traceback.format_exc() )
            logging.info("Argument.xml creation failed")
            self.markTaskAsNotSubmitted( 'all' )
            return

        # Add entry in xml file
        self.addEntry()

        # Copy arguments.xml from a local FS to gridftp server  
        logging.info("Copying file")
        self.copyFile()


        # Create jobs in CrabWorkerDB
        logging.info("Registering job")
        if not self.registerJobs() == 0:
            return

        # Building cmdRng
        logging.debug("Rng needed %s and length %s" %(self.cmdRng,len(eval(self.cmdRng))))

        if jobNumber > 0:

            logging.debug("Building new job rangs after %s" %jobNumber)
            tmp = [] 
            for i in eval(self.cmdRng):

                if int(i) > jobNumber :
                    tmp.append(i)

            self.cmdRng = tmp
            logging.debug("Try to submit %s" %self.cmdRng)   

        # Payload will be received by CW and TT 
        payload = self.taskToComplete['name'] +"::"+ \
          str(self.config['retries']) +"::"+ \
          str(self.cmdRng)

        # Send message 
        myThread.transaction.begin()
        msg = {'name':'CrabJobCreatorComponent:NewTaskRegistered'\
              , 'payload':payload}
        self.newMsgService.publish(msg)
        myThread.transaction.commit()
        self.newMsgService.finish()

        logging.info("Sending message finish")
        return 0 

    def copyFile(self):
        """
        Copy arguments file from local path to gridftp server
        """


        # init SE interface
        logging.info("Starting copying to %s " \
     %str(self.config['SEurl']))
        try:
            seEl = SElement(self.config['SEurl']\
                 , self.config['SEproto'], \
                  self.config['SEport'])

        except Exception, ex:
            logMsg = ("ERROR : Unable to create \
   SE destination interface for %s" %self.taskToComplete['name'])
            logMsg += str(ex)
            logging.info( logMsg )
            logging.info( traceback.format_exc() )
            self.markTaskAsNotSubmitted( 'all' )
            return 

        try:
            loc = SElement("localhost", "local")
        except Exception, ex:    
            logMsg = ("ERROR : Unable to create SE source interface for %s" \
               %self.taskToComplete['name'])
            logMsg += str(ex)
            logging.info( logMsg )
            logging.info( traceback.format_exc() )
            self.markTaskAsNotSubmitted( 'all' )
            return 
         
        sbi = SBinterface( loc, seEl )

        source = os.path.abspath(self.pathFile)
        dest = os.path.join(self.config['storagePath'],\
          self.taskToComplete['name'] , os.path.basename(self.pathFile))

        logging.info("Copying From " + source + "To" + dest)

        try:
            sbi.copy( source, dest, proxy=self.taskToComplete['user_proxy'], opt=self.copyTout)
        except AuthorizationException, ex:
            logging.info(str(ex.detail))
            msg = "ERROR: Unable to copy file on the Storage Element: %s " \
                 % str(ex)
            msg += "File for "+ self.taskToComplete['name'] +" not copied \n"
            logging.info(msg)
            self.markTaskAsNotSubmitted( 'all' )
            return 
        except Exception, ex:
            logging.info( str(ex) )
            self.markTaskAsNotSubmitted( 'all' ) 
            return

    def markTaskAsNotSubmitted(self, cmdRng):
        """
        Marks cmdRng jobs for taskToComplete as not submitted 
        """

        if cmdRng == 'all':
            cmdRng = [ j['jobId'] for j in self.taskToComplete.jobs ]
        else:
            cmdRng = [cmdRng] 

        if self.taskToComplete is not None:
   
            # register we_Jobs
            jobSpecId = []
            for job in self.taskToComplete.jobs:
                jobName = job['name']
                cacheArea = os.path.join( \
        self.config['CacheDir'], str(\
             self.taskToComplete['name'] + '_spec'), jobName )
                jobDetails = {'id':jobName, 'job_type':\
              'Processing', 'cache':cacheArea, \
          'owner':self.taskToComplete['name'], 'status': 'create', \
    'max_retries':self.config['retries'], 'max_racers':1 }

                try:
                    if self.cwdb.existsWEJob(jobName) == False:
                        self.cwdb.registerWEJob(jobDetails)
                        if job['jobId'] in cmdRng and \
                     job['jobId'] not in jobSpecId:
                            jobSpecId.append(jobName)
                except Exception, e:
                    logging.error(str(e))
                    logging.error(traceback.format_exc())
                    continue

            # mark as failed
            self.cwdb.stopResubmission(jobSpecId)

            for jId in jobSpecId:
                try:
                    self.cwdb.updateWEStatus(jId, 'reallyFinished')
                except Exception, e:
                    logging.error(str(e))
                    logging.error(traceback.format_exc())
                    continue
            logging.info('Task %s successfully marked '\
                 %self.taskToComplete['name'])

        else:

            logging.info("Fallback not submitted marking for '%s'"\
                  %self.taskToComplete['name'])
            for jobbe in cmdRng:
                jobName = self.taskToComplete['name'] + "_job" + str(jobbe)
                try:
                    if not self.cwdb.existsWEJob(jobName):
                        cacheArea = os.path.join( \
self.config['CacheDir'], str(self.taskToComplete['name'] \
                  + '_spec'), jobName )
                        jobDetails = {'id':jobName, 'job_type':'Processing', \
'cache':cacheArea, 'owner':self.taskToComplete['name'], 'status': \
'reallyFinished', 'max_retries':self.config['retries'], \
                          'max_racers':1 }
                        self.cwdb.registerWEJob(jobDetails)
                    else:
                        self.cwdb.updateWEStatus(jId, 'reallyFinished')
                except Exception, e:
                    logging.error(str(e))
                    logging.error(traceback.format_exc())

        return


    def parseCommandXML(self):
        """
        Parse configuration files
        """
        status = 0
        cmdSpecFile = os.path.join(\
          self.config['CacheDir'], \
          self.taskToComplete['name']  + \
          '_spec/cmd.xml' )
        
        try:
            doc = minidom.parse(cmdSpecFile)
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]

            self.cmdRng =  str( cmdXML.getAttribute('Range') )
            self.owner = str( cmdXML.getAttribute('Subject') )

        except Exception, e:
            status = 6       
            reason = "Error while parsing command XML for task %s, \
             it will not be processed" % self.taskToComplete['name'] 
            reason += str(e)
            logging.info( reason )
            logging.info( traceback.format_exc() )
        return status

    def addEntry(self):
        """
        _addEntry_

        add an entry to the xml file
        """
        from IMProv.IMProvLoader import loadIMProvFile
        improvDoc = loadIMProvFile( self.pathFile ) 

        entrname = 'Job'
        for dictions in self.listArguments:
            report = IMProvNode(entrname , None, **dictions)
            improvDoc.addNode(report) 
        file( self.pathFile , 'w').write(str(improvDoc))
        return
 
    def registerJobs(self):
        """
        Register taskToComplete jobs
        """
        cmdRng_tmp=[]
        ranges = eval(self.cmdRng)

        for job in self.taskToComplete.jobs:
            jobName = job['name']
            cacheArea = os.path.join( \
     self.config['CacheDir'], \
     self.taskToComplete['name'] + '_spec', jobName )
            weStatus = 'create'
            if len(ranges)>0: 
                if job['jobId'] in ranges : weStatus = 'Submitting'

            else:
                logging.info('Registration %s' %jobName)
                weStatus = 'Submitting'              
            jobDetails = {
                          'id':jobName, 'job_type':\
                     'Processing', 'cache':cacheArea, \
                          'owner':self.taskToComplete['name']\
                          , 'status': weStatus, \
                          'max_retries': self.config['retries']\
                          , 'max_racers':1 \
                         }
            jobAlreadyRegistered = True 

            try:
                logging.info('%s will be verified' %jobName)             
                jobAlreadyRegistered = self.cwdb.existsWEJob(jobName)
       
            except Exception, e:
                logMsg = ('Error while checking job registration: \
                  assuming %s as not registered' % jobName)
                logMsg +=  str(e)
                logging.info ( logMsg )
                logging.info( traceback.format_exc() ) 
                jobAlreadyRegistered = False

            if jobAlreadyRegistered == True:
                logging.info('%s already registred' %jobName)
                continue

            logging.info('Registering %s'%jobName)
            try:
                self.cwdb.registerWEJob(jobDetails)
            except Exception, e:
                logMsg = 'Error while registering job for JT: %s' % jobName
                logMsg +=  traceback.format_exc()
                logging.info (logMsg)
                return 1

            cmdRng_tmp.append(job['jobId'])
            logging.info('Registration for %s performed'%jobName)
        if len(ranges) < 1: self.cmdRng=str(cmdRng_tmp)
        return 0

    def getSplitParam(self, subId):
        """
        Get split parameters for a subId from the client 
        """

        
        # Load task name for wmcore db by subscription id
        taskName = self.getTaskBySubscription.execute(subId)[0]

        status = 0
        cmdSpecFile = os.path.join(\
          self.config['CacheDir'], \
          taskName  + \
          '_spec/cmd.xml' )

        try:
            doc = minidom.parse(cmdSpecFile)
            cmdXML = doc.getElementsByTagName("TaskCommand")[0]
            cfgParams = eval( cmdXML.getAttribute("CfgParamDict"), {}, {} )

            splitPerJob = cfgParams.get('split_per_job','files_per_job')
            splitValue = cfgParams.get('split_value',1)

        except Exception, e:
            status = 6
            reason = "Error while parsing XML file for task %s, \
             it will not be processed" % taskName
            reason += str(e)
            logging.info( reason )
            logging.info( traceback.format_exc() )
        return splitPerJob, splitValue 
 
 
    def databaseWork(self):
        """
        Queries DB for all subscriptions, if matching calls
        extension process
        """
        logging.info("starting DB Pool now...")

        myThread = threading.currentThread()

        pickSub = self.getListSubscription.execute()

        logging.info('I found these new subscriptions %s to split'%pickSub)

  
        # Create dictionary of subscriptions that will be processd 
        for sub in pickSub:

            # if subscription is found in SUBSWATCH means 
            # that it is still processed by the last thread 
            if sub not in SUBSWATCH:            

                # Load the subscrition to split 
                subscriptionToSplit = Subscription(id = sub)

                try:
                    subscriptionToSplit.load()
                except Exception, e:
                    logMsg = ("Subscription %s can't be loaded" %str(sub))
                    logMsg += str(e)
                    logging.info( logMsg )
                    logging.info( traceback.format_exc() )
                    continue 

                # SplitterFactory object 
                splitter = SplitterFactory()
                jobFactory = splitter(package = "WMCore.WMBS",
                                  subscription = subscriptionToSplit)
                try:
                    splitPerJob, splittingValue = self.getSplitParam( sub )
                    kwargs = { splitPerJob : splittingValue }
                    jobGroups = jobFactory( **kwargs )
                    #jobGroups = jobFactory( events_per_job = 10 )
                except Exception, e:
                    logMsg = ("Problem when splitting %s" \
                              %str(sub))              
                    logMsg += str(e) 
                    logging.info( logMsg )
                    logging.info( traceback.format_exc() )
                    continue 

                # Number of job 0 - exit and try again next time 
                if len(jobGroups[0].jobs)==0:
                    continue 

                # Add jobgroup to the list of subscriptions to process  
                SUBSWATCH[sub] = jobGroups

        # Loop on all subscription recently found 
        for subId in SUBSWATCH.keys(): 
            if self.taskExtension(SUBSWATCH[subId]) == 0:
                logging.info("WORK succeeds to END")
                del SUBSWATCH[subId]

        logging.info('databases work ends')
    
    def algorithm(self, parameters):
        """
        Queries DB for all watched subscription, if matching split subscription and extend task information. 
        """
        logging.info("Running pool / split / extend algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.databaseWork()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise

