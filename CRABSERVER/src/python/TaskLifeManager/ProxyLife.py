import os
import copy

# logging
import logging
from logging.handlers import RotatingFileHandler

# Message service import
from MessageService.MessageService import MessageService

# module from TaskTracking component
from TaskTracking.UtilSubject import UtilSubject
from TaskTracking.TaskStateAPI import findTaskPA, getStatusUUIDEmail

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdCommon.Database.MysqlInstance import MysqlInstance

class ProxyLife:

    def __init__(self, dBlite, path, min = 3600*36):
        self.proxiespath = path
        if min < 3600*6:
            min = 3600*6
        self.minimumleft = min
        self.bossCfgDB = dBlite

        # register
        self.ms = MessageService()
        self.ms.registerAs("TaskLifeManager")

        ## preserv proxyes notified
        self.__allproxies = []

    ###############################################
    ######       SYSTEM  INTERACTIONS        ######

    def executeCommand(self, command):
        import commands
        status, outp = commands.getstatusoutput(command)
        return outp

    def checkUserProxy(self, cert=''):
        if cert != '' and os.path.exists(cert):
            proxiescmd = 'voms-proxy-info -timeleft -file ' + str(cert)
            output = self.executeCommand( proxiescmd )
            return output
        return -1

    def cleanProxy(self, proxy):
        self.executeCommand( "rm -f " + str(proxy) )

    def notified(self, proxy):
        if proxy in self.__allproxies:
            return True
        return False

    def denotify(self, proxy):
        if proxy in self.__allproxies:
            self.__allproxies.remove(proxy)

    def notify(self, proxy):
        if not self.notified(proxy):
            self.__allproxies.append(proxy)
   
    ###############################################
    ######          DB INTERACTIONS          ######

    def getListProxies(self):
        proxyList = []
        dbCfg = copy.deepcopy(dbConfig)
        dbCfg['dbType'] = 'mysql'

        Session.set_database(dbCfg)
        Session.connect(self)
        Session.start_transaction(self)
        sqlStr="select distinct(proxy) from js_taskInstance;"
        Session.execute(sqlStr)
        for tupla in Session.fetchall(self):
            proxyList.append(tupla[0]) 
        Session.close(self)

        return proxyList

    def getTaskList(self, proxy):
        dictionary = {}
        dbCfg = copy.deepcopy(dbConfig)
        dbCfg['dbType'] = 'mysql'

        Session.set_database(dbCfg)
        Session.connect(proxy)
        Session.start_transaction(proxy)
        ## get active tasks for proxy 'proxy'
        sqlStr="select taskName, eMail from js_taskInstance " + \
               "where proxy = '"+str(proxy)+"' and notificationSent < 2;"
        Session.execute(sqlStr)
        for tupla in Session.fetchall(proxy):
            if tupla[1] in dictionary.keys():
                dictionary[tupla[1]].append(tupla[0])
            else:
                dictionary.setdefault(tupla[1],[tupla[0]])
        Session.close(proxy)

        return dictionary

    def archiveBliteTask(self, taskname):
        # Blite API import
        from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
        from ProdCommon.BossLite.Common.Exceptions import TaskError, JobError

        mySession = BossLiteAPI("MySQL", self.bossCfgDB)
        taskObj = None
        try:
            taskObj = mySession.loadTaskByName( taskname )
        except TaskError, te:
            logging.error( "Problem loading the task: " + str(taskname) )
            taskObj = None
        if taskObj != None:
            try:
                logging.info( "Archiving task: " + str(taskname) )
                mySession.archive( taskObj )
            except TaskErrpr, te:
                logging.error( "Problem archiving task: " + str(taskObj['name']) )
                logging.error( str(te) )
        else:
            logging.error( "Problem archiving task: " + taskname )
        
    def getListJobName(self, taskname):
        joblist = []
        dbCfg = copy.deepcopy(dbConfig)
        dbCfg['dbType'] = 'mysql'

        Session.set_database(dbCfg)
        Session.connect(taskname)
        Session.start_transaction(taskname)
        sqlStr="select id from we_Job where owner = '"+str(taskname)+"';"
        Session.execute(sqlStr)
        for tupla in Session.fetchall(taskname):
            joblist.append(tupla[0])
        Session.close(taskname)

        return joblist

    def checkResubmit(self, jobspecid):
        from ProdAgent.WorkflowEntities import Job as wfJob

        dbCfg = copy.deepcopy(dbConfig)
        dbCfg['dbType'] = 'mysql'
        Session.set_database(dbCfg)
        Session.connect(jobspecid)
        ## get info from we_job table
        jobInfo = wfJob.get(jobspecid)
        Session.close(jobspecid)

        if int(jobInfo['retries']) >= int(jobInfo['max_retries']):
           return True
        else:
            return False

    def archiveServerTask(self, taskname):
        from ProdAgent.WorkflowEntities.JobState import doNotAllowMoreSubmissions
        jobtoclean = self.getListJobName(taskname)
        try:
            logging.info("Archiving server jobs...")
            doNotAllowMoreSubmissions(jobtoclean)
        except Exception, ex:
            logging.error( "Not achiving server job " + str(jobtoclean) )
            logging.error( "   cause: " + str(ex) )

    def notifyExpiring(self, mail, tasks, lifetime):
        mexage = "ProxyExpiring"

        payload = str(mail) + "::" + str(lifetime)

        logging.info(" Publishing ['"+ mexage +"']")
        logging.info("   payload = " + payload )
        self.ms.publish( mexage, payload)
        self.ms.commit()

    ###############################################
    ######     PUBLIC CALLABLE METHODS       ######

    def pollProxies(self):
        """
        __pollProxies__

        loops on the proxies and makes related actions
        """
        ##################
        ##### STARTS #####
        logging.info( "Start proxy's polling...." )
        if os.path.exists(self.proxiespath):

            ## get the list of proxies
            proxieslist = self.getListProxies()
            for proxyfull in proxieslist:

                ## get the remaining proxy life time
                logging.info("Checking proxy [" + str(proxyfull) + "]")
                timeleft = -1
                try:
                    timeleft = int(self.checkUserProxy(proxyfull))
                except ValueError, ex:
                    timeleft = -1
                except Exception, ex:
                    logging.info(str(ex))

                ###############################################################
                ######
                ## problem checking the proxy ##
                if timeleft < 0:
                    logging.error( "Problem on checking proxy: [" + proxyfull + "]") 
                ######
                ## if expired archive the jobs/taks ##
                elif timeleft == 0:
                    ## get the dictionary of mail-[tasklist] asscoiated to the proxy
                    tasksbymail = self.getTaskList(proxyfull)
                    logging.info( "Proxy expired [" + proxyfull + "]: " + str(timeleft) + "sec." )
                    for mail, tasks in tasksbymail.iteritems():
                        for task in tasks:
                            ## archive the jobs for blite
                            self.archiveBliteTask(task)
                            ## archive the jobs for the server
                            self.archiveServerTask(task)
                    logging.info( "Cleaning expired proxy: " + str(proxyfull) ) 
                    ## delete the proxy file
                    self.cleanProxy(proxyfull)
                ######
                ## if valid proxy, but not too long notify to renew ##
                elif timeleft <= self.minimumleft:
                    ## get the dictionary of mail-[tasklist] asscoiated to the proxy
                    tasksbymail = self.getTaskList(proxyfull)
                    for mail, tasks in tasksbymail.iteritems():
                        if not self.notified(proxyfull):
                             ## notify the expired proxy
                            logging.info( "Renew your proxy: " + str(mail) )
                            self.notifyExpiring(mail, tasks, timeleft)
                            self.notify(proxyfull)
                ######
                ## long proxy, do nothing ##
                else:
                    logging.debug(" Valid proxy [" + proxyfull + "]: " + str(timeleft) + "sec.")
                    ## reabilitate if necessary
                    self.denotify(proxyfull)
                ###############################################################

                logging.info("")
        else:
            logging.error( "Could not find proxies path [" + self.proxiespath +"]." )

        logging.info( "Proxy's polling ended." )
        ###### ENDS ######
        ##################

