# logging
import logging
from logging.handlers import RotatingFileHandler

import os
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdCommon.Database.MysqlInstance import MysqlInstance
import copy

class ProxyLife:

    def __init__(self, dBlite, path, min = 3600*36):
        self.proxiespath = path
        if min < 3600*6:
            min = 3600*6
        self.minimumleft = min
        self.bossCfgDB = dBlite

    def executeCommand(self, command):
        pin, pout = os.popen4(self, command)
        msg = pout.read()
        return msg 

    def checkUserProxy(self, cert=''):
        if cert != '' and os.path.exists(cert):
            proxiescmd = 'voms-proxy-info -timeleft -file ' + str(cert)
            output = executeCommand( proxiescmd )
            return output
        return None

    def getTaskList(self, proxy):
        dictionary = {}
        dbCfg = copy.deepcopy(dbConfig)
        dbCfg['dbType'] = 'mysql'

        Session.set_database(dbCfg)
        Session.connect(proxy)
        Session.start_transaction(proxy)

        sqlStr="select taskName, eMail from js_taskInstance where proxy = '"+str(proxy)+"';"
        Session.execute(sqlStr)

        for tupla in Session.fetchall(proxy):
            if tupla[1] in dictionary.keys():
                dictionary[tupla[1]].append(tupla[0])
            else:
                dictionary.setdefault(tupla[1],[tupla[0]])

        Session.commit(proxy)
        Session.close(proxy)
        return dictionary

    def archiveBliteTask(self, taskname):
        # Blite API import
        from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
        from ProdCommon.BossLite.Common.Exceptions import TaskError, JobError
#        selbossCfgDB = {\
#                      'dbName': 'CrabServerDB', \
#                      'user': 'root', \
#                      'passwd': 'root', \
#                      'socketFileLocation': '/home/crab/work/mysqldata/mysql.sock' \
#                    }

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
            logging.error( "Problem archiving task: " + str(taskObj['name']) )
        
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

        Session.commit(taskname)
        Session.close(taskname)

        return joblist

    def checkResubmit(self, jobspecid):
        from ProdAgent.WorkflowEntities import Job as wfJob

        dbCfg = copy.deepcopy(dbConfig)
        dbCfg['dbType'] = 'mysql'
        Session.set_database(dbCfg)
        Session.connect(jobspecid)

        jobInfo = wfJob.get(jobspecid)
        if int(jobInfo['retries']) >= int(jobInfo['max_retries']):
           return True
        else:
            return False
        Session.close(jobspecid)

    def archiveServerTask(self, taskname):
        from ProdAgent.WorkflowEntities.JobState import doNotAllowMoreSubmissions
        jobtoclean = self.getListJobName(taskname)
        try:
            logging.info("Archiving server jobs...")
            doNotAllowMoreSubmissions(jobtoclean)
        except Exception, ex:
            logging.error( "Not achiving server job " + str(jobtoclean) )
            logging.error( "   cause: " + str(ex) )

    def cleanProxy(self, proxy):
        #executeCommand( "rm -f " + str(proxy) )
        pass

    def pollProxies(self):
        logging.info( "Start proxy's polling...." )
        if os.path.exists(self.proxiespath):
            proxieslist = os.listdir(self.proxiespath)
            for proxy in proxieslist:
                proxyfull = self.proxiespath + "/" + proxy #os.path.join(self.proxiespath,proxy)
                timeleft = 0
                try:
                    timeleft = int(self.checkUserProxy(proxyfull))
                except Exception, ex:
                    timeleft = 0
                tasksbymail = self.getTaskList(proxyfull)
                if timeleft <= 0:
                    logging.info( "Proxy expired [" + proxyfull + "]: " + str(timeleft) + "sec." )
                    for mail, tasks in tasksbymail.iteritems():
                        for task in tasks:
                            logging.info( "TaskToClean: " + str(task) )
                            self.archiveBliteTask(task)
                            self.archiveServerTask(task)
                    logging.info( "Cleaning expired proxy: " + str(proxyfull) ) 
                    self.cleanProxy(proxyfull)
                elif timeleft <= self.minimumleft:
                    for mail, tasks in tasksbymail.iteritems():
                        logging.info( "Renew your proxy: " + str(mail) )
                else:
                    logging.debug(" Valid proxy [" + proxyfull + "]: " + str(timeleft) + "sec.")
                logging.info("")
        else:
            logging.error( "Could not find proxies path [" + self.proxiespath +"]." )
        logging.info( "Proxy's polling ended." )

