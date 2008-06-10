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

    def __init__(self, dBlite, path, dictSE, min = 3600*36):
        self.proxiespath = path
        if min < 3600*6:
            min = 3600*6
        self.minimumleft = min
        self.bossCfgDB = dBlite
        self.dictSE = dictSE

        # register
        self.ms = MessageService()
        self.ms.registerAs("TaskLifeManager")

        ## preserv proxyes notified for expiring
        self.__allproxies = []

        ## preserv proxy nitified for cleaning
        self.__cleanproxies = []

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

    def buildScript(self, tasklist):
        import time
        scriptname = "deleteSB_"+str(time.time())+"_.py"
        scriptpath = os.path.join(os.getcwd(), scriptname)
        taskstring = "["
        for task in tasklist:
            if task != "" and task != None:
                taskstring += "'" + os.path.join( self.dictSE['base'], task) + "',"
        taskstring += " '']"
        logging.debug(taskstring)

        pythonscript = "\n" + \
        "from ProdCommon.Storage.SEAPI.SElement import SElement\n" + \
        "from ProdCommon.Storage.SEAPI.SBinterface import *\n" + \
        "from ProdCommon.Storage.SEAPI.Exceptions import OperationException\n" + \
        "import os\n" + \
        "import sys\n\n" + \
        "print 'Initializing...'\n" + \
        "proxy = ''\n" + \
        "if len(sys.argv) == 2:\n" + \
        "    proxy = sys.argv[1]\n" + \
        "    if not os.path.exists(proxy):\n" + \
        "        raise ('Proxy not existing!')\n" + \
        "elif len(sys.argv) < 2:\n" + \
        "    raise ('No arguments passed. Pass the complete path of a valid proxy.')\n" + \
        "else:\n" + \
        "    raise ('Too many arguments passed. Pass just the complete path of a valid proxy.')\n" + \
        "storage = SElement('"+self.dictSE['SE']+"', '"+self.dictSE['prot']+"', '"+self.dictSE['port']+"')\n" + \
        "SeSbI = SBinterface(storage)\n" + \
        "tasks = "+taskstring+"\n\n" + \
        "print 'Start cleaning...\\n\'\n" + \
        "for taskpath in tasks:\n" + \
        "    try:\n" + \
        "        if taskpath != '':\n " + \
        "            SeSbI.delete( taskpath, proxy )\n" + \
        "    except OperationException, ex:\n" + \
        "        print 'Problem deleting task: [' + taskpath + ']'\n" + \
        "        for error in ex.detail:\n" + \
        "            print error\n" + \
        "        print ex.output\n" + \
        "print '\\n...done!'\n"
        logging.debug("\n\n " + pythonscript + " \n\n")
        file(scriptpath, 'w').write(pythonscript)

        return scriptpath


    def dumpToFile(self, tasklist):
        towrite = []
        for task in tasklist:
           obj = UtilSubject(self.dictSE["drop"], task, "")
           towrite.append( obj.getInfos()[0] )
        import time
        filename = "tasklist_"+str(time.time())
        filepath = os.path.join(os.getcwd(), filename)
        file(filepath, 'w').write(str(towrite))
        return filepath

    ###############################################
    ######          SELF UTILITIES           ###### 

    def notified(self, proxy):
        if proxy in self.__allproxies:
            return True
        return False

    def denotify(self, proxy):
        if self.notified(proxy):
            self.__allproxies.remove(proxy)

    def notify(self, proxy):
        if not self.notified(proxy):
            self.__allproxies.append(proxy)


    def cleanasked(self, proxy):
        if proxy in self.__cleanproxies:
            return True
        return False

    def decleaned(self, proxy):
        if self.cleanasked(proxy):
            self.__cleanproxies.remove(proxy)

    def askclean(self, proxy):
        if not self.cleanasked(proxy):
            self.__cleanproxies.append(proxy)

   
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

    def notifyExpiring(self, email, tasks, lifetime):
        taskspath = self.dumpToFile(tasks)

        mexage = "ProxyExpiring"

        payload = str(email) + "::" + str(lifetime) + "::" + str(taskspath)

        logging.info(" Publishing ['"+ mexage +"']")
        logging.info("   payload = " + payload )
        self.ms.publish( mexage, payload)
        self.ms.commit()

    def notifyToClean(self, tasklist):
        if len(tasklist) > 0:
            cmdpath = self.buildScript(tasklist)
            mexage = "TaskLifeManager::CleanStorage"
            payload = str(self.dictSE['mail']) + "::" + str(cmdpath)
            logging.info(" Publishing ['"+ mexage +"']")
            logging.info("   payload = " + payload )
            self.ms.publish( mexage, payload)
            self.ms.commit()
        else:
            pass

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
                timeleft = 0
                try:
                    timeleft = int(self.checkUserProxy(proxyfull))
                except ValueError, ex:
                    timeleft = -1
                except Exception, ex:
                    logging.info(str(ex))

                ###############################################################
                ######
                ## problem checking the proxy ##
                #if timeleft < 0:
                #    logging.error( "Problem on checking proxy: [" + proxyfull + "]") 
                ######
                ## if expired archive the jobs/taks ##
                if timeleft <= 0:
                    ## get the dictionary of mail-[tasklist] asscoiated to the proxy
                    tasksbymail = self.getTaskList(proxyfull)
                    logging.info( "Proxy expired [" + proxyfull + "]: " + str(timeleft) + "sec." )
                    allTasks = []
                    for mail, tasks in tasksbymail.iteritems():
                        for task in tasks:
                            ## archive the jobs for blite
                            self.archiveBliteTask(task)
                            ## archive the jobs for the server
                            self.archiveServerTask(task)
                            ## append for hand clean
                            allTasks.append(task)
                    logging.info( "Cleaning expired proxy: " + str(proxyfull) ) 
                    ## delete the proxy file
                    self.cleanProxy(proxyfull)
                    ## notify the admin to hand-clean
                    if not self.cleanasked(proxyfull):
                        logging.debug ("proxy: "+str(proxyfull)+" not in " +str(self.__cleanproxies))
                        self.notifyToClean(allTasks)
                        self.askclean(proxyfull)
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
                    self.decleaned(proxyfull)
                ######
                ## long proxy, do nothing ##
                else:
                    logging.debug(" Valid proxy [" + proxyfull + "]: " + str(timeleft) + "sec.")
                    ## reabilitate if necessary
                    self.denotify(proxyfull)
                    self.decleaned(proxyfull)
                ###############################################################

                logging.info("")
        else:
            logging.error( "Could not find proxies path [" + self.proxiespath +"]." )

        logging.debug(str(self.__cleanproxies))
        logging.info( "Proxy's polling ended." )
        ###### ENDS ######
        ##################


