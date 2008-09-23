import os
import copy

# logging
import logging
from logging.handlers import RotatingFileHandler

# Message service import
from MessageService.MessageService import MessageService

# module from TaskTracking component
from TaskTracking.TaskTrackingUtil import TaskTrackingUtil

# Blite API import
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import TaskError, JobError


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

        ## clean script
        logging.info("Cleaning old script...")
        self.delOldScript()

        ## session blite
        self.bossSession = None

    ###############################################
    ######       SYSTEM  INTERACTIONS        ######

    def executeCommand(self, command):
        import commands
        status, outp = commands.getstatusoutput(command)
        return outp

    def checkUserProxy(self, cert=''):
        if cert == '':
            logging.error( "Not existing proxy!")
            return 60*60*24*3 #3 days
        elif os.path.exists(cert):
            proxiescmd = 'voms-proxy-info -timeleft -file ' + str(cert)
            output = self.executeCommand( proxiescmd )
            return output
        return -1

    def cleanFiles(self, files):
        logging.info(files)
        self.executeCommand( "rm -f " + str(files) )
        logging.debug("Executed command: " + str("rm -f " + str(files)))

    def delOldScript(self):
        workdir = os.getenv("PRODAGENT_WORKDIR")
        dir = os.path.join( workdir, "TaskLifeManager" )
        try:
            files = os.listdir(dir)
            for file in files:
                if file == '.' or file == '..' or \
                  not (file.startswith("deleteSB_") and file.endswith("_.py")):
                    continue
                self.cleanFiles( os.path.join( dir, file ) )
        except Exception, ex:
            logging.info("Problem cleaning old script: " +str(ex))
            pass

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
        "            SeSbI.deleteRec( taskpath, proxy )\n" + \
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
           ttuid = TaskTrackingUtil("0")
           towrite.append( ttuid.getOriginalTaskName(task) )
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

        sqlStr="select distinct(proxy) from js_taskInstance;"
        tuple = self.bossSession.select(sqlStr)
        if tuple != None:
            for tupla in tuple:
                proxyList.append(tupla[0]) 

        return proxyList

    def getTaskList(self, proxy):
        dictionary = {}

        ## get active tasks for proxy 'proxy'
        sqlStr="select taskName, eMail from js_taskInstance " + \
               "where proxy = '"+str(proxy)+"' and notificationSent < 2;"
        tuple = self.bossSession.select(sqlStr)
        if tuple != None:
            for tupla in tuple:
                if tupla[1] in dictionary.keys():
                    dictionary[tupla[1]].append(tupla[0])
                else:
                    dictionary.setdefault(tupla[1],[tupla[0]])

        return dictionary

    def archiveBliteTask(self, mySession, taskname):
        logging.info( "Archiving blite task: " + str(taskname) )
        taskObj = None
        try:
            taskObj = mySession.loadTaskByName( taskname )
        except TaskError, te:
            logging.error( "Problem loading the task: " + str(taskname) )
            taskObj = None
        if taskObj != None:
            try:
                mySession.archive( taskObj )
            except TaskError, te:
                logging.error( "Problem archiving task: " + str(taskObj['name']) )
                logging.error( str(te) )

    def getListJobName(self, taskname):
        joblist = []

        sqlStr="select id from we_Job where owner = '"+str(taskname)+"';"
        tuple = self.bossSession.select(sqlStr)
        if tuple != None:
            for tupla in tuple:
                joblist.append(tupla[0])

        return joblist

    def archiveServerTask(self, taskname):
        logging.info("Archiving server jobs...")
        jobtoclean = self.getListJobName(taskname)
        if len(jobtoclean) > 0:
            try:
                sqlStr = ""
                for jobSpecId in jobtoclean:
                    sqlStr="UPDATE we_Job SET "+    \
                           "racers=max_racers+1, retries=max_retries+1 "+ \
                           "WHERE id=\""+ str(jobSpecId)+ "\";"
                self.bossSession.select(sqlStr)
            except Exception, ex:
                logging.error( "Not achiving server job " + str(jobtoclean) )
                logging.error( "   cause: " + str(ex) )
                import traceback
                logging.error(" details: \n" + str(traceback.format_exc()) )

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

        mySession = BossLiteAPI("MySQL", self.bossCfgDB)
        self.bossSession = mySession.bossLiteDB

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
                    logging.info("Problem checking proxy validity: " + str(ex))
                    timeleft = -1
                except Exception, ex:
                    logging.info("Problem checking proxy validity: " + str(ex))
                    continue
                logging.info("Proxy still valid for: " + str(proxyfull) + " seconds.")

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
                            self.archiveBliteTask(mySession, task)
                            ## archive the jobs for the server
                            self.archiveServerTask(task)
                            ## append for hand clean
                            allTasks.append(task)
                    logging.info( "Cleaning expired proxy: " + str(proxyfull) ) 
                    ## delete the proxy file
                    self.cleanFiles(proxyfull)
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
                logging.info(" ")
        else:
            logging.error( "Could not find proxies path [" + self.proxiespath +"]." )

        logging.debug(str(self.__cleanproxies))

        try:
            mySession.bossLiteDB.close()
            del mySession
        except:
            logging.info("not closed..")

        logging.info( "Proxy's polling ended." )
        ###### ENDS ######
        ##################


