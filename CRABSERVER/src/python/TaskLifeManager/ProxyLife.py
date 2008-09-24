import os

# logging
import logging

# Message service import
from MessageService.MessageService import MessageService

# module from TaskTracking component
from TaskTracking.TaskTrackingUtil import TaskTrackingUtil

# API modules
from TaskLifeAPI import TaskLifeAPI

# Blite API import
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import TaskError


class ProxyLife:

    def __init__(self, dBlite, path, dictSE, minim = 3600*36):
        self.proxiespath = path
        if minim < 3600*6:
            minim = 3600*6
        self.minimumleft = minim
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

    ###############################################
    ######       SYSTEM  INTERACTIONS        ######

    def executeCommand(self, command):
        import commands
        status, outp = commands.getstatusoutput(command)
        return outp

    def cleanFiles(self, files):
        self.executeCommand( "rm -f %s "% str(files) )
        logging.debug("Executed command: %s "% str("rm -f " + str(files)))

    def delOldScript(self):
        workdir = os.getenv("PRODAGENT_WORKDIR")
        dirwk = os.path.join( workdir, "TaskLifeManager" )
        try:
            files = os.listdir(dirwk)
            for filet in files:
                if filet == '.' or filet == '..' or \
                  not (filet.startswith("deleteSB_") and filet.endswith("_.py")):
                    continue
                self.cleanFiles( os.path.join( dirwk, filet ) )
        except Exception, ex:
            logging.info("Problem cleaning old script: %s"% str(ex))

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
        "from ProdCommon.Storage.SEAPI.Exceptions import *\n" + \
        "import os\n" + \
        "import sys\n\n" + \
        "print 'Initializing...'\n" + \
        "proxy = ''\n" + \
        "if len(sys.argv) == 2:\n" + \
        "    proxy = sys.argv[1]\n" + \
        "    if not os.path.exists(proxy):\n" + \
        "        print ('Proxy not existing!')\n" + \
        "        import sys\n" + \
        "        sys.exit(1)\n" + \
        "elif len(sys.argv) < 2:\n" + \
        "    print ('Pass the complete path of a valid proxy.')\n" + \
        "    import sys\n" + \
        "    sys.exit(1)\n" + \
        "else:\n" + \
        "    print ('Pass just the complete path of a valid proxy.')\n" + \
        "    import sys\n" + \
        "    sys.exit(1)\n" + \
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
    ######          PUBLISHING MESG          ######

    def notifyExpiring(self, email, tasks, lifetime):
        taskspath = self.dumpToFile(tasks)

        mexage = "ProxyExpiring"
        payload = str(email) + "::" + str(lifetime) + "::" + str(taskspath)
        logging.info(" Publishing ['%s']"% mexage)
        logging.info("   payload = %s"%  payload )
        self.ms.publish( mexage, payload)
        self.ms.commit()

    def notifyToClean(self, tasklist):
        if len(tasklist) > 0:
            cmdpath = self.buildScript(tasklist)
            mexage = "TaskLifeManager::CleanStorage"
            payload = str(self.dictSE['mail']) + "::" + str(cmdpath)
            logging.info(" Publishing ['%s']"% mexage)
            logging.info("   payload = %s"% payload )
            self.ms.publish( mexage, payload)
            self.ms.commit()

    ###############################################
    ######     PUBLIC CALLABLE METHODS       ######

    def pollProxies(self):
        """
        __pollProxies__

        loops on the proxies and makes related actions
        """
        logging.info( "Start proxy's polling...." )

        mySession = BossLiteAPI("MySQL", self.bossCfgDB)
        tlapi = TaskLifeAPI()

        if os.path.exists(self.proxiespath):

            ## get the list of proxies
            proxieslist = tlapi.getListProxies( mySession.bossLiteDB )
            for proxyfull in proxieslist:

                ## get the remaining proxy life time
                logging.info("Checking proxy [%s]"% str(proxyfull))
                timeleft = 0
                cred = None
                try:
                    cred = Credential( proxyfull )
                    timeleft = int(cred.checkValidity())
                except Exception, exc:
                    logging.info("Problem checking proxy validity: %s"% str(exc))

                ## credentialt expired ##
                if timeleft <= 0:
                    logging.info( "Credential expired [%s]: %s s"% (proxyfull, str(timeleft)) )

                    tasksbymail = tlapi.getTaskList(proxyfull, mySession.bossLiteDB)
                    allTasks = []
                    for mail, tasks in tasksbymail.iteritems():
                        for task in tasks:
                            ## archive
                            tlapi.archiveBliteTask(mySession, task)
                            tlapi.archiveServerTask(task, mySession.bossLiteDB)
                            ## append for hand clean
                            allTasks.append(task)

                    if cred != None:
                        cred.cleanMe()

                    ## if not already asked notify the admin to hand-clean
                    if not self.cleanasked(proxyfull):
                        self.notifyToClean(allTasks)
                        self.askclean(proxyfull)

                ## short credential ##
                elif timeleft <= self.minimumleft:
                    logging.info("Credential still valid for: %s s"% str(timeleft))

                    tasksbymail = tlapi.getTaskList(proxyfull, mySession.bossLiteDB)
                    for mail, tasks in tasksbymail.iteritems():
                        ## notify the expired proxy
                        if not self.notified(proxyfull):
                            logging.info( "Renew your credential: %s"% str(mail))
                            self.notifyExpiring(mail, tasks, timeleft)
                            self.notify(proxyfull)
                    self.decleaned(proxyfull)

                ## long proxy, do nothing ##
                else:
                    logging.info("Proxy still valid for: %s s"% str(timeleft))
                    ## reabilitate if necessary
                    self.denotify(proxyfull)
                    self.decleaned(proxyfull)
        else:
            logging.error( "Could not find proxies path [%s]"% self.proxiespath)

        logging.debug(str(self.__cleanproxies))

        try:
            mySession.bossLiteDB.close()
            del mySession
        except:
            logging.info("not closed..")

        logging.info( "Proxy's polling ended." )

###########
class Credential(object):
    """Credential class"""

    def __init__(self, path, minlen = 60*60*24*3):
        if not os.path.exists(path) and path != "/":
            raise Exception("Credential not found!")
        self._path = path
        self._minimumlen = minlen

    def executeCommand(self, command):
        import commands
        status, outp = commands.getstatusoutput(command)
        return outp

    def checkValidity(self):
        proxiescmd = 'voms-proxy-info -timeleft -file ' + str(self._path)
        output = self.executeCommand( proxiescmd )
        return output

    def cleanMe(self):
        logging.info(self._path)
        self.executeCommand( "rm " + str(self._path) )
        logging.debug("Executed command: " + str("rm " + str(self._path)))

