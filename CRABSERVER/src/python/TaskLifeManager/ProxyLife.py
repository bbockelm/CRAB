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

    def __init__(self, dBlite, path, dictSE, additionalParams={}, minim = 3600*36):
        self.proxiespath = path
        if minim < 3600*6:
            minim = 3600*6

        self.minimumleft = minim
        self.bossCfgDB = dBlite
        self.dictSE = dictSE

        # stuff needed for glExec renewal technicalities
        self.useGlExecDelegation = additionalParams.get("glExecDelegation", 'false')=='true' 

        # register
        self.ms = MessageService()
        self.ms.registerAs("TaskLifeManager")

        # preserv proxyes notified for expiring
        self.__allproxies = []

        ## preserv proxy nitified for cleaning
        self.__cleanproxies = []

        ## clean script
        #logging.info("Cleaning old script...")
        #self.delOldScript()

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
        logstring = "["
        for task in tasklist:
            if task != "" and task != None:
                taskstring += "'" + os.path.join( self.dictSE['base'], task) + "',"
                logstring += "'" + os.path.join( self.dictSE['drop'], task) + "_spec',"
        taskstring += " '']"
        logstring += " '']"
        logging.debug(taskstring)

        pythonscript = "\n" + \
        "from ProdCommon.Storage.SEAPI.SElement import SElement\n" + \
        "from ProdCommon.Storage.SEAPI.SBinterface import *\n" + \
        "from ProdCommon.Storage.SEAPI.Exceptions import *\n" + \
        "import os\n" + \
        "import sys\n\n" + \
        "print 'Initializing...'\n"+\
        "proxy = None\n"+\
        "storage = ''\n"+\
        "opt = ''\n"+\
        'msg  = """ \n'+\
        "Usage:\n"+\
        "python delete_* local\n"+\
        "python delete_* remote <full path of the proxy>\n"+\
        '"""\n'+\
        "if len(sys.argv) < 2 or sys.argv[1] not in ['local','remote']:\n"+\
        "    print msg\n"+\
        "    sys.exit(1)\n"+\
        "elif sys.argv[1]=='local':\n"+\
        "    storage = SElement('','local')\n"+\
        "    opt='-rf'\n"+\
        "elif sys.argv[1]=='remote':\n"+\
        "    if len(sys.argv) != 2:\n"+\
        "        print ('Pass just the complete path of a valid proxy.')\n"+\
        "        print msg\n"+\
        "        sys.exit(1)\n"+\
        "    else:\n"+\
        "        proxy = sys.argv[2]\n"+\
        "        if not os.path.exists(proxy): \n"+\
        "            print ('Proxy not existing!')\n"+\
        "            sys.exit(1)\n"+\
        "        else:\n"+\
        "             storage = SElement('"+self.dictSE['SE']+"', '"+self.dictSE['prot']+"', '"+self.dictSE['port']+"')\n" + \
        "else:\n"+\
        "    print msg\n"+\
        "storage_logs = SElement('','local')\n"+\
        "SeSbI_logs = SBinterface(storage_logs)\n" + \
        "SeSbI = SBinterface(storage)\n" + \
        "tasks = "+taskstring+"\n\n" + \
        "logs = "+logstring+"\n\n" + \
        "print 'Start cleaning...\\n\'\n" + \
        "for i in range(len(tasks)):\n" + \
        "   if tasks[i] != '':\n" + \
        "        try:\n" + \
        "            SeSbI.deleteRec( tasks[i], proxy, opt=opt )\n" + \
        "        except OperationException, ex:\n" + \
        "            print 'Problem deleting task: [' + tasks[i] + ']'\n" + \
        "            for error in ex.detail:\n" + \
        "                print error\n" + \
        "            print ex.output\n" + \
        "   if logs[i] != '':\n" + \
        "        try:\n" + \
        "            SeSbI_logs.deleteRec( logs[i], proxy, opt=opt )\n" + \
        "        except OperationException, ex:\n" + \
        "            print 'Problem deleting task: [' + logs[i] + ']'\n" + \
        "            for error in ex.detail:\n" + \
        "                print error\n" + \
        "            print ex.output\n" + \
        "print '\\n...done'\n"
        logging.debug("\n\n " + pythonscript + " \n\n")
        file(scriptpath, 'w').write(pythonscript)

        return scriptpath


    def dumpToFile(self, tasklist):
        from TaskTracking.TaskStateAPI import TaskStateAPI
        towrite = []
        ttdb = TaskStateAPI()
        for task in tasklist:
            uuid = ttdb.getStatusUUIDEmail( task )[1]
            ttuid = TaskTrackingUtil()
            towrite.append( ttuid.getOriginalTaskName(task, uuid) )
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

    def pollProxies(self, credConfig):
        """
        __pollProxies__

        loops on the proxies and makes related actions
        """
        logging.info( "Start proxy's polling...." )

        from ProdCommon.Credential.CredentialAPI import CredentialAPI
        CredAPI = CredentialAPI( credConfig )

        CredAPI.credObj.myproxyServer = '$MYPROXY_SERVER'
 
        mySession = BossLiteAPI("MySQL", self.bossCfgDB)
        tlapi = TaskLifeAPI()

        if os.path.exists(self.proxiespath):

            ## get the list of proxies
            if credConfig['credential'] == 'Token':
                proxieslist=[]
                proxiesTemp = tlapi.getListTokens( mySession.bossLiteDB )
                for proxy in proxiesTemp:
                    if os.path.exists(proxy): proxieslist.append(proxy)
            else:
                proxieslist = tlapi.getListProxies( mySession.bossLiteDB )
 
            for proxyfull in proxieslist:

                ## get the remaining proxy life time
                logging.info("Checking proxy [%s]"% str(proxyfull))
                timeleft = 0
                try:
                    timeleft = CredAPI.getTimeLeft( proxyfull )
                except Exception, exc:
                    logging.info("Problem checking proxy validity: %s"% str(exc))
                    import traceback
                    logging.info( str(traceback.format_exc()) )
                    continue

                ## credential expired ##
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

                    try:
                        logging.info("Destroying proxy %s" %proxyfull) 
                        CredAPI.destroyCredential( proxyfull )
                    except Exception, ex:
                        logging.error("Problem '%s' destroying credential '%s'."%(str(ex),str(proxyfull)))
                    
                    ## if not already asked notify the admin to hand-clean
                    if not self.cleanasked(proxyfull):
                        self.notifyToClean(allTasks)
                        self.askclean(proxyfull)

                ## short credential ##
                elif timeleft <= self.minimumleft:
                    logging.info("Credential still valid for: %s s"% str(timeleft))

                    ## proxy renewal through myproxy delegation ##
                    delegatedtimeleft = 0
                    if credConfig['credential'] == 'Proxy': 
                        logging.info("Trying to renew proxy [%s]"% str(proxyfull))

                        if self.useGlExecDelegation == True:
                            # glExec renewal specific parts
                            # TODO
                            # Sanjay fix here
                            # change the proxy ownership so that CrabServer can renew it
                            pass

                        try:

                            CredAPI.renewalMyProxy(proxyfull)
                            delegatedtimeleft = CredAPI.getTimeLeft(proxyfull)
                            logging.info("Renewed credential still valid for: %s s"% str(delegatedtimeleft))

                        except Exception, exc:

                            logging.info("Problem renewing proxy : %s"% str(exc))
                            import traceback
                            logging.info( str(traceback.format_exc()) )
                            delegatedtimeleft = 0

                        if self.useGlExecDelegation == True:
                            # glExec renewal specific parts
                            # TODO
                            # Sanjay fix here
                            # set again the proxy ownership for glExec
                            pass

                    if credConfig['credential'] == 'Token':
                        logging.info("Trying to renew Token [%s]"% str(proxyfull))

              	        try:

                            CredAPI.renewalMyToken(proxyfull)
                            delegatedtimeleft = CredAPI.getTimeLeft(proxyfull)
                            logging.info("Renewed credential still valid for: %s s"% str(delegatedtimeleft))

                        except Exception, exc:

                            logging.info("Problem renewing Token : %s"% str(exc))
                            import traceback
                            logging.info( str(traceback.format_exc()) )
                            delegatedtimeleft = 0

                    if delegatedtimeleft <= timeleft: 
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

