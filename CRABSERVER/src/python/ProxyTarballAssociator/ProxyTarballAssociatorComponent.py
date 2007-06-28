#!/usr/bin/env python
"""
_ProxyTarballAssociatorComponent_

"""

__version__ = "$Revision: 1.8 $"
__revision__ = "$Id: ProxyTarballAssociatorComponent.py,v 1.8 2007/06/26 15:35:45 corvo Exp $"

import os
import socket
import pickle
import logging
import time
from logging.handlers import RotatingFileHandler
import popen2
import commands
import re
from MessageService.MessageService import MessageService

class ProxyTarballAssociatorComponent:
    """
    _ProxyTarballAssociatorComponent_

    """
    def __init__(self, **args):
        self.args = {}
        self.args['Logfile'] = None
        self.args['dropBoxPath'] = None
        self.args['ProxiesDir'] = None
        self.args['bossClads'] = None
        self.args['crabMaxRetry'] = 5
        self.args.update(args)
           
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
        #  //
        # // Log Handler is a rotating file that rolls over when the
        #//  file hits 1MB size, 3 most recent files are kept
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        #  //
        # // Set up formatting for the logger and set the 
        #//  logging level to info level
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        
        logging.info("ProxyTarballAssociatorComponent Started...")

        self.association = []
        self.files = []
        self.proxyDir = str(self.args['ProxiesDir'])
        self.dropBoxPath = str( self.args['dropBoxPath'] )
        self.CrabworkDir = ''
        self.bossClads = str(self.args['bossClads'])
        self.maxRetry = int(self.args['crabMaxRetry'])

        try:
            path = self.dropBoxPath+"/proxytar.set"
            logging.info("Opening Proxy Tar Status "+path)
            f = open(path, 'r')
            self.files = pickle.load(f)
            f.close()
        except IOError, ex:
            logging.info("Failed to open proxytar.set. Building a new status item")
            self.files = []

    def __call__(self, event, payload):
        """
        _operator()_

        Define response to events
        """

        logging.debug("Event: %s %s" % (event, payload))

        if event == "DropBoxGuardianComponent:NewProxy":
            self.associated = self.matchingProxy()
            return 

        if event == "DropBoxGuardianComponent:NewFile":
            try:
                # Unpack the project
                os.chdir(self.dropBoxPath)
                uniqDir = "" + payload
                payload = payload + '.tgz'

                # Build the unique dir and copy the payload
                cmd = "tar -z --list --file=%s"%payload
                #outp, inp, errp = popen2.popen3(cmd)
                #errBuf = errp.readlines()
                #outBuf = outp.readlines()
                #inp.close()
                #errp.close()
                #outp.close()

		# Fabio -- new Fix
                retCode, outBuf = commands.getstatusoutput(cmd)

                if retCode != 0: #str(errBuf) != '[]':
                     logging.info("Warning: Unable to open " + str(payload))# + '\nThe file will be removed.')
		     logging.info("RetCode =%d"%retCode)
                     #os.system('rm -f '+ payload)
		     os.system('mv %s %s.bad'%(payload, payload) )
                     raise Exception
		     
                prjName = outBuf.split('\n')[0] #outBuf[0][:-1]
                cmd = 'tar xvzf '+ payload + ' > /dev/null; rm -f '+ payload +" && " 
                cmd = cmd + 'mv '+prjName+ ' '+uniqDir
                logging.debug(cmd)
                os.system(cmd)
		
                #Matteo Add:send a message to Task Tracking for Extracted Tar
                #extract UUID
                startU=5+len(prjName)
                uuid = uniqDir[startU:]
                logging.info("PAYLOAD ="+payload+" prjName = "+prjName+" uniqDir = "+uniqDir+" UUID = "+uuid)

                # get the project name
                os.chdir(self.dropBoxPath)
           
                # Associate project to proxy
                self.CrabworkDir = self.dropBoxPath+'/'+uniqDir 
                self.associated = self.matchingProxy(uuid, self.CrabworkDir)
                logging.debug(""+str(self.associated) )
                
                # materialize the component status to preserve it from crashes
                f = open( self.dropBoxPath+"/proxytar_tmp", 'w')
                pickle.dump(self.files, f)
                f.close()
		os.rename(self.dropBoxPath+"/proxytar_tmp",  self.dropBoxPath+"/proxytar.set")
            except Exception, e:
                logging.info("Warning: Unable to associate " + str(self.CrabworkDir) +"\n"+str(e))
                self.ms.publish("ProxyTarballAssociatorComponent:UnableToManage", uniqDir)
                self.ms.commit()  
            return

        # Logging events 
	if event == "ProxyTarballAssociatorComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        if event == "ProxyTarballAssociatorComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return

        return 
        
    def startComponent(self):
        """
        _startComponent_

        Start up the component
        """

        # create message service
        self.ms = MessageService()
                                                                
        # register
        self.ms.registerAs("ProxyTarballAssociatorComponent")
                                                                                
        # subscribe to messages
        self.ms.subscribeTo("DropBoxGuardianComponent:NewFile")
        self.ms.subscribeTo("DropBoxGuardianComponent:NewProxy") # add-in to manage proxies in very late
        
        self.ms.subscribeTo("ProxyTarballAssociatorComponent:StartDebug")
        self.ms.subscribeTo("ProxyTarballAssociatorComponent:EndDebug")

        while True :
            # Events listening and translation
            self.associated = ""
            type, payload = self.ms.get()
                
            logging.debug("ProxyTarballAssociatorComponent: %s %s" % ( type, payload))
            self.ms.commit() # anticipated to avoid pending items to block the startup # Fabio 
            self.__call__(type, payload)

            for a in self.associated:
                 self.ms.publish("ProxyTarballAssociatorComponent:CrabWork", a)
                 self.ms.commit()

            # Prepared project ticket
            logging.info(self.associated)# for Debug only #Fabio

# ## Matching Procedure
    def matchingProxy(self, uuid=None, taskDir = None):
        pProxy = []
        prePayload = ""
        # cache the pending tasks list
        if taskDir != None:
            # save payload part and get the actual taskDir
            prePayload = taskDir
            self.files.append(taskDir) 

        # update the local knowledge on proxies
        pProxy = self.getProxyList(self.proxyDir)
        ## MATT - start
        pCache = []
        for p in pProxy:
            pCacheT = os.popen3('openssl x509 -in '+p+' -subject -noout')[1].readlines()
            if len(pCacheT) > 0:
                pCache.append( pCacheT[0] )
        ## MATT - end

        # get the subject informations from the file in taskDir/share
        subjFileName = 'userSubj'         
        matched = []
        dummyFileList = []
        dummyFileList += self.files
        logging.debug("FileList:"+str(dummyFileList))
        for pendingTaskDir in self.files:
            subject = ""
            try: 
                f = open(pendingTaskDir+"/share/"+subjFileName, 'r')
                subject = f.readline()
                f.close()
            except Exception, e:
                logging.info("Warning: Unable to open " + str(pendingTaskDir+"/share/"+subjFileName))
                logging.info("   the project file could be corrupted. It won't be processed.")
		logging.info(e)
                dummyFileList.remove(pendingTaskDir)
                self.files = dummyFileList
                # send also a message to task tracking??? # Fabio
                break

            for s in pCache:
                 logging.debug("pCache: " + s)
                 if  subject.strip() not in s.strip():
                     # if there is no a valid proxy yet then skip the task for this iteration
                     continue
                 
                 i = pCache.index(s)
                 logging.debug("pProxy: " + pProxy[i])
                 cmd = 'ln -s '+ str(pProxy[i]) +' '+ pendingTaskDir+'/share/userProxy; '
                 cmd = cmd + 'chmod 600 '+ str(pProxy[i]) + ';'
                 cmd = cmd + 'cp ' + pendingTaskDir+'/share/cmssw.xml ' + pendingTaskDir+'/share/cmssw.xml.orig'
                 os.system(cmd)

                 # crab.cfg server personalizations
                 self.cfg4server(pendingTaskDir) # substitutes the cfg4server script # Fabio
                 self.xml4server(pendingTaskDir, pProxy[i]) ## MATTY: added proxy field

                 #Matteo Add: send a massage to Task Tracking for modified configuration for server
                 taskName = str(pendingTaskDir.split('/')[-1])
                 # self.ms.publish("ProxyTarballAssociatorComponent:ModCfg", taskName)
                 # self.ms.commit()

                 logging.info("Proxy->Project Association: "+pProxy[i]+"->"+pendingTaskDir)

                 # build notification
                 useProxy = pendingTaskDir+'/share/userProxy'
		 cwMsg = str(useProxy)+':'+pendingTaskDir+':'+prePayload+':'+str(self.maxRetry)
                 matched.append(cwMsg) 
                 logging.debug(matched[0])
                 try:  
                     dummyFileList.remove(pendingTaskDir)
                 except:
                     pass
              
                 #Matteo Add: send a massage to Task Tracking for Proxy->Project Association
		 if uuid != None:
                      proxyName = str(pProxy[i].split('/')[-1])
                      ttMsg = str(uuid+':'+taskName+':'+proxyName)
                      self.ms.publish("ProxyTarballAssociatorComponent:WorkDone", ttMsg)
                      self.ms.commit()

                 # subjects loop
                 break
            # pending Task loop
            pass
        # list for still wayting projects 
        self.files = dummyFileList
        return matched

    def getProxyList(self, pDir):
       pfList = []
       # old gridsite version
       for root, dirs, files in os.walk(self.proxyDir):
            for f in files:
                if f == 'userproxy.pem':
                    pfList.append(os.path.join(root, f))

       # new gridsite version
       if len(pfList)==0:
            pfList = [ self.proxyDir+'/'+q  for q in os.listdir(self.proxyDir) if q[0]!="." ]

       return pfList

    def xml4server(self, taskDir, proxy):  ## MATTY: added proxy par
         # new names convension cfg4server
         lines = []
         f = open(taskDir+'/share/cmssw.xml', 'r')
         lines = f.readlines()
         f.close()
         buf = ""
         buf = buf.join(lines)
         idInit = buf.find("task name=")+ len("task name=") + 1 # position on the first char of the name
         idEnd = buf.find("\"", idInit)
         name = buf[idInit:idEnd]
         
         for l in xrange(len(lines)):
              lines[l]=lines[l].replace(name, taskDir.split('/')[-1] )

         ## MATTY: *start* changing proxy field
         try:
              proxyInit = buf.find("task_info=")+ len("task_info=") + 1
              proxyEnd = buf.find("\"", proxyInit)
              name = buf[proxyInit:proxyEnd]
              for ll in xrange(len(lines)):
                   lines[ll]=lines[ll].replace(name, proxy )
         except Exception, ex:
              logging.error("Exception changing proxy on cmssw.xml: ["+str(ex)+"]")
              logging.error("If you are using an old versione of SchedulerEdg.py in your client forget this problem!")
         ## MATTY: *end* changing proxy field

         f = open(taskDir+'/share/cmssw.xml', 'w')
         f.writelines(lines)
         f.close()

    def cfg4server(self, taskDir):
       os.chdir(taskDir+'/share')
       os.system('cp crab.cfg crab.orig_cfg')
       f = open('crab.cfg','r')
       cfgData = []
       cfgData = f.readlines()
       f.close()
       foundItems = ['server_mode', 'dont_check_proxy', 'use_central_bossDB', 'boss_clads']
       idxCrab = 0
       idxUser = 0
       for i in xrange(len(cfgData)):
           l = cfgData[i]
           if l[0] != '#':
              if '[CRAB]' in l:
                  idxCrab = i+1
              elif '[USER]' in l:
                  idxUser = i+1
              elif 'server_mode' in l: 
                  cfgData[i] = 'server_mode = 9999\n'
                  if 'server_mode' in foundItems: 
                       foundItems.remove('server_mode') 
              elif 'dont_check_proxy' in l:
                  cfgData[i] = 'dont_check_proxy = 1\n' 
                  if 'dont_check_proxy' in foundItems: 
                       foundItems.remove('dont_check_proxy')
              elif 'use_central_bossDB' in l:
                  cfgData[i] = 'use_central_bossDB = 2\n'
                  if 'use_central_bossDB' in foundItems: 
                       foundItems.remove('use_central_bossDB')
              elif 'boss_clads' in l:
                  cfgData[i] = 'boss_clads = '+ self.bossClads + '\n'
                  if 'boss_clads' in foundItems: 
                       foundItems.remove('boss_clads')
           pass
       pass 
       # add missing entries (if any)
       for i in foundItems:
             if i == 'server_mode':
                  cfgData.insert(idxCrab, '\nserver_mode = 9999\n')
             if i == 'dont_check_proxy':
                  cfgData.insert(idxUser, '\ndont_check_proxy = 1\n')
             if i == 'use_central_bossDB':
                  cfgData.insert(idxUser, '\nuse_central_bossDB = 2\n')
             if i == 'boss_clads':
                  cfgData.insert(idxUser, '\nboss_clads = '+ self.bossClads+'\n')
       pass
       f = open('crab.cfg','w')
       f.writelines(cfgData)
       f.close()

       # Matteo add
       for sched in [cl for cl in os.listdir('.') if cl.split('.')[-1]=='clad' and 'sched_param' in cl]:
            os.system( 'cp %s %s.orig'%(sched,sched) )
            f = open(sched,'r')
            readedData = f.readlines()
            f.close()
            for m in xrange(len(readedData)):
                 n = readedData[m]
                 if 'RBconfigVO' in n:
                      readedData[m] = 'RBconfigVO = "'+ self.dropBoxPath +'/' + n.split('/')[-1]
                 elif 'RBconfig' in n:
                      readedData[m] = 'RBconfig = "'+ self.dropBoxPath +'/' + n.split('/')[-1]
                 elif 'WMSconfig' in n:
                      readedData[m] = 'WMSconfig = '+ self.dropBoxPath +'/' + n.split('/')[-1]
                 pass
            pass
            f = open(sched,'w')
            f.writelines(readedData)
            f.close()
       pass



