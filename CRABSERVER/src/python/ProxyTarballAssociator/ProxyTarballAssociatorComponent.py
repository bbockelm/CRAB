#!/usr/bin/env python
"""
_ProxyTarballAssociatorComponent_

"""

__version__ = "$Revision: 1.0 $"
__revision__ = "$Id: ProxyTarballAssociatorComponent.py,v 1.0 2006/11/20 17:50:00 farinafa Exp $"

import os
import socket
import pickle
import logging
import time
from logging.handlers import RotatingFileHandler
import popen2
import re

from MessageService.MessageService import MessageService

# RC-Component: 12 Feb 2007 # Fabio

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
                uniqDir = payload.split('.')[0]
                # Build the unique dir and copy the payload 
                outp, inp =popen2.popen4("tar -z --list --file=%s | head -n 1"%payload)
                prjName = outp.readlines()[0][:-1]
                outp.close()
                inp.close()
                cmd = 'tar xvzf '+ payload + ' > /dev/null; rm -f '+ payload +"; " 
                cmd = cmd + 'mv '+prjName+ ' '+uniqDir 
                os.system(cmd)
                # get the project name
                os.chdir(self.dropBoxPath)
           
                # Associate project to proxy
                self.CrabworkDir = self.dropBoxPath+'/'+uniqDir # +':'+projDir
                self.associated = self.matchingProxy(self.CrabworkDir)
                logging.debug(""+str(self.associated) )
            except Exception, e:
                logging.info("Warning: Unable to associate " + str(self.CrabworkDir) + str(e))
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

            # Prepared project ticket
            logging.info(self.associated)# for Debug only #Fabio

            for a in self.associated:
                 self.ms.publish("ProxyTarballAssociatorComponent:CrabWork", a)
                 self.ms.commit()

# ## Matching Procedure
    def matchingProxy(self, taskDir = None):
        pProxy = []
        # cache the pending tasks list
        if taskDir != None:
            # save payload part and get the actual taskDir
            prePayload = taskDir
            self.files.append(taskDir) 

        # update the local knowledge on proxies
        pProxy = self.getProxyList(self.proxyDir)
#        pCache = [ os.popen3('openssl x509 -in '+p+' -subject -noout')[1].readlines()[0] for p in pProxy ]
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
            except:
                logging.info("Warning: Unable to open " + str(pendingTaskDir+"/share/"+subjFileName))
                break

            for s in pCache:
                 logging.debug("pCache: " + s)
                 #if not re.compile(subject.strip()).search(s.strip()):
                 if  subject.strip() not in s.strip():
                     # if there is no a valid proxy yet then skip the task for this iteration
                     continue
                 
                 i = pCache.index(s)
                 logging.debug("pProxy: " + pProxy[i])
                 # if subject and re.compile(subject.strip()).search(pCache[i].strip()):
                 cmd = 'ln -s '+ pProxy[i] +'_tmp '+ pendingTaskDir+'/share/userProxy; '
                 cmd = cmd + 'cp '+pProxy[i]+' '+pProxy[i]+'_tmp;'
                 proxy = pProxy[i]+'_tmp'
                 cmd = cmd + 'chmod 600 '+ proxy + ';'
                 cmd = cmd + 'cfg4server.sh '+pendingTaskDir+'/share '+self.bossClads+'; '
                 cmd = cmd + 'cp ' + pendingTaskDir+'/share/cmssw.xml ' + pendingTaskDir+'/share/cmssw.xml.orig'
                 os.system(cmd)

                 # new names convension cfg4server
                 lines = []
                 f = open(pendingTaskDir+'/share/cmssw.xml', 'r')
                 lines = f.readlines()
                 f.close()
                 buf = ""
                 buf = buf.join(lines)
                 idInit = buf.find("task name=")+ len("task name=") + 1 # position on the first char of the name
                 idEnd = buf.find("\"", idInit)
                 name = buf[idInit: idEnd]
                 for l in xrange(len(lines)):
                     lines[l]=lines[l].replace(name, pendingTaskDir.split('/')[-1] )
                 f = open(pendingTaskDir+'/share/cmssw.xml', 'w')
                 f.writelines(lines)
                 f.close()

                 logging.info("Proxy->Project Association: "+pProxy[i]+"->"+pendingTaskDir)
                 # build notification
                 matched.append(proxy+':'+pendingTaskDir+':'+prePayload) 
                 logging.debug(matched[0])
                 try:  
                     dummyFileList.remove(pendingTaskDir)
                 except:
                     pass
                 # subjects loop
                 break
            # pending Task loop
            pass
        # list for stll wayting projects 
        self.files = dummyFileList
        return matched


    def getProxyList(self, pDir):
       pfList = []
       # old gridsite version
       for root, dirs, files in os.walk(self.proxyDir):
            for f in files:
                if f == 'usercert.pem':
                    pfList.append(os.path.join(root, f))

       # new gridsite version
       if len(pfList)==0:
            pfList = [ self.proxyDir+'/'+q  for q in os.listdir(self.proxyDir) ]

       return pfList

