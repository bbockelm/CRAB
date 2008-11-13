import logging

# Blite API import
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.Common.Exceptions import TaskError, JobError

class TaskLifeAPI:

    def __init__(self):
        pass

    def getListProxies(self, dbSession):
        """
        _getListProxies_
        """
        proxyList = []

        sqlStr="SELECT distinct(proxy) FROM js_taskInstance " + \
               "WHERE notificationSent < 2 and (proxy <> NULL OR proxy <> '') and status <> 'arrived' and status <> 'submitting';"
        tuple = dbSession.select(sqlStr)
        if tuple != None:
            for tupla in tuple:
                proxyList.append(tupla[0])

        return proxyList

    def getTaskList(self, proxy, dbSession):
        dictionary = {}

        ## get active tasks for proxy 'proxy'
        sqlStr="SELECT taskName, eMail FROM js_taskInstance " + \
               "WHERE proxy = '"+str(proxy)+"' AND notificationSent < 2;"
        tuple = dbSession.select(sqlStr)
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

    def getListJobName(self, taskname, dbSession):
        joblist = []

        sqlStr="SELECT id FROM we_Job WHERE owner = '"+str(taskname)+"';"
        tupl = dbSession.select(sqlStr)
        if tupl != None:
            for tupla in tupl:
                joblist.append(tupla[0])

        return joblist

    def archiveServerTask(self, taskname, dbSession):
        logging.info("Archiving server jobs...")
        jobtoclean = self.getListJobName(taskname, dbSession)
        if len(jobtoclean) > 0:
            try:
                sqlStr = ""
                for jobSpecId in jobtoclean:
                    sqlStr="UPDATE we_Job SET "+    \
                           "racers=max_racers+1, retries=max_retries+1 "+ \
                           "WHERE id=\""+ str(jobSpecId)+ "\";"
                    dbSession.modify(sqlStr)
            except Exception, ex:
                logging.error( "Not achiving server job " + str(jobtoclean) )
                logging.error( "   cause: " + str(ex) )
                import traceback
                logging.error(" details: \n" + str(traceback.format_exc()) )

