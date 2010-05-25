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

        sqlStr="SELECT distinct(proxy) FROM tt_taskInstance " + \
               "WHERE notification_sent < 2 and (proxy <> NULL OR proxy <> '') and status <> 'arrived' and status <> 'submitting';"
        tuple = dbSession.select(sqlStr)
        if tuple != None:
            for tupla in tuple:
                proxyList.append(tupla[0])

        return proxyList

    def getListTokens(self, dbSession):
        """
        _getListTokens_
        """
        proxyList = []

        sqlStr="SELECT distinct(proxy) FROM tt_taskInstance ;" 

        tuple = dbSession.select(sqlStr)
        if tuple != None:
            for tupla in tuple:
                proxyList.append(tupla[0])

        return proxyList


    def getTaskList(self, proxy, dbSession):
        dictionary = {}

        ## get active tasks for proxy 'proxy'
        sqlStr="SELECT task_name, e_mail FROM tt_taskInstance " + \
               "WHERE proxy = '"+str(proxy)+"' AND notification_sent < 2;"
        tuple = dbSession.select(sqlStr)
        if tuple != None:
            for tupla in tuple:
                if tupla[1] in dictionary.keys():
                    dictionary[tupla[1]].append(tupla[0])
                else:
                    dictionary.setdefault(tupla[1],[tupla[0]])

        return dictionary

    def getTaskEndedFrom(self, from_time, dbSession):
        taskList = [] 
        sqlStr="SELECT task_name, land_time, ended_time FROM tt_taskInstance " + \
               "WHERE ended_time > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND);"
        tuple = dbSession.select(sqlStr)
        if tuple != None:
            for tupla in tuple:
                taskList.append(tupla[0])

        return taskList

    def getTaskArrivedFrom(self, from_time, cleaned, dbSession):
        taskList = []
        cleantxt = ""
        if cleaned is True:
            cleantxt = " AND cleaned_time = 0"
        sqlStr="SELECT task_name, land_time, ended_time, notification_sent, proxy " + \
               "FROM tt_taskInstance " + \
               "WHERE land_time <= DATE_SUB(Now(),INTERVAL %s SECOND) %s ;" \
               %(str(from_time), cleantxt)
         
        tuple = dbSession.select(sqlStr)

        if tuple != None:
            for tupla in tuple:
                taskList.append( tupla )

        return taskList

    def taskCleaned( self, dbSession, taskName ):
        """
        _taskCleaned_

        set the related field with the actual time
        """
        try:
            import time
            sqlStr='UPDATE tt_taskInstance SET cleaned_time="'+time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(time.time()))+'"\
                    WHERE task_name="'+taskName+'";'
            rowModified=dbSession.modify(sqlStr)
        except Exception, exc:
            logging.error(str(exc))
            return False
        return True


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

                ### set jobs status here
                for jobbe in taskObj.jobs:
                    try:
                        mySession.getRunningInstance(jobbe)
                    except JobError, ex:
                        logging.error('Problem loading job running info')
                    jobbe.runningJob['state'] = "Cleaned"
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

