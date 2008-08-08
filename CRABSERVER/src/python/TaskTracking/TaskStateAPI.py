import logging
import string
# threads
from threading import BoundedSemaphore

# DB PA
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect
from ProdAgentDB.Config import defaultConfig as dbConfig

import traceback

semWorkStatus = BoundedSemaphore(1)#for synchronisation between thread 

class TaskStateAPI:

    def __init__(self):
        pass

    def openConnPA(self):
        """
        _openConnPA_

        opening connection with the PA DB
        """
        conn=connect(False)
        dbCur=conn.cursor()
        return conn, dbCur

    def closeConnPA( self, conn, dbCur):
        """
        _closeConnPA_

        closing connection with the PA DB
        """
        dbCur.close()
        conn.close()

    def checkNSubmit( self, dbSession, taskName, idJob):
        """
        _checkNSubmit_

        return 0 if the job is resubmitted the max number of times (MaxRetries)
        return 1 if the job is not resubmitted the max number of times (MaxRetries)
        """
        jobMaxRetries = 0
        jobRetries = 0
        jobSpecId = taskName + "_job" + str(idJob)
        jobState = ""
        try:
            sqlStr='SELECT max_retries,retries,status '+ \
                   'FROM we_Job WHERE '+  \
                   'id="'+jobSpecId+'";'
            tupla = dbSession.select( sqlStr )
            if tupla != None and len(tupla) > 0:
                jobMaxRetries = int(tupla[0][0])
                jobRetries = int(tupla[0][1])
                jobState = str(tupla[0][2])
            else:
                return 1, None, None, None
        except Exception, ex:
            logging.error(" Error in method "  + self.checkNSubmit.__name__ )
            logging.error(" Exception: " + str(ex) )
            logging.error( str(traceback.format_exc()) )
            return 1, None, None, None
        if jobMaxRetries > jobRetries and jobState != "finished":
            return 1, jobMaxRetries, jobRetries, jobState
        return 0, jobMaxRetries, jobRetries, jobState

    def jobStatusServer( self, dbSession, taskName, idJob):
        """
        return the status of we_Job.status
        """
        jobSpecId = taskName + "_job" + str(idJob)
        jobState = ""
        try:
            sqlStr='SELECT status '+ \
                   'FROM we_Job WHERE '+  \
                   'id="'+jobSpecId+'";'
            tupla = dbSession.select( sqlStr )
            jobState = str(tupla[0][0])
        except Exception, ex:
            logging.error(" Error in method "  + self.checkNSubmit.__name__ )
            logging.error(" Exception: " + str(ex) )
            logging.error( str(traceback.format_exc()) )
            return None

        return jobState


    def insertTaskPA( self, taskName, status ):
        """
        _insertTaskPA_
        """

        notificationSent = 0
        endedLevel = 0
        eMail = ''
        tresholdLevel = 0
        proxy = ''
        uuid = ""

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            sqlStr="INSERT INTO js_taskInstance (id, taskName, eMail, tresholdLevel, notificationSent, endedLevel, proxy, uuid, status, work_status) "\
                   "VALUES('','"+taskName+"','"+eMail+"','"+str(tresholdLevel)+"','"+str(notificationSent)+"',\
	                                           '"+str(endedLevel)+"','"+proxy+"','"+uuid+"','"+status+"', 0);"
            dbCur.execute("START TRANSACTION")
            dbCur.execute(sqlStr)
	    dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    logging.info("New Task ("+taskName+") inserted in the PA's DB")
        except:
            dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    raise

    def updateNotSubmitted( self, taskName, eMail, tresholdLevel, proxy, uuid, status ):
        """
        _updateNotSubmitted_

        updating after message from proxyTarBall component
        """
        logging.info( "   -> updating the task table for task: " + taskName )

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
	        sqlStr='UPDATE js_taskInstance SET eMail="'+eMail+'", tresholdLevel="'+str(tresholdLevel)+'", status="'+status+'", proxy="'+proxy+'", uuid="'+uuid+'"\
                        WHERE taskName="'+taskName+'";'
                try:
                    rowModified=dbCur.execute(sqlStr)
                except Exception,ex:
                    raise ProdAgentException("Error updating 'endedLevel' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
            dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
        except:
	    dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    raise

    def updateProxy( self, taskName, proxy):
        logging.info( "   -> updating the task table for task: " + taskName )

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
                sqlStr='UPDATE js_taskInstance SET proxy="'+proxy+'"\
                        WHERE taskName="'+taskName+'";'
                dbCur.execute(sqlStr)
            else:
                logging.error( "Error updating 'proxy' to '"+proxy+"' in js_taskInstance. TaskName: '" + str(taskName) + "': task not found.")
            dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
        except:
            dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
            raise

    def updateEmailThresh( self, taskName, email, threshold):
        logging.info( "   -> updating the task table for task: " + taskName )

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
                sqlStr='UPDATE js_taskInstance SET eMail="'+email+'", tresholdLevel="'+threshold+'"\
                        WHERE taskName="'+taskName+'";'
                dbCur.execute(sqlStr)
            else:
                logging.error( "Error updating 'eMail' to '"+email+"' in js_taskInstance. TaskName: '" + str(taskName) + "': task not found.")
            dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
        except:
            dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
            raise

    def updateStatus( self, taskName, status ):
        """
        _updateStatus_

        updating after message from proxyTarBall component, and when the task is being submitted
        """
        logging.info( "   -> updating the task table for task: " + taskName )

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
	        sqlStr='UPDATE js_taskInstance SET status="'+status+'"\
                        WHERE taskName="'+taskName+'";'
                dbCur.execute(sqlStr)
            else:
                logging.error( "Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "': task not found.")
            dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
        except:
            dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    raise


    def findTaskPA( self, taskName):
        """
        _findTaskPA_

        Query the PA database
        """

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            sqlStr='SELECT eMail,tresholdLevel,notificationSent,endedLevel from js_taskInstance WHERE taskName="'+taskName+'" AND endedLevel <> 100;'

            dbCur.execute("START TRANSACTION")
            try:
                dbCur.execute(sqlStr)
            except Exception,ex:
                raise ProdAgentException("Task not found in js_taskInstance. TaskName: '" + str(taskName) + "'.")
            row = dbCur.fetchall()
	    dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    if len(row) > 0:
	        return row[0]
	    else:
	        return None
        except:
	    dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    logging.error( "Error quering PA DB! Method: " + self.findTaskPA.__name__ )
	    raise


    def queryMethod( self, strQuery, taskName ):
        """
        _queryMethod_

        standard method for doing queries
        """
        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            dbCur.execute("START TRANSACTION")
            if taskName != None:
                if self.checkExistPA(conn, dbCur, taskName):
                    try:
		        dbCur.execute(strQuery)
	            except Exception,ex:
	                raise ProdAgentException("Task not found in js_taskInstance.")
   	            rows = dbCur.fetchall()
	        else:
	            dbCur.execute("COMMIT")
		    self.closeConnPA( dbCur, conn )
		    return None
            else:
                try:
	            dbCur.execute(strQuery)
   	        except Exception,ex:
	            raise ProdAgentException("Task not found in js_taskInstance.")
	        rows = dbCur.fetchall()
            dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
            return rows
        except:
            dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
            logging.error( "Error quering PA DB! Query: " + strQuery)
            raise

    def unlockTask( self, taskId ):
        """
        unlockTask
        - input:
        - output:
          number of rows affected, 0 or 1.
        - work: take first task not locked and not finished is one exist and lock it
        """
        sql = "UPDATE js_taskInstance "+\
              "SET work_status=0 "\
              "WHERE work_status = 1 AND ID = " + str(taskId) + ";"
 
        conn, dbCur = self.openConnPA()

        #TODO: execute dont return the number of affected rows, need to refactor it.
        rowsAffected = 0
        try:
            semWorkStatus.acquire()
            try:
                try:
                    dbCur.execute("START TRANSACTION")
                    rowsAffected = dbCur.execute(sql)
                    dbCur.execute("COMMIT")
                except:
                    dbCur.execute("ROLLBACK")
                    raise
            finally:
                semWorkStatus.release()
        finally:
            self.closeConnPA(dbCur, conn)

        return rowsAffected

    def resetAllWorkStatus(self):
        """
        resetAllWorkStatus
        - input:
        - output:
        - work: set all work_status to 0
        """
        sql = "UPDATE js_taskInstance "+\
              "SET work_status=0 "\
              "WHERE work_status <> 0;"

        conn, dbCur = self.openConnPA()

        try:
            semWorkStatus.acquire()
            try:
                try:
                    dbCur.execute("START TRANSACTION")
                    rowsAffected = dbCur.execute(sql)
                    dbCur.execute("COMMIT")
                except:
                    dbCur.execute("ROLLBACK")
                    raise
            finally:
                semWorkStatus.release()
        finally:
            self.closeConnPA(dbCur, conn)

    def unlockTaskByTaskName( self, taskName ):
        """
        unlockTask
        - input: taskName
        - output: record affected
          number of rows affected, 0 or 1.
        - work: unlock the task
        """
        #take first task not locked and lock it.
        sql = "UPDATE js_taskInstance "+\
              "SET work_status=0 "\
              "WHERE work_status = 1 AND TaskName = '" + taskName + "';"

        conn, dbCur = self.openConnPA()
  
        #TODO: execute dont return rowsAffected, need to be refact this methid
        rowsAffected = 0 
        try:
            semWorkStatus.acquire()
            try:
                try:
                    dbCur.execute("START TRANSACTION")
                    dbCur.execute(sql)
                    rowsAffected = 1
                    dbCur.execute("COMMIT")
                except:
                    dbCur.execute("ROLLBACK")
                    raise
            finally:
                semWorkStatus.release()
        finally:
            self.closeConnPA(dbCur, conn)

        return rowsAffected

    def setTaskControlled( self, dbSession, taskId ):
        """
        setTaskControlled
        see the description of the private function __setTaskControlled__
        """
        try:
            self.__setTaskControlled__(dbSession, taskId)
        except:
                raise

    def resetControlledTasks(self, dbSession ):
        """
        resetControlledTasks
        see the description of the private function __resetControlledTasks__
        """

        try:
            self.__resetControlledTasks__(dbSession)
        except:
            raise

    def lockUnlockedTaskByTaskName( self, taskName ):
        """
        lockTask
        - input: task name
        - output: record affected
          number of rows affected, 0 or 1, -1 id task dont exist.
        - work: try to lock the task if it wasnt (work_status = 0 or = 2) , if the task was just locked, return 0 of course
        """
        #take first task not locked and lock it.

        conn, dbCur = self.openConnPA()
        rowsAffected = 0
        try:
            semWorkStatus.acquire()
            try:
                try:
                    dbCur.execute("START TRANSACTION")
                    sql = 'SELECT COUNT(*) FROM js_taskInstance WHERE TaskName = "' + taskName + '" AND (work_status = 0 OR work_status = 2)'
                    dbCur.execute(sql)
                    rows = dbCur.fetchall()
                    count = int(rows[0][0])
                    if count > 0:
                        sql = 'UPDATE js_taskInstance '+\
                              'SET work_status = 1 '\
                             'WHERE TaskName = "' + taskName + '";'
                        dbCur.execute(sql)
                        sql = 'SELECT COUNT(*) FROM js_taskInstance WHERE TaskName = "' + taskName + '" AND work_status = 1'
                        dbCur.execute(sql)
                        rows = dbCur.fetchall()
                        count = int(rows[0][0])
                        if count == 1:
                            rowsAffected = 1
                    else:
                        #check if task still exist
                        sql = 'SELECT COUNT(*) FROM js_taskInstance WHERE TaskName = "' + taskName + '"'
                        dbCur.execute(sql)
                        rows = dbCur.fetchall()
                        count = int(rows[0][0])
                        if count == 0:
                            rowsAffected = -1
   
                    if rowsAffected == 1:
                        dbCur.execute("COMMIT")
                    else:
                        dbCur.execute("ROLLBACK")
                except:
                    dbCur.execute("ROLLBACK")
                    raise
            finally:
                semWorkStatus.release()
        finally:
            self.closeConnPA(dbCur, conn)

        return rowsAffected

    def getNLockFirstNotFinished(self, dbSession):
        """
        getNLockFirstNotFinished
        - input:
        - output:
          a row if one exist
        - work: take first task not locked and not finished is one exist and lock it
        """
        ok = 0
        row = None
        try:
            semWorkStatus.acquire()
            while ok == 0:
                row = self.__getFirstNotFinished__(dbSession)
                if len(row) > 1:
                    raise Exception("len(row) > 1 unnexcepted from getFirstNotFinished(..) = " + str(len(row)))
                if len(row) == 0:
                    ok = 1
                else:#len(row) == 1
                    ok = 0
                if ok == 0: #a task
                    taskId = row[0][0]
                    rowsAffected = self.__lockTask__(taskId, dbSession)
                    if rowsAffected == 1:
                         ok = 1 #record successfully locked
                    elif rowsAffected != 0:
                         raise Exception("rowsAffected for lockTask(..) = " + str(rowsAffected))
                    else:
                         pass
        finally:
            semWorkStatus.release()
        return row

    def __getFirstNotFinished__( self, dbSession ):
       """
       __getFirstNotFinished__
       - *private function*
       - input:
         #1 conn
       - output:
         a row if one exist
       - work: take first task not locked and not finished
       """
       sql = "SELECT ID, taskName,eMail,tresholdLevel,notificationSent,endedLevel,status,uuid "+\
             "FROM js_taskInstance "+\
             "WHERE (work_status = 0) "+\
             "AND (notificationSent < 2 ) "+\
             "ORDER BY ID "+\
             "LIMIT 1;"
       tupla = dbSession.select(sql)
       if tupla == None:
           return ()
       return tupla

    def __lockTask__( self, taskId, dbSession ):
        """
        __lockTask__
        - *private function*
        - input:
          #1 taskId
        - output:
          #1 rows affected
        - work: try to lock the task by work_status = 1
        """
        #TODO: need to refactor the recordAffected value ^^
        sql = "UPDATE js_taskInstance "\
              "SET work_status=1 "\
              "WHERE ID = " + str(taskId) + " AND work_status = 0"
        dbSession.modify(sql)
        rowsAffected = 1
        sql = "SELECT work_status "+\
              "FROM js_taskInstance "+\
              "WHERE id = '" + str(taskId) + "';"
        row = dbSession.select(sql)
        if len(row) == 1:
            if row[0][0] == 1:
                rowsAffected = 1
        return rowsAffected


    def __getTaskWorkStatusByTaskId__(self, taskId, conn, dbCur):
        """
        __getTaskWorkStatusByTaskId__
        - *private function*
        - input: taskId, conn & dbCur
        - output: none
        - work: set to work_status = 0 all the task with work_status = 2
        """
        sql = "SELECT work_status FROM js_taskInstance "\
              "WHERE ID = " + str(taskId)
        dbCur.execute(sql)
        row = dbCur.fetchall()
        if len(row) > 0:
            return int(row[0][0])
        else:
            return -1

    def __resetControlledTasks__(self, dbSession):
        """
        __resetControlledTasks__
        - *private function*
        - input: conn & dbCur
        - output: none
        - work: set to work_status = 0 all the task with work_status = 2
        """
        sql = "UPDATE js_taskInstance "\
              "SET work_status = 0 "\
              "WHERE (work_status = 2)"
        dbSession.modify(sql)

    def __setTaskControlled__(self, dbSession, taskId):
        """
        __setTaskControlled__
        - *private function*
        - input: dbSession
        - output: none
        - work: set to work_status = 2
        """
        sql = "UPDATE js_taskInstance "\
              "SET work_status = 2 "\
              "WHERE ID = " + str(taskId)
        dbSession.modify(sql)

    def getStatusUUIDEmail( self, taskName ):
        """
        _getStatus_
        """
        queryString =  "SELECT status, uuid, eMail from js_taskInstance where taskName = '"+taskName+"';"
        task2Check = self.queryMethod(queryString,taskName)
        if task2Check != None:
            return task2Check[0]
        return None
   
    def getStatus( self, taskName ):
        """
        _getEmail_
        """
        queryString =  "SELECT status from js_taskInstance where taskName = '"+taskName+"';"
        task2Check = self.queryMethod(queryString, taskName)

        return task2Check


    def checkExistPA( self, conn, dbCur, taskName, dbSession = None):
        """
        _checkExistPA_

        call this ONLY from inside a TRANSACTION
        """
        if dbSession == None:
            try:
                sqlStr='SELECT eMail from js_taskInstance WHERE taskName="'+taskName+'";'
                dbCur.execute("START TRANSACTION")
                try:
                    dbCur.execute(sqlStr)
                except Exception,ex:
                    raise ProdAgentException("Task not found in js_taskInstance. TaskName: '" + str(taskName) + "'.")
                row = dbCur.fetchall()
                dbCur.execute("COMMIT")
                if len(row) == 1:
                    return 1
                return 0
            except:
                dbCur.execute("ROLLBACK")
                logging.debug("Task not found: " +str(taskName) )
        else:
            try:
                sqlStr='SELECT eMail from js_taskInstance WHERE taskName="'+taskName+'";'
                tupla = dbSession.select(sqlStr)
                if len(tupla) == 1:
                    return 1
                return 0
            except:
                logging.debug("Task not found: " +str(taskName) )

        return 0

    def updatingEndedPA( self, dbSession, taskName, newPercentage, status ):
        """
        _updatingEndedPA_
        """
        msg = ""
        msg += "   -> updating task: " + taskName
        msg += "        setting endedLevel at '" + newPercentage +"'"
        msg += "        setting status  at    '" + status +"'"
   
        try:
            if self.checkExistPA(None, None, taskName, dbSession):
                sqlStr='UPDATE js_taskInstance SET endedLevel="'+newPercentage+'", status="'+status+'"\
                        WHERE taskName="'+taskName+'";'
                rowModified=dbSession.modify(sqlStr)
        except:
	    raise

        return msg;

    def updatingNotifiedPA( self, taskName, sended):
        """
        _updatingNotified_
        """
        msg = ""
        sendFlag = str(sended)
        msg += "   -> updating the task table for task: " + taskName
        msg += "      setting the field notificationSend at '" + sendFlag +"'"

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            ## opening connection with PA's DB
            conn, dbCur = self.openConnPA()
	    dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
	         sqlStr='UPDATE js_taskInstance SET notificationSent="'+sendFlag+'"\
		         WHERE taskName="'+taskName+'";'
                 rowModified=dbCur.execute(sqlStr)
            dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
        except:
            dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    raise

        return msg


    def updatingStatus( self, taskName, status, notification ):
        """
        updatingStatus
        """
        msg = "" 
        msg += "   -> updating the task table for task: " + taskName

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
	    dbCur.execute("START TRANSACTION")
	    if self.checkExistPA(conn, dbCur, taskName):
	         sqlStr='UPDATE js_taskInstance SET status="'+status+'", notificationSent="'+str(notification)+'"\
		         WHERE taskName="'+taskName+'";'
                 rowModified=dbCur.execute(sqlStr)
	    dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
        except:
            dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    raise

        return msg

    def cleaningTaskPA( self, taskName ):
        """
        _cleaningTaskPA_

        cleaning task from PA's DB
        """

        logging.info( "   Cleaning the task table from task: " + taskName )

        ## opening connection with PA's DB
        conn, dbCur = self.openConnPA()
        try:
            dbCur.execute("START TRANSACTION")
            if self.checkExistPA(conn, dbCur, taskName):
	        sqlStr='DELETE from js_taskInstance WHERE taskName="'+taskName+'";'
                try:
                    rowModified=dbCur.execute(sqlStr)
                except Exception,ex:
                    raise ProdAgentException("Error cleaning js_taskInstance for task: '" + str(taskName) + "'.")
            dbCur.execute("COMMIT")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
        except:
            dbCur.execute("ROLLBACK")
            ## closing connection with PA's DB
            self.closeConnPA( dbCur, conn )
	    logging.error( "Error updating PA DB! Method: " + self.cleaningTaskPA.__name__ )
	    raise
 
