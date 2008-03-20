import logging
import string
# threads
from threading import BoundedSemaphore

# DB PA
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect

from ProdCommon.Database import Session

from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job

import traceback

semWorkStatus = BoundedSemaphore(1)#for synchronisation between thread 

def openConnPA():
    """
    _openConnPA_

    opening connection with the PA DB
    """
    conn=connect(False)
    dbCur=conn.cursor()
    #logging.debug( "Conn opened\n" )
    return conn, dbCur

def closeConnPA( conn, dbCur):
    """
    _closeConnPA_

    closing connection with the PA DB
    """
    dbCur.close()
    conn.close()
    #logging.debug("Conn closed\n")


def checkNSubmit( taskName, idJob):
    """
    _checkNSubmit_

    return 0 if the job is resubmitted the max number of times (MaxRetries)
    return 1 if the job is not resubmitted the max number of times (MaxRetries)
    """
#    return 1, 1, 0
#    """
    jobMaxRetries = 0
    jobRetries = 0
    jobSpecId = taskName + "_" + str(idJob)
    jobState = ""
    try:
        Session.set_database(dbConfig)
        Session.connect(jobSpecId)
        Session.start_transaction(jobSpecId)

        jobInfo = { 'MaxRetries': 1, 'Retries': 0 }
        if JobState.isRegistered( jobSpecId ) == True:
            jobInfo = JobState.general(jobSpecId)

        Session.commit(jobSpecId)
        Session.close(jobSpecId)

        jobMaxRetries = int(jobInfo['MaxRetries'])
        jobRetries = int(jobInfo['Retries'])
        if 'State' in jobInfo:
            jobState = str(jobInfo['State'])
    except Exception, ex:
        Session.commit_all()
        Session.close_all()

        logging.error(" Error in method "  + checkNSubmit.__name__ )
        logging.error(" Exception: " + str(ex) )
        logging.error( str(traceback.format_exc()) )
        return 1, None, None

    if jobMaxRetries > jobRetries and jobState != "finished":
        return 1, jobMaxRetries, jobRetries
    return 0, jobMaxRetries, jobRetries


def insertTaskPA( taskName, status ):
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
    conn, dbCur = openConnPA()
    try:
	sqlStr="INSERT INTO js_taskInstance (id, taskName, eMail, tresholdLevel, notificationSent, endedLevel, proxy, uuid, status, work_status) "\
               "VALUES('','"+taskName+"','"+eMail+"','"+str(tresholdLevel)+"','"+str(notificationSent)+"',\
	                                           '"+str(endedLevel)+"','"+proxy+"','"+uuid+"','"+status+"', 0);"
	#logging.info(sqlStr)
        dbCur.execute("START TRANSACTION")
        #try:
        dbCur.execute(sqlStr)
        #except Exception,ex:
        #raise ProdAgentException("Error inserting the task in js_taskInstance. Taskname: '" + str(taskName) + "'.")
	dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.info("New Task ("+taskName+") inserted in the PA's DB")
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	#logging.error( "Error inserting a new task ("+ taskName +") in the PA's DB!" )
	raise


def updateNotSubmitted( taskName, eMail, tresholdLevel, proxy, uuid, status ):
    """
    _updateNotSubmitted_

    updating after message from proxyTarBall component
    """
    logging.info( "   -> updating the task table for task: " + taskName )

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        dbCur.execute("START TRANSACTION")
        if checkExistPA(conn, dbCur, taskName):
	    sqlStr='UPDATE js_taskInstance SET eMail="'+eMail+'", tresholdLevel="'+str(tresholdLevel)+'", status="'+status+'", proxy="'+proxy+'", uuid="'+uuid+'"\
		    WHERE taskName="'+taskName+'";'
	    #logging.info(sqlStr)
            try:
                rowModified=dbCur.execute(sqlStr)
            except Exception,ex:
                raise ProdAgentException("Error updating 'endedLevel' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	#logging.error( "Error updating PA DB! Method: " + updateNotSumbitted.__name__ )
	raise

def updateProxy( taskName, proxy):
    """
    """
    logging.info( "   -> updating the task table for task: " + taskName )

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        dbCur.execute("START TRANSACTION")
        if checkExistPA(conn, dbCur, taskName):
            sqlStr='UPDATE js_taskInstance SET proxy="'+proxy+'"\
                    WHERE taskName="'+taskName+'";'
            #logging.info(sqlStr)
            #try:
            dbCur.execute(sqlStr)
            #except Exception,ex:
            #raise ProdAgentException("Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        else:
            logging.error( "Error updating 'proxy' to '"+proxy+"' in js_taskInstance. TaskName: '" + str(taskName) + "': task not found.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
        dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
        #logging.error( "Error updating PA DB! Method: " + updatingEndedPA.__name__ )
        raise

def updateStatus( taskName, status ):
    """
    _updateStatus_

    updating after message from proxyTarBall component, and when the task is being submitted
    """
    logging.info( "   -> updating the task table for task: " + taskName )

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        dbCur.execute("START TRANSACTION")
        if checkExistPA(conn, dbCur, taskName):
	    sqlStr='UPDATE js_taskInstance SET status="'+status+'"\
		    WHERE taskName="'+taskName+'";'
	    #logging.info(sqlStr)
            #try:
            dbCur.execute(sqlStr)
            #except Exception,ex:
            #raise ProdAgentException("Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        else:
            logging.error( "Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "': task not found.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	#logging.error( "Error updating PA DB! Method: " + updatingEndedPA.__name__ )
	raise


def findTaskPA( taskName):
    """
    _findTaskPA_

    Query the PA database

    CREATE TABLE js_taskInstance (
       id int NOT NULL auto_increment,
       taskName varchar(255) NOT NULL default '',
       eMail varchar(255) NOT NULL default '',
       tresholdLevel int (3) UNSIGNED NOT NULL default '100',
       notificationSent int (1) NOT NULL default '0',
       endedLevel int (3) UNSIGNED NOT NULL default '0',
       primary key(id),
       unique(taskName),
       key(taskName)
    ) TYPE = InnoDB DEFAULT CHARSET=latin1;

    """

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
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
        closeConnPA( dbCur, conn )
	if len(row) > 0:
	    return row[0]
	else:
	    return None
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error quering PA DB! Method: " + findTaskPA.__name__ )
	raise


def queryMethod(strQuery, taskName):
    """
    _queryMethod_

    standard method for doing queries
    """
    #logging.info(strQuery)
    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        dbCur.execute("START TRANSACTION")
	if taskName != None:
	    if checkExistPA(conn, dbCur, taskName):
		try:
		    dbCur.execute(strQuery)
	        except Exception,ex:
	            raise ProdAgentException("Task not found in js_taskInstance.")
   	        rows = dbCur.fetchall()
	    else:
	        dbCur.execute("COMMIT")
		closeConnPA( dbCur, conn )
		return None
	else:
            try:
	        dbCur.execute(strQuery)
   	    except Exception,ex:
	        raise ProdAgentException("Task not found in js_taskInstance.")
	    rows = dbCur.fetchall()
	dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	return rows
    except:
        dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
        logging.error( "Error quering PA DB! Query: " + strQuery)
	raise

#def getAllNotFinished():
#    """
#    _getAllNotFinished_
#    """
#    queryString = "SELECT taskName,eMail,tresholdLevel,notificationSent,endedLevel,status,uuid"+\
#                  " FROM js_taskInstance"+\
#		  " WHERE status <> 'not submitted' AND ((endedLevel < 100 AND status <> 'ended') OR notificationSent < 2);"
##status <> 'killed' AND status <> 'not submitted' AND ((endedLevel < 100 AND status <> 'ended') OR notificationSent < 2);"
#    task2Check = queryMethod(queryString, None)
#    
#    return task2Check


def unlockTask(taskId):
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
 
    conn, dbCur = openConnPA()

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
        closeConnPA(dbCur, conn)

    return rowsAffected


def resetAllWorkStatus():
    """
    resetAllWorkStatus
    - input:
    - output:
    - work: set all work_status to 0
    """
    sql = "UPDATE js_taskInstance "+\
          "SET work_status=0 "\
          "WHERE work_status <> 0;"

    conn, dbCur = openConnPA()

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
        closeConnPA(dbCur, conn)


def unlockTaskByTaskName(taskName):
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

    conn, dbCur = openConnPA()

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
        closeConnPA(dbCur, conn)

    return rowsAffected


def setTaskControlled(taskId):
    """
    setTaskControlled
    see the description of the private function __setTaskControlled__
    """
    conn, dbCur = openConnPA()

    try:
        dbCur.execute("START TRANSACTION") 
        try:
            __setTaskControlled__(taskId, conn, dbCur)
            dbCur.execute("COMMIT")
        except:
            dbCur.execute("ROLLBACK")
            raise
    finally:
        closeConnPA(dbCur, conn)


def resetControlledTasks():
    """
    resetControlledTasks
    see the description of the private function __resetControlledTasks__
    """
    conn, dbCur = openConnPA()

    try:
        dbCur.execute("START TRANSACTION")
        try:
            __resetControlledTasks__(conn, dbCur)
            dbCur.execute("COMMIT")
        except:
            dbCur.execute("ROLLBACK")
            raise
    finally:
        closeConnPA(dbCur, conn)


def lockUnlockedTaskByTaskName(taskName):
    """
    lockTask
    - input: task name
    - output: record affected
      number of rows affected, 0 or 1, -1 id task dont exist.
    - work: try to lock the task if it wasnt (work_status = 0 or = 2) , if the task was just locked, return 0 of course
    """
    #take first task not locked and lock it.

    conn, dbCur = openConnPA()
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
        closeConnPA(dbCur, conn)

    return rowsAffected


def getNLockFirstNotFinished():
    """
    getNLockFirstNotFinished
    - input:
    - output:
      a row if one exist
    - work: take first task not locked and not finished is one exist and lock it
    """
    ok = 0
    row = None
    conn, dbCur = openConnPA()
    try:
        semWorkStatus.acquire()
        try:
            try:
                while ok == 0:
                    dbCur.execute("START TRANSACTION")
                    row = __getFirstNotFinished__(conn, dbCur)
                    if len(row) > 1:
                        raise Exception("len(row) > 1 unnexcepted from getFirstNotFinished(..) = " + str(len(row)))

                    if len(row) == 0:
                        ok = 1
                    else:#len(row) == 1
                        ok = 0

                    if ok == 0: #a task
                        taskId = row[0][0]
                        rowsAffected = __lockTask__(taskId, conn, dbCur)
                        if rowsAffected == 1:
                            ok = 1 #record successfully locked
                        elif rowsAffected != 0:
                            raise Exception("rowsAffected for lockTask(..) = " + str(rowsAffected))
                        else:
                            dbCur.execute("ROLLBACK")
            finally:
                semWorkStatus.release()

            dbCur.execute("COMMIT")
        except:
            dbCur.execute("ROLLBACK")
            raise
    finally:
        closeConnPA(dbCur, conn)

    return row


def __getFirstNotFinished__(conn, dbCur):
   """
   __getFirstNotFinished__
   - *private function*
   - input:
     #1 conn
     #2 dbCur
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
#         "AND ( (endedLevel < 100 AND status <> 'ended' AND status = 'not submitted' ) OR notificationSent < 2 ) "+\
#         "ORDER BY ID "+\
#         "LIMIT 1;"
   """
      "FROM js_taskInstance "+\
      "WHERE (work_status = 0) "+\
      "AND (status <> 'not submitted' AND notificationSent < 2) AND ((endedLevel < 100 AND status <> 'ended') OR notificationSent < 2)"+\
      "ORDER BY ID "\
      "LIMIT 1;"
   """

   #try:
   dbCur.execute(sql)
   row = dbCur.fetchall()
   #except:
   #logging.error( "Error quering PA DB! Query: " + sql)
   #raise

   return row


def __lockTask__(taskId, conn, dbCur):
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
          #"AND (status <> 'not submitted' AND ((endedLevel < 100 AND status <> 'ended') OR notificationSent < 2)) OR  (status = 'not submitted' AND notificationSent < 2);"
    #try:
    dbCur.execute(sql)
    rowsAffected = 1
    sql = "SELECT work_status "+\
        "FROM js_taskInstance "+\
        "WHERE id = '" + str(taskId) + "';"
    row = dbCur.fetchall()
    if len(row) == 1:
        if row[0][0] == 1:
            rowsAffected = 1

    return rowsAffected


def __getTaskWorkStatusByTaskId__(taskId, conn, dbCur):
    """
    __resetControlledTasks__
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


def __resetControlledTasks__(conn, dbCur):
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
    dbCur.execute(sql)


def __setTaskControlled__(taskId, conn, dbCur):
    """
    __resetControlledTasks__
    - *private function*
    - input: conn & dbCur
    - output: none
    - work: set to work_status = 2
    """
    sql = "UPDATE js_taskInstance "\
          "SET work_status = 2 "\
          "WHERE ID = " + str(taskId)
    dbCur.execute(sql)


def getStatusUUIDEmail( taskName ):
    """
    _getStatus_
    """

    queryString =  "SELECT status, uuid, eMail from js_taskInstance where taskName = '"+taskName+"';"
    task2Check = queryMethod(queryString,taskName)
    #logging.info("task2Check = " + str(task2Check) )
    #try:
    #    logging.info("task2Check[0] = " + str(task2Check[0]) )
    #except:
    #    pass
    if task2Check != None:
        return task2Check[0]
    return None
   
def getStatus( taskName ):
    """
    _getEmail_
    """
    queryString =  "SELECT status from js_taskInstance where taskName = '"+taskName+"';"
    task2Check = queryMethod(queryString, taskName)

    return task2Check


def checkExistPA( conn, dbCur, taskName):
    """
    _checkExistPA_

    call this ONLY from inside a TRANSACTION
    """
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

    return 0


def updatingEndedPA( taskName, newPercentage, status ):
    """
    _updatingEndedPA_
    """
    msg = ""
    msg += "   -> updating the task table for task: " + taskName
    msg += "   Setting the field endedLevel at '" + newPercentage +"'"
    msg += "   Setting the field status  at '" + status +"'"

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        dbCur.execute("START TRANSACTION")
        if checkExistPA(conn, dbCur, taskName):
	    sqlStr='UPDATE js_taskInstance SET endedLevel="'+newPercentage+'", status="'+status+'"\
		    WHERE taskName="'+taskName+'";'
            #try:
            rowModified=dbCur.execute(sqlStr)
            #except Exception,ex:
            #raise ProdAgentException("Error updating 'endedLevel' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	#logging.error( "Error updating PA DB! Method: " + updatingEndedPA.__name__ )
	raise

    return msg;


def updatingNotifiedPA( taskName, sended):
    """
    _updatingNotified_
    """
    msg = ""
    sendFlag = str(sended)
    msg += "   -> updating the task table for task: " + taskName
    msg += "   Setting the field notificationSend at '" + sendFlag +"'"

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        ## opening connection with PA's DB
        conn, dbCur = openConnPA()
	dbCur.execute("START TRANSACTION")
	if checkExistPA(conn, dbCur, taskName):
	    sqlStr='UPDATE js_taskInstance SET notificationSent="'+sendFlag+'"\
		    WHERE taskName="'+taskName+'";'
            #try:
            rowModified=dbCur.execute(sqlStr)
            #except Exception,ex:
            #    raise ProdAgentException("Error updating 'notificationSent' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
	dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	#logging.error( "Error updating PA DB! Method: " + updatingNotifiedPA.__name__ )
	raise

    return msg


def updatingStatus( taskName, status, notification ):
    """
    updatingStatus
    """
    msg = "" 
    msg += "   -> updating the task table for task: " + taskName

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        ## opening connection with PA's DB
        conn, dbCur = openConnPA()
	dbCur.execute("START TRANSACTION")
	if checkExistPA(conn, dbCur, taskName):
	    sqlStr='UPDATE js_taskInstance SET status="'+status+'", notificationSent="'+str(notification)+'"\
		    WHERE taskName="'+taskName+'";'
            #try:
            rowModified=dbCur.execute(sqlStr)
            #except Exception,ex:
            #    raise ProdAgentException("Error updating task killed in js_taskInstance. TaskName: '" + str(taskName) + "'.")
	dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	#logging.error( "Error updating PA DB! Method: " + updatingNotifiedPA.__name__ )
	raise

    return msg

def cleaningTaskPA( taskName ):
    """
    _cleaningTaskPA_

    cleaning task from PA's DB
    """

    logging.info( "   Cleaning the task table from task: " + taskName )

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        dbCur.execute("START TRANSACTION")
	if checkExistPA(conn, dbCur, taskName):
	    sqlStr='DELETE from js_taskInstance WHERE taskName="'+taskName+'";'
            try:
                rowModified=dbCur.execute(sqlStr)
            except Exception,ex:
                raise ProdAgentException("Error cleaning js_taskInstance for task: '" + str(taskName) + "'.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error updating PA DB! Method: " + cleaningTaskPA.__name__ )
	raise
 
