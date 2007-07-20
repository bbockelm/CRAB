import logging
import string

# DB PA
from JobState.Database.Api import JobStateInfoAPIMySQL
from JobState.Database.Api.RacerException import RacerException
from JobState.Database.Api.RetryException import RetryException
from JobState.Database.Api.RunException import RunException
from JobState.Database.Api.SubmitException import SubmitException
from JobState.Database.Api.TransitionException import TransitionException
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect


def openConnPA():
    """
    _openConnPA_

    opening connection with the PA DB
    """
    conn=connect(False)
    dbCur=conn.cursor()
    logging.debug( "Conn opened\n" )
    return conn, dbCur

def closeConnPA( conn, dbCur):
    """
    _closeConnPA_

    closing connection with the PA DB
    """
    dbCur.close()
    conn.close()
    logging.debug("Conn closed\n")

def checkNSubmit( taskName, idJob):
    """
    _checkNSubmit_

    return 0 if the job is resubmitted the max number of times (MaxRetries)
    return 1 if the job is not resubmitted the max number of times (MaxRetries)
    """

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        sqlStr='SELECT MaxRetries, Retries from js_JobSpec where JobSpecID="'+taskName+'_'+idJob+'" ;'

        dbCur.execute("START TRANSACTION")
        try:
            dbCur.execute(sqlStr)
        except Exception,ex:
            raise ProdAgentException("Error checking the jobs in js_JobSpec. Taskname: '" + str(taskName) +"' - jobId: '" + str(idJob) + "'." )
	rows = dbCur.fetchall()
	dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	if len(rows) == 1:
	    if rows[0][0] <= rows[0][1]:
		return 0, None, None
        #rows[0][0], rows[0][1]
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error quering PA DB! Method: " + checkNSubmit.__name__ )
	raise
    return 1, rows[0][0], rows[0][1]

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
	sqlStr="INSERT INTO js_taskInstance VALUES('','"+taskName+"','"+eMail+"','"+str(tresholdLevel)+"','"+str(notificationSent)+"',\
	                                           '"+str(endedLevel)+"','"+proxy+"','"+uuid+"','"+status+"');"
	logging.info(sqlStr)
        dbCur.execute("START TRANSACTION")
        try:
            dbCur.execute(sqlStr)
        except Exception,ex:
            raise ProdAgentException("Error inserting the task in js_taskInstance. Taskname: '" + str(taskName) + "'.")
	dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.info("New Task ("+taskName+") inserted in the PA's DB")
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error inserting a new task ("+ taskName +") in the PA's DB!" )
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
        else:
            logging.error( "Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error updating PA DB! Method: " + updateNotSumbitted.__name__ )
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
            try:
                rowModified=dbCur.execute(sqlStr)
            except Exception,ex:
                raise ProdAgentException("Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        else:
            logging.error( "Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error updating PA DB! Method: " + updatingEndedPA.__name__ )
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

def getAllNotFinished():
    """
    _getAllNotFinished_
    """
    queryString = "SELECT taskName,eMail,tresholdLevel,notificationSent,endedLevel,status,uuid"+\
                  " FROM js_taskInstance"+\
		  " WHERE status <> 'killed' AND status <> 'not submitted' AND ((endedLevel < 100 AND status <> 'ended') OR notificationSent < 2);"
    task2Check = queryMethod(queryString, None)
    
    return task2Check

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
    logging.info( "   -> updating the task table for task: " + taskName )
    logging.debug( "   Setting the field endedLevel at '" + newPercentage +"'")
    logging.debug( "   Setting the field status  at '" + status +"'")

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        dbCur.execute("START TRANSACTION")
        if checkExistPA(conn, dbCur, taskName):
	    sqlStr='UPDATE js_taskInstance SET endedLevel="'+newPercentage+'", status="'+status+'"\
		    WHERE taskName="'+taskName+'";'
            try:
                rowModified=dbCur.execute(sqlStr)
            except Exception,ex:
                raise ProdAgentException("Error updating 'endedLevel' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        else:
            logging.error( "Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error updating PA DB! Method: " + updatingEndedPA.__name__ )
	raise


def updatingNotifiedPA( taskName, sended):
    """
    _updatingNotified_
    """

    sendFlag = str(sended)
    logging.info( "   -> updating the task table for task: " + taskName )
    logging.debug( "   Setting the field notificationSend at '" + sendFlag +"'")

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        ## opening connection with PA's DB
        conn, dbCur = openConnPA()
	dbCur.execute("START TRANSACTION")
	if checkExistPA(conn, dbCur, taskName):
	    sqlStr='UPDATE js_taskInstance SET notificationSent="'+sendFlag+'"\
		    WHERE taskName="'+taskName+'";'
            try:
                rowModified=dbCur.execute(sqlStr)
            except Exception,ex:
                raise ProdAgentException("Error updating 'notificationSent' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        else:
            logging.error( "Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
	dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error updating PA DB! Method: " + updatingNotifiedPA.__name__ )
	raise

def updatingStatus( taskName, status, notification ):
    """
    updatingStatus
    """
    
    logging.info( "   -> updating the task table for task: " + taskName )

    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        ## opening connection with PA's DB
        conn, dbCur = openConnPA()
	dbCur.execute("START TRANSACTION")
	if checkExistPA(conn, dbCur, taskName):
	    sqlStr='UPDATE js_taskInstance SET status="'+status+'", notificationSent="'+str(notification)+'"\
		    WHERE taskName="'+taskName+'";'
            try:
                rowModified=dbCur.execute(sqlStr)
            except Exception,ex:
                raise ProdAgentException("Error updating task killed in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        else:
            logging.error( "Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
	dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error updating PA DB! Method: " + updatingNotifiedPA.__name__ )
	raise


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
        else:
            logging.error( "Error updating 'status' to '"+status+"' in js_taskInstance. TaskName: '" + str(taskName) + "'.")
        dbCur.execute("COMMIT")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
    except:
	dbCur.execute("ROLLBACK")
        ## closing connection with PA's DB
        closeConnPA( dbCur, conn )
	logging.error( "Error updating PA DB! Method: " + cleaningTaskPA.__name__ )
	raise
 
