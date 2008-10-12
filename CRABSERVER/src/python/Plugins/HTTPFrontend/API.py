import time, os, datetime,commands
import logging
# DB PA
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect
from ProdAgentDB.Config import defaultConfig as dbConfig


def openConnPA():
    """
    _openConnPA_
     opening connection with the PA DB
    """
    conn=connect(False)
    dbCur=conn.cursor()
    return conn, dbCur

def closeConnPA( conn, dbCur):
    """
    _closeConnPA_
    closing connection with the PA DB
    """
    dbCur.close()
    conn.close()

def queryMethod( strQuery ):
    """
    _queryMethod_
    standard method for doing queries
    """
    ## opening connection with PA's DB
    conn, dbCur = openConnPA()
    try:
        dbCur.execute("START TRANSACTION")
        try: 
            dbCur.execute(strQuery)
        except Exception,ex:
            raise ProdAgentException("Query problems")
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

def composeDestinationCondition(destination = 'all'):
    if destination == 'all' or destination == ():
       dest_condition=""
    else:
       dest_condition="and ("
       for sub in destination:
          dest_condition+="destination like '%"+sub+"%' or "
       dest_condition+=" false)"
    return dest_condition

def getQueues(destination = 'all'):
    dest_condition = composeDestinationCondition(destination);
    queryString = "select distinct(destination) from bl_runningjob where 1 "+dest_condition
    queues = queryMethod(queryString)
    return queues

# # #
#  js_taskInstance
#

def getTimeStatusTask( time ):
    dateCondition= '' 
    dateCondition = " and land_time  > DATE_SUB(Now(),INTERVAL "+str(time)+"  SECOND)"
 
    queryString = "SELECT status,land_time  FROM js_taskInstance where 1 "+dateCondition +"  ORDER by status;"
    taskCheck = queryMethod(queryString)
    return taskCheck


def getTasks(from_time, ended=''):
    notif= ended
    if ended == True: notif =' and notificationSent > 1 '
    elif ended == False: notif= ' and notificationSent < 2 '
    dateCondition= ''
    dateCondition = " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND)"
    
    queryString = "SELECT taskName, status FROM js_taskInstance where 1 "+notif+" "+dateCondition +" ORDER by land_time;"
    print queryString 
    taskCheck = queryMethod(queryString)
    return taskCheck


def getNumTask(from_time, ended=''):
    notif= ended
    if ended == True: notif =' and notificationSent > 1 '
    elif ended == False: notif= ' and notificationSent < 2 '
    dateCondition= '' 
    dateCondition = " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND)"
 
    queryString = "SELECT count(status),status  FROM js_taskInstance where 1 "+notif+" "+dateCondition +"  group by status ORDER by status;"
    print queryString 
    taskCheck = queryMethod(queryString)
    return taskCheck

def getSites(from_time='all',Sites='all'):
    """
    """  
    condition =  " where destination <> '' "
    dateCondition = '' 
    if from_time != 'all': 
       dateCondition =  " and submission_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    destCondition  = composeDestinationCondition(Sites)
    outputSites={};
    OverallCount = 0;
    queryString = "select count(*),destination from bl_runningjob "+condition+" "+dateCondition+" "+destCondition+"group by destination;"
    print queryString
    taskCheck = queryMethod(queryString)
    tmpSite=[] 
    for count, site in taskCheck:
       tmpSite.append(str(site).split(':')[0])
    dict_results={}     
    # try to remove duplicated entries 
    for s in sorted(set(tmpSite)):
        sum_count=0     
        for count, site in taskCheck:
            if str(site).find(str(s))>-1:
                sum_count += count     
        dict_results[s]=sum_count  

    return dict_results

def getTimeStatusJob( time ) :
    
    dateCondition= ''
    dateCondition = " and submission_time  > DATE_SUB(Now(),INTERVAL "+str(time)+"  SECOND)"
    
    queryString = "SELECT status_scheduler,submission_time  FROM bl_runningjob where 1 "+dateCondition +"  ORDER by status_scheduler;"
    print queryString 
    taskCheck = queryMethod(queryString)
    return taskCheck


def getKeyNum(key,destination='all',from_time='all'):
    dateCondition =  " and submission_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    dest_condition = composeDestinationCondition(destination);
    queryString = "select count("+key+"),"+key+" from bl_runningjob where 1 "+dateCondition+" "+dest_condition+" group by "+key+" "
    taskCheck = queryMethod(queryString)
    
    return taskCheck


# Statistics users
def getUserName(from_time):
    queryString = "select user_name,land_time from js_taskInstance "+\
                  "where land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) and user_name <> ''  group by proxy ORDER by land_time ;"
    taskCheck = queryMethod(queryString)
    return taskCheck



# Componets Monitor
def getPIDservice(search_service,service):
    cmd = 'ps -ef |grep "'+search_service+'"  |grep -v "grep '+search_service+'" | cut -d " " -f 6 | head -1'

    shellOut = commands.getstatusoutput(cmd)
    pid = shellOut[1]
    if  not os.path.exists("/proc/"+str(pid)) or str(pid) == "" :
        msg = [service,"Not Running"]
    else:
        msg = [service,"PID : "+pid ]

    return msg

def getpidof(procname,service):
    cmd = os.popen("ps -A -o pid,command")
    for l in cmd.readlines():
        s = l.strip().split(' ')[1]
        pid = 0
        if procname in s and s[0] =='/':
           pid = l.strip().split(' ')[0]
           break
    if  not os.path.exists("/proc/"+str(pid)) or str(pid) == 0 :
        msg = [service,"Not Running"]
    else:
        msg = [service,"PID : "+pid ]

    return msg
