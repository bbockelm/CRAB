import time, os, datetime,commands
import logging
# DB PA
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect
from ProdAgentDB.Config import defaultConfig as dbConfig
import re

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
#  tt_taskInstance
#

def getTimeStatusTask( time ):
    dateCondition= '' 
    dateCondition = " and land_time  > DATE_SUB(Now(),INTERVAL "+str(time)+"  SECOND)"
 
    queryString = "SELECT status,land_time  FROM tt_taskInstance where 1 "+dateCondition +"  ORDER by status;"
    taskCheck = queryMethod(queryString)
    return taskCheck


def getTasks(from_time, ended=''):
    notif= ended
    if ended == True: notif =' and notificationSent > 1 '
    elif ended == False: notif= ' and notificationSent < 2 '
    dateCondition= ''
    dateCondition = " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND)"
    
    queryString = "SELECT task_name, status FROM tt_taskInstance where 1 "+notif+" "+dateCondition +" ORDER by land_time;"
    print queryString 
    taskCheck = queryMethod(queryString)
    return taskCheck

def getUserTasks(username='', from_time='', ended=''):
    user = ''
    if username != 'All':
        user = " and user_name = '%s'" %(username)
    timer = from_time
    if from_time != '':
        timer = " and land_time > DATE_SUB(Now(),INTERVAL %s SECOND)" %(str(from_time))
    notif = ended
    if ended == True: notif = " and notificationSent > 1"
    elif ended == False: notif =" and notificationSent < 2"
    queryString = "SELECT task_name, status, ended_level FROM tt_taskInstance " +\
                  "WHERE 1 %s %s %s ORDER BY land_time DESC;"%(user, notif, timer)
    taskCheck = queryMethod(queryString)
    return taskCheck

def getNumTask(from_time, ended=''):
    notif= ended
    if ended == True: notif =' and notificationSent > 1 '
    elif ended == False: notif= ' and notificationSent < 2 '
    dateCondition= '' 
    dateCondition = " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND)"
 
    queryString = "SELECT count(status),status  FROM tt_taskInstance where 1 "+notif+" "+dateCondition +"  group by status ORDER by status;"
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


def getKeyNum_task(key,destination='all',from_time='all'):
    dateCondition =  " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    dest_condition = composeDestinationCondition(destination);
    queryString = "select count("+key+"),"+key+" from bl_task  bl_task join tt_taskInstance on\
                    bl_task.name=tt_taskInstance.task_name  where 1 "+dateCondition+" "+dest_condition+" group by "+key+" "
    taskCheck = queryMethod(queryString)
    
    return taskCheck


def countTasks(key,from_time='all'):
    datasetCondition =  " where dataset='%s' "%key 
    dateCondition =  " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    queryString = "select count(bl_task.id) from bl_task bl_task join tt_taskInstance on \
                   bl_task.name=tt_taskInstance.task_name "+datasetCondition+" "+dateCondition+";"
    taskCheck = queryMethod(queryString)[0][0]
    
    return taskCheck

def countJobs(key,from_time='all'):
    datasetCondition =  " where dataset='%s' "%key 
    dateCondition =  " and submission_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    queryString = "select count(bl_runningjob.id)  from bl_runningjob left join  bl_task on (bl_runningjob.task_id=bl_task.id) "\
                   +datasetCondition+" "+dateCondition+";"
    taskCheck = queryMethod(queryString)
    
    return taskCheck[0][0]

def countUsers(key,from_time='all'):
    datasetCondition =  " where dataset='%s' "%key 
    dateCondition =  " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    queryString = "select count(distinct(user_name)) from tt_taskInstance tt_taskInstance join bl_task on \
                   bl_task.name=tt_taskInstance.task_name "+datasetCondition+" "+dateCondition+";"
    taskCheck = queryMethod(queryString)[0][0]
    
    return taskCheck


def getKeyNum(key,destination='all',from_time='all'):
    dateCondition =  " and submission_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    dest_condition = composeDestinationCondition(destination);
    queryString = "select count("+key+"),"+key+" from bl_runningjob where 1 "+dateCondition+" "+dest_condition+" group by "+key+" "
    taskCheck = queryMethod(queryString)
    
    return taskCheck

def getTaskNameList(dataset,from_time):

    datasetCondition =  " where dataset='%s' "%dataset            
    dateCondition =  " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    queryString = "select task_name,user_name from tt_taskInstance join bl_task on (tt_taskInstance.task_name=bl_task.name) "+datasetCondition+" "+dateCondition+";"
    taskCheck = queryMethod(queryString)
    return taskCheck

def getUserNameList(dataset,from_time):

    datasetCondition =  " where dataset='%s' "%dataset            
    dateCondition =  " and land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    queryString = "select count(user_name),user_name from tt_taskInstance join bl_task on (tt_taskInstance.task_name=bl_task.name) "\
                   +datasetCondition+" "+dateCondition+"group by user_name;"
    taskCheck = queryMethod(queryString)
    return taskCheck


def getJobExit(dataset,from_time):
    datasetCondition =  " where dataset='%s' "%dataset            
    dateCondition =  " and submission_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    queryString = "select application_return_code, wrapper_return_code  from bl_runningjob left join  bl_task on (bl_runningjob.task_id=bl_task.id) " \
                   +datasetCondition+" "+dateCondition+" and status_scheduler = 'Cleared';"
    taskCheck = queryMethod(queryString)
    return taskCheck


def getWrapExit(dataset,from_time):
    datasetCondition =  " and dataset='%s' "%dataset
    dateCondition =  " and submission_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    queryString = "select count(wrapper_return_code),wrapper_return_code  from bl_runningjob left join  bl_task on (bl_runningjob.task_id=bl_task.id) where application_return_code = 0  and status_scheduler = 'Cleared' " +datasetCondition+" "+dateCondition+" group by wrapper_return_code;"
    taskCheck = queryMethod(queryString)
    return taskCheck


def getApplExit(dataset,from_time):
    datasetCondition =  " where dataset='%s' "%dataset            
    dateCondition =  " and submission_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) "
    queryString = "select count(application_return_code),application_return_code  from bl_runningjob left join  bl_task on (bl_runningjob.task_id=bl_task.id) "\
                   +datasetCondition+" "+dateCondition+" and status_scheduler = 'Cleared' group by application_return_code ;"
    taskCheck = queryMethod(queryString)
    return taskCheck

# Statistics users
def getUserName(from_time):
    queryString = "select user_name,land_time from tt_taskInstance "+\
                  "where land_time  > DATE_SUB(Now(),INTERVAL "+str(from_time)+"  SECOND) and user_name <> ''  group by proxy ORDER by land_time ;"
    taskCheck = queryMethod(queryString)
    return taskCheck

def getAllUserName():
    queryString = "select distinct(user_name) from tt_taskInstance ORDER BY user_name;"
    taskCheck = queryMethod(queryString)
    return taskCheck

# Components Monitor
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
    
def getPIDof(procname):
    if procname == 'mysqld':
        pids = os.popen("ps -C %s wwho pid,pid"%procname).read()
    else:
        pids = os.popen("ps -C %s wwho pgid,pid"%procname).read()
    if re.match('\s*[0-9]+\s*[0-9]+\s*',str(pids)):
        pgid, pid = str(pids).split()[0:2]
        if pid == pgid:
            return pid.rstrip()
    else:
        return "Not Running"

                                    
def isSensorRunning(comp):
    # get all sensors pids with their component pid
    sensors = os.popen('ps -C sar wwho pid,cmd').readlines()
    for sensor in sensors:
        spid,cpid = sensor.split()[0:4:3]
        Rcomp = os.popen('ps -p '+str(cpid)+' wwho cmd').read()  #.split('/')[-2]
        if re.search(comp,Rcomp):
            return True, spid, cpid
    return False, 0, 0

def isSensorDaemonRunning(comp):
    # get all sensors pids with their component pid
    sensors = os.popen('ps -C sensord wwho pid,cmd').readlines()
    for sensor in sensors:
        spid,Rcomp = sensor.split()[0:4:3]
        if re.search(comp,Rcomp):
            return True, spid
    return False, 0

##### grid status #####
def jobsByWMS(timer1="", timer2=""):
    condquery = ""
    if len(timer1) > 0:
         condquery += " submission_time > '%s' and " %timer1
    if len(timer2) > 0:
         condquery += " submission_time < '%s' and " %timer2
    queryString = "SELECT service, status_scheduler, count(status) FROM bl_runningjob " +\
                  "WHERE %s service <> 'NULL' GROUP BY service, status ORDER BY service;" %condquery
    results = queryMethod(queryString)
    return results

def messageListing(compName):
    which = ""
    if compName != "All" and len(compName) > 0:
        which = " AND target.name='%s' " %str(compName)
    query = "SELECT ms_message.messageid as id,ms_type.name as event,source.name as source,target.name as dest,ms_message.time,ms_message.delay "\
            "FROM ms_type,ms_message,ms_process as source,ms_process as target "\
            "WHERE ms_type.typeid=ms_message.type AND source.procid=ms_message.source AND target.procid=ms_message.dest %s "\
            "ORDER BY ms_message.time;"%which
    results = queryMethod(query)
    return query, results

##### internal server processing #####
def getOutputQueue():
    query = "select count(*) from bl_runningjob where closed = 'N' and process_status = 'output_requested';"
    results = queryMethod(query)
    return results

def getOutputFailedQueue():
    query = "select count(*) from bl_runningjob where closed = 'N' and process_status = 'failed';"
    results = queryMethod(query)
    return results

def jobTrackingLoad():
    query = "select count(*) from bl_runningjob where closed = 'N' and process_status like '%handled'; ";
    results = queryMethod(query)
    return results

def dequeuedJobs():
    query = "select count(*) from bl_runningjob where status = 'processed'";
    results = queryMethod(query)
    return results

def processStatusDistribution():
    query = "select process_status, count(*) from bl_runningjob where closed = 'N' group by process_status;";
    results = queryMethod(query)
    return results

def statusInfoTask(taskname):
    if taskname is not None and len(taskname) > 0:
        query = "select closed, status, process_status, output_request_time from bl_runningjob where task_id = (select id from bl_task where name = '%s');"%str(taskname)
        results = queryMethod(query)
        return results
    return None

