class CrabWorkerAPI:

    def __init__(self):
        self.wejobmap = { \
                          'id':          'id', \
                          'spec':        'job_spec_file', \
                          'job_type':    'job_type', \
                          'max_retries': 'max_retries',\
                          'max_racers':  'max_racers', \
                          'owner':       'owner', \
                          'status':      'status', \
                          'cache':       'cache_dir' \
                        }

        pass

    def getWEStatus(self, jobId, dbSession = None):
        """
        _getWEStatus_

        get the actual we_job status
        """
        sqlStr = "select status from we_Job where id = '" + str(jobId) + "';"
        tupla = dbSession.select( sqlStr )
        if len(tupla) == 1:
            return str(tupla[0][0])
        else:
            raise Exception("Not just one entry has been found: "+str(tupla))

    def updateWEStatus(self, jobId, status, dbSession = None):
        """
        _updateWEStatus_

        update the status of the we_job
        """
        sqlStr = "update we_Job set status = '"+str(status)+"' " + \
                 "where id = '"+str(jobId)+"';"
        dbSession.modify(sqlStr)

    def existsWEJob(self, jobId, dbSession = None):
        """
        _existsWEJob_

        verify if a we_job already exists in the db
        """
        sqlStr = "select count(*) from we_Job where id = '" +str(jobId)+ "';"
        tupla = dbSession.select(sqlStr)
        if int(tupla[0]) == 1:
            return True
        elif int(tupla[0]) == 0:
            return False
        else:
            raise Exception("More then one entry has been found: "+str(tupla))

    def stopResubmission(self, jobIdList = [], dbSession = None):
        """
        _stopResubmission_
 
        Set racers to maxRacers + 1 and retries to maxRetries + 1
        """
        sqlStr = ""
        for jobSpecId in jobIdList:
            sqlStr = "UPDATE we_Job SET " + \
                     "racers=max_racers+1, retries=max_retries+1 " + \
                     "WHERE id=\""+ str(jobSpecId)+ "\";"
        dbSession.modify(sqlStr)

    def increaseSubmission(self, jobId, dbSession = None):
        """
        _increaseSubmission

        increase the we_Job submission counter
        """
        sqlStr = "UPDATE we_Job SET retries=retries+1 WHERE id='" + str(jobId) + "'"
        dbSession.modify(sqlStr)

    def registerWEJob(self, jobInfo, dbSession = None):
        """
        _registerWEJob_

        insert a new job in we_job table with the passed parameters
        """
        description = jobInfo.keys()
        # create values part
        sqlStrValues = '('
        comma = False
        for attribute in description:
            if comma:
                sqlStrValues += ','
            elif not comma :
                comma = True 
            sqlStrValues += self.wejobmap[attribute]
        sqlStrValues += ')'
        # start creating the full query
        sqlStr = "INSERT INTO we_Job " + sqlStrValues + " VALUES("
        valueComma = False
        for attribute in description:
            if valueComma:
                sqlStr += ','
            else:
                valueComma = True
            sqlStr += '"' + str(jobInfo[attribute]) + '"'
        sqlStr += ')'
        sqlStr += " ON DUPLICATE KEY UPDATE "
        comma = False
        for attribute in description:
            if comma and attribute != 'jobID':
                sqlStr += ','
            elif not comma and attribute != 'jobID':
                comma = True
            if attribute != 'jobID':
                sqlStr += self.wejobmap[attribute] + '="' + \
                          str(jobInfo[attribute]) + '"'
        dbSession.modify(sqlStr)
