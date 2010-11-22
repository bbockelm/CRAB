#!/usr/bin/env python

"""
_CreateXmlJobReport_


"""
import xml.dom.minidom
import xml.dom.ext
import string
import os
import re
import logging

class JobXml:
    """
    _Job_
    
    """
    
    #------------------------------------------------------------------------
    def __init__(self):
        self.JOBID          = "id"
        self.STATUS         = "status"
        self.EXITSTATUS     = "exe_exit"
        self.JOBEXIT        = "job_exit"
        self.JOBREPORT      = "Job"
        self.JOBCLEARED     = "cleared"
        self.ALLOWED_STATES = ("Done(failed)","Running","Aborted","Cancelled","Cleared","Done","Ready","Submitted","Scheduled","Unknown","Waiting", "CannotSubmit","Killing","Killed","Submitting","Done (Failed)", "Created", "Cancelled by user", "EXIT", "RUN", "Abandoned")
        self.SITE           = "site"
        self.RESUB          = "resubmit" 
        self.STATCODE       = "sched_status"
        self.SCHEDID        = "sched_id"
        self.ENDED          = "ended" 
        self.ACTION         = 'action'
        self.JOBSUBISSION   = "submission"         
        self.PROCSTATUS     = "procestatus"

        self.jobid      = ""
        self.status     = ""
        self.exitstatus = ""
        self.jobexit    = ""
        self.jobcleared = ""
        self.site       = ""
        self.resub      = ""
        self.statcode   = ""
        self.sId        = ""
        self.ended      = ""
        self.action     = ""
        self.jobsubmission = ""
        self.procstatus = ""

        self.doc            = xml.dom.minidom.Document()
	#self.root           = self.doc.createElement( self.ROOTNAME )
	#self.init           = False
        
    #------------------------------------------------------------------------
    def initialize(self, jobid, status, job_exit, exe_exit, job_cleared, resub, site, sched_status, sId = "", ended = "", act = "", jsub= "", procs = "" ):
        self.jobid      = jobid
        self.status     = status
        self.exitstatus = exe_exit
        self.jobexit    = job_exit
        self.jobcleared = job_cleared
        self.statcode   = sched_status
        self.sId        = sId
        self.ended      = ended
        self.action     = act
        self.jobsubmission = jsub
        self.procstatus = procs

        jobrep = self.doc.createElement(self.JOBREPORT)
        jobrep.setAttribute(self.JOBID, str(self.jobid))

        jobrep.setAttribute(self.STATUS, self.status)
        jobrep.setAttribute(self.EXITSTATUS, str(self.exitstatus))
        jobrep.setAttribute(self.JOBEXIT, str(self.jobexit))
        jobrep.setAttribute(self.JOBCLEARED, str(self.jobcleared))
        jobrep.setAttribute(self.SITE, str(site))
        jobrep.setAttribute(self.RESUB, str(resub))
        jobrep.setAttribute(self.STATCODE, str(self.statcode))
        jobrep.setAttribute(self.SCHEDID, str(self.sId))
        jobrep.setAttribute(self.ENDED, str(self.ended))
        jobrep.setAttribute(self.ACTION, str(self.action))
        jobrep.setAttribute(self.JOBSUBISSION, str(self.jobsubmission))
        jobrep.setAttribute(self.PROCSTATUS, str(self.procstatus))

        self.report = jobrep
        return self


    #------------------------------------------------------------------------
    def getJobTagName(self):
        return self.JOBREPORT

    #------------------------------------------------------------------------
    def getAllowedStates(self):
        return self.ALLOWED_STATES
    
    #------------------------------------------------------------------------
    def getJobIDTagName(self):
        return self.JOBID

    #------------------------------------------------------------------------
    def getStatusTagName(self):
        return self.STATUS

    #------------------------------------------------------------------------
    def getJobStatus(self):
        return self.status
    
    #------------------------------------------------------------------------
    def getJobExitCode(self):
        return self.jobexit
    
    #------------------------------------------------------------------------
    def getExeExitCode(self):
        return self.exitstatus

    #------------------------------------------------------------------------
    def getJobCleared(self):
        return self.jobcleared
    
    #------------------------------------------------------------------------
    def getJobID(self):
        return self.jobid
    
    #------------------------------------------------------------------------
    def getDoc(self):
        return self.report

    #------------------------------------------------------------------------
    def toXml(self):
        return self.report.toxml()

    #------------------------------------------------------------------------
    def getJobFieldNameList(self):
        return [ \
                 self.JOBID, \
                 self.STATUS, \
                 self.EXITSTATUS, \
                 self.JOBEXIT, \
                 self.JOBREPORT, \
                 self.JOBCLEARED, \
                 self.SITE, \
                 self.RESUB, \
                 self.STATCODE, \
                 self.SCHEDID, \
                 self.ENDED, \
                 self.ACTION \
               ]

##-------------------------------------------------------------------------------------------------------



##class ReportFactory:
##    """
##    _Report_
##    """
##    #------------------------------------------------------------------------
##    def __init__(self, id, status):
        
    
##-------------------------------------------------------------------------------------------------------



        
class CreateXmlJobReport:
    """
    _CreateXmlJobReport_

    """
    
    #------------------------------------------------------------------------
    def __init__(self):
        self.ROOTNAME       = "TaskTracking"
        self.TASKREPORT     = "TaskReport"
        self.EMAIL          = "email"
        self.OWNER	    = "owner"
        self.TASKNAME       = "taskName"
        self.ENDED          = "ended"
        self.THRESHOLDREQ   = "thresholdRequested"
        self.TOTJOB         = "totJob"
        self.ALLOWED_STATES = ("Done(failed)","Running","Aborted","Cancelled","Cleared","Done","Ready","Submitted","Scheduled","Unknown","Waiting", "CannotSubmit","Killed","Submitting", "Done (Failed)", "Created", "Cancelled by user", "EXIT", "RUN", "Abandoned")
        self.COUNT          = 'count'
        self.SITE           = "site"
        self.RESUB          = "resubmit"
        self.STATCODE       = "sched_status"
        self.SCHEDID        = "sched_id"
        self.TASKSTATUS     = "TaskStatus"
        self.ACTION         = "action"
        self.PROCSTATUS     = "procestatus"

        self.doc            = xml.dom.minidom.Document()
        self.root           = self.doc.createElement( self.ROOTNAME )
        self.init           = False
	
        self.statusHash     = {}

    #------------------------------------------------------------------------
    def initialize(self, tname, email, owner, percent_ended, threshold, totjob, process = "Processed"):
    	#root = self.doc.createElementNS(self.NS,"TaskReport")
	#taskrep =  self.doc.createElementNS(self.NS,"TaskReport")
        taskrep =  self.doc.createElement(self.TASKREPORT)
        #taskrep.setAttributeNS(self.NS,"taskName",tname)#
        #taskrep.setAttributeNS(self.NS,"email",email)#
        #taskrep.setAttributeNS(self.NS,"ended",str(percent_ended))#
	
        taskrep.setAttribute(self.TASKNAME,     tname)
        email = email.strip()

        # remove leading and traling spaces
        email = email.strip()

        # check email format validity
        #for addr in email.split():
        #    if not self.checkEmailAddress( addr ):
        #        errmsg = "Error parsing email address; address ["+ addr +"] has "
        #        errmsg += "an invalid format"
        #        raise RuntimeError, errmsg
            
        taskrep.setAttribute(self.EMAIL,        email)
        taskrep.setAttribute(self.ENDED,        str(percent_ended) )
        taskrep.setAttribute(self.THRESHOLDREQ, str(threshold) )
        taskrep.setAttribute(self.OWNER,	owner)
        taskrep.setAttribute(self.TOTJOB,       str(totjob) )
        taskrep.setAttribute(self.TASKSTATUS,   str(process) )
        
        self.root.appendChild(taskrep)
        self.doc.appendChild(self.root)
        self.init = True

    #------------------------------------------------------------------------
    def addJob(self, jobid, status, jobexit, exeexit, cleared, resub, site):
        J = JobXml()
        
        self.root.appendChild( J.initialize(jobid, status, jobexit, exeexit, cleared, resub, site).getDoc() )


    #------------------------------------------------------------------------
    def addJob(self, aJob ):
        self.root.appendChild( aJob.getDoc() );
        

    #------------------------------------------------------------------------
    def addEmailAddress(self, email):
    	if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"

        reg = re.compile('^[\w\.-]+@[\w\.-]+$')
        if not reg.match( email ):
            errmsg = "Error parsing email address; address ["+email+"] has "
            errmsg += "an invalid format"
            raise RuntimeError, errmsg

        
	
        element = self.doc.getElementsByTagName(self.TASKREPORT)[0]
        currentMail = element.getAttribute( self.EMAIL )
        
        currentMail += " " + email
        currentMail = currentMail.strip(  )
        element.setAttribute( self.EMAIL, currentMail )

    #------------------------------------------------------------------------
    def toXml(self):
        return self.doc.toxml()

    #------------------------------------------------------------------------
    def printMe(self):
    	if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
        xml.dom.ext.PrettyPrint(self.doc)


    #------------------------------------------------------------------------
    def getTaskname( self ):
    	if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
        element = self.doc.getElementsByTagName( self.TASKREPORT )[0]
        return element.getAttribute( self.TASKNAME )
	
    #------------------------------------------------------------------------
    def getUserMail( self ):
    	if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"

        element = self.doc.getElementsByTagName(self.TASKREPORT )[0]
        mailArray = element.getAttribute(self.EMAIL).split( " " )
        return mailArray
    
    #------------------------------------------------------------------------
    def getPercentTaskCompleted( self ):
    	if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
        element = self.doc.getElementsByTagName(self.TASKREPORT )[0]
        return element.getAttribute( self.ENDED )
	   
    
    #------------------------------------------------------------------------
    def getTaskReportForSingleJob(self):
        report = "CANNOT SET REPORT TEXT"
        
        #print "Report for single Job: "
        allJobs = self.root.getElementsByTagName( JobXml().getJobTagName() )
        #print "len=%d\n" % len(allJobs)
        if allJobs[0].getAttribute( JobXml().getStatusTagName())=="CannotSubmit":
            report = " has not been submitted by the server.\nPlease, check your log files and try to execute the command 'crab -testJdl' to verify if there are sites that can satisfy your requirements."
            #return report

        if allJobs[0].getAttribute( JobXml().getStatusTagName())=="Killed":
            report = " has been correctly killed."
            
            
        if allJobs[0].getAttribute( JobXml().getStatusTagName())=="NotKilled":
            report = " has not been correctly killed."                                                                  
        return report
          
    #------------------------------------------------------------------------
    def getTaskOutcome(self):

        allJobs = self.root.getElementsByTagName( JobXml().getJobTagName() )
        if len(allJobs) == 1 and allJobs[0].getAttribute( JobXml().getJobIDTagName())== "all":
            outcome = ""
            allJobs = self.root.getElementsByTagName( JobXml().getJobTagName() )
            
            if allJobs[0].getAttribute( JobXml().getStatusTagName())=="CannotSubmit":
                outcome = " has not been submitted by the server."
                
            if allJobs[0].getAttribute( JobXml().getStatusTagName())=="Killed":
                outcome = " has been correctly killed."
                
            if allJobs[0].getAttribute( JobXml().getStatusTagName())=="NotKilled":
                outcome = " has not been correctly killed."                                                                  
        else:
            if(  int( self.getPercentTaskCompleted() ) == 100):
		outcome = "is completed at 100%"
            else:
                outcome = "Reached the requested threshold level "+ str(self.getThresholdRequest()) + "%"
            
        return outcome
    
    #------------------------------------------------------------------------
    def getTaskReport(self):


        allJobs = self.root.getElementsByTagName( JobXml().getJobTagName() )

        report = ""

        if len(allJobs) == 1 and allJobs[0].getAttribute( JobXml().getJobIDTagName())== "all":
            report = self.getTaskReportForSingleJob()
        else:
            if(  int( self.getPercentTaskCompleted() ) == 100):
		report += "is completed at: 100%\n\n"
            else:
		report += "Reached the requested threshold level "
		report += str(self.getThresholdRequest()) + "%\n" 
		report += "Actual level: " + self.getPercentTaskCompleted() + "%\n\n"
                
            report += "Status Report:\n"
            allJobs = self.root.getElementsByTagName( JobXml().getJobTagName() )
            statusStat = {}
            for status in JobXml().getAllowedStates():
                statusStat[ status ] = 0

                
            for job in allJobs:
                statusStat[ job.getAttribute( JobXml().getStatusTagName() ) ] += 1
                        
            for status in statusStat.keys():
                if statusStat[ status ] == 0:
                    continue
                report += str(statusStat[ status ]) + " Job(s) in status [" + status + "]\n"
        
        numJobMex = ""
        if str(self.getTotalJob()) != "0":
            numJobMex = " and composed by " + str(self.getTotalJob()) + " job(s)\n"
        
        
	msg = "The task '" + self.getTaskname() +"' owned by " + self.getOwner() + numJobMex  + report
	
        
	return msg
	
    #------------------------------------------------------------------------
    def getTotalJob(self):
	if not self.init:
                raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first" 
        element = self.doc.getElementsByTagName(self.TASKREPORT )[0]
        return element.getAttribute(self.TOTJOB)
	
    #------------------------------------------------------------------------
    def getThresholdRequest(self):
	if not self.init:
                raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
        element = self.doc.getElementsByTagName(self.TASKREPORT )[0]
        return element.getAttribute(self.THRESHOLDREQ)

    #------------------------------------------------------------------------
    def getOwner(self):
	if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
        element = self.doc.getElementsByTagName(self.TASKREPORT )[0]
        return element.getAttribute(self.OWNER)

    #------------------------------------------------------------------------
    def checkEmailFormat(self, email):
        reg = re.compile('^[\w\.-]+@[\w\.-]+$')
        if not reg.match( email ):
            return False
##            errmsg = "Error parsing email address; address ["+email+"] has "
##            errmsg += "an invalid format"
##            raise RuntimeError, errmsg
        
        return True

    #------------------------------------------------------------------------
    def toFile(self, filename):
        if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"

	filename_tmp = filename+".tmp"
        file = open(filename_tmp, 'w')
        tryals = 2
        count = 0
        while (count < tryals):
            try:
                xml.dom.ext.PrettyPrint(self.doc, file)
                break
            except MemoryError, ex:
                logging.error("Memory error writing on file: %s"%str(filename))
                count += 1
            except Exception, ex:
                logging.error("%s error writing on file: %s"%(str(ex),str(filename)))
                count += 1

        file.close()
        command_rename = "mv "+str(filename_tmp)+" "+str(filename)+";"
        os.popen( command_rename )  # this should be an atomic operation thread-safe and multiprocess-safe
#	os.rename(filename_tmp, filename) # this should be an atomic operation thread-safe and multiprocess-safe

    #------------------------------------------------------------------------
    def fromFile(self, filename):
    	self.statusHash = {}
	if not os.path.exists(filename):
		errmeg = "Cannot open file [" + filename + "] for reading. File is not there."
		raise RuntimeError, errmeg
  	file = open(filename, "r")
	self.doc = xml.dom.minidom.parse( filename )

        element = self.doc.getElementsByTagName( self.ROOTNAME )
        if len(element) == 0:
            errmsg="Cannot find root node with name '" + self.ROOTNAME +"' in the xml document ["+filename+"]"
            raise RuntimeError, errmsg

        ## assing to self.root the actual <TaskTracking> node in memory loaded from the file
        self.root = element[0]

        element = self.doc.getElementsByTagName( self.TASKREPORT )
        if len(element) == 0:
            errmsg="Cannot find root node with name '" + self.TASKREPORT +"' in the xml document ["+filename+"]"
            raise RuntimeError, errmsg

        # tr is now the <TaskReport> ... </TaskReport> Node
        tr = element[0]
        
	if not tr.hasAttribute(self.EMAIL) or not tr.hasAttribute(self.TASKNAME) or not tr.hasAttribute(self.ENDED) or not tr.hasAttribute(self.THRESHOLDREQ) or not tr.hasAttribute(self.OWNER) or not tr.hasAttribute(self.TOTJOB):
		errmsg="Missing one or more of the following attributes for TaskReport node: [email], [ended], [taskName], [thresholdRequested], [owner], [totJob] in file ["+filename+"]"
		raise RuntimeError, errmsg
		
		
	mail = tr.getAttribute( self.EMAIL )
	mail = mail.strip(  )

        # check email format validity
        for addr in mail.split():
            if not self.checkEmailFormat( addr ):
                errmsg = "Error parsing email address; address ["+ addr +"] has "
                errmsg += "an invalid format. Multiple addresses must be separated by spaces."
                raise RuntimeError, errmsg
        
	tr.setAttribute( self.EMAIL, mail )

        Jobs = self.doc.getElementsByTagName( JobXml().getJobTagName() )
        for job in Jobs:
            status = job.getAttribute( JobXml().getStatusTagName() )
            if status not in JobXml().getAllowedStates():
                errmsg = "Status [" + status + "] not allowed. Please check file [" + filename + "]"
                raise RuntimeError, errmsg

	self.init = True


    def getJobValues(self):
        tagdiction = {}
        Jobs = self.doc.getElementsByTagName( JobXml().getJobTagName() )
        counter = 1
        for eve in Jobs:
            evediction = {}
            for fld in JobXml().getJobFieldNameList():
                evediction.setdefault(fld, eve.getAttribute( fld ) )
            tagdiction.setdefault(counter, evediction)
            counter += 1
        return tagdiction

       
if __name__=="__main__":
	c = CreateXmlJobReport()
	#c.initialize("taskname alvise", "dorigoa@pd.infn.it tdluigi@yahoo.it moreno@pd.infn.it", "dorigoa", 76, 60, 21)
	#c.fromFile("/data/dorigoa/killed.xml")
        c.fromFile("/data/cms/logs/mcinquil_crab_0_081009_114551_f65c0bb2-3c16-4765-823f-9367da23ac3d_spec/xmlReportFile.xml")
        print c.getJobValues()
#        print "%s\n" % c.toXml()
        
	#c.addStatusCount("JobFailed",1)
        #c.addStatusCount("JobSuccess", "2")
	#c.addEmailAddress("pippo2@pd.infn.it")
	#c.printMe()
	#c.printStates()
	#print "%s\n" % c.getTaskReport()
	#print "To: %s\n" % str(c.getUserMail())
##        J1 = Job()
##        J2 = Job()
##        J3 = Job()
##        J1.initialize("Cream01", "JobFailed", 1, 0)
##        J2.initialize("Cream02", "JobFailed", 1, 0)
##        J3.initialize("Cream03", "JobRunning", 1, 0)

##        c.addJob( J1 )
##        c.addJob( J2 )
##        c.addJob( J3 )

        print "%s\n" % c.getTaskReport()
        
        #print "%s" % c.toXml()
#	c.toFile("prova2.xml")
