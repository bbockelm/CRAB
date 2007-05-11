#!/usr/bin/env python
"""
_CreateXmlJobReport_


"""
import xml.dom.minidom
import xml.dom.ext
import string
import os
import re
class Job:
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
        self.ALLOWED_STATES = ("Running","JobSuccess","JobFailed","Aborted","Cancelled","Cleared","JobInProgress","Done","Ready","Submitted","Scheduled","Unknown","Waiting", "NotSubmitted","Killed")
        
        self.jobid      = ""
        self.status     = ""
        self.exitstatus = ""
        self.jobexit    = ""
        
        self.doc            = xml.dom.minidom.Document()
	#self.root           = self.doc.createElement( self.ROOTNAME )
	#self.init           = False
        
    #------------------------------------------------------------------------
    def initialize(self, jobid, status, job_exit, exe_exit):
        self.jobid      = jobid
        self.status     = status
        self.exitstatus = exe_exit
        self.jobexit    = job_exit

        jobrep = self.doc.createElement(self.JOBREPORT)
        jobrep.setAttribute(self.JOBID, str(self.jobid))

        allowed = status in self.ALLOWED_STATES
	
	if not allowed:
            errmsg = "Status [" + status + "] not allowed"
            raise RuntimeError, errmsg
        
        jobrep.setAttribute(self.STATUS, self.status)
        jobrep.setAttribute(self.EXITSTATUS, str(self.exitstatus))
        jobrep.setAttribute(self.JOBEXIT, str(self.jobexit))

        self.report = jobrep
        return self

    #------------------------------------------------------------------------
    def getJobTagName(self):
        return self.JOBREPORT

    #------------------------------------------------------------------------
    def getAllowedStates(self):
        return self.ALLOWED_STATES

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
    def getJobID(self):
        return self.jobid
    
    #------------------------------------------------------------------------
    def getDoc(self):
        return self.report

    #------------------------------------------------------------------------
    def toXml(self):
        return self.report.toxml()




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
        self.ALLOWED_STATES = ("Running","JobSuccess","JobFailed","Aborted","Cancelled","Cleared","JobInProgress","Done","Ready","Submitted","Scheduled","Unknown","Waiting", "NotSubmitted","Killed")
	self.COUNT          = 'count'
	
	self.doc            = xml.dom.minidom.Document()
	self.root           = self.doc.createElement( self.ROOTNAME )
	self.init           = False
	
	self.statusHash     = {}

    #------------------------------------------------------------------------
    def initialize(self, tname, email, owner, percent_ended, threshold, totjob):
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
        
        self.root.appendChild(taskrep)
        self.doc.appendChild(self.root)
	self.init = True

    #------------------------------------------------------------------------
    def addJob(self, jobid, status, jobexit, exeexit):
        J = Job()
        
        #JJ = Job()
        #try:
        #J.initialize(jobid, status, jobexit, exeexit)
        #JJ.initialize("sara", status, jobexit, exeexit)
        
        #print "New Job=%s" % J.getDoc().toxml()
        #print "New Job=%s" % JJ.getDoc().toxml()
        #except RuntimeError, msg:
            
        #jdoc = J.getDoc()

        #xmldoc = xml.dom.minidom.parseString( "<pippo alfa='1'/>" )
        
        self.root.appendChild( J.initialize(jobid, status, jobexit, exeexit).getDoc() )
        #self.root.appendChild( JJ.getDoc() )

        #localTaskRep = self.doc.getElementsByTagName(self.TASKREPORT)[0]

        #print "localTaskRep=%s" % localTaskRep.toxml()
        
        #print "self.root=%s" % self.root.toxml()

    #------------------------------------------------------------------------
    def addJob(self, aJob ):
        self.root.appendChild( aJob.getDoc() );
        
    #------------------------------------------------------------------------
##    def addStatusCount(self, status_name, status_count):
##	if not self.init:
##		raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
##	allowed = status_name in self.ALLOWED_STATES
	
##	if not allowed:
##		errmsg = "Status [" + status_name + "] not allowed"
##		raise RuntimeError, errmsg
		
##	newStatus = self.doc.createElement(status_name)
##	newStatus.setAttribute(self.COUNT,str(status_count))
##	self.doc.childNodes[0].appendChild(newStatus)

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
##    def printStates( self ):
##    	for key in self.statusHash.keys():
##		#i = int(self.statusHash[key])
##		print "%s %s" % (key, self.statusHash[key])
##		#print msg

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
##    def getTotalStatusCount( self ):
##    	if not self.init:
##		raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
##	totalcount = 0;
##	for key in self.statusHash.keys():
##		totalcount += int(self.statusHash[key])
		
##	return totalcount
	
    #------------------------------------------------------------------------
    def getTaskReport(self):
    	#msg=" Dear user " + self.getUserMail() + ", your task ["
	msg =  "The task '"

        
        
	msg += self.getTaskname() +"' owned by " + self.getOwner() + " and composed by " + str(self.getTotalJob()) + " jobs\n"
	if(  int( self.getPercentTaskCompleted() ) == 100):
		msg+= "is completed at: 100%\n\n"
	else:
		msg += "Reached the requested threshold level "
		msg += str(self.getThresholdRequest()) + "%\n" #str(self.getPercentTaskCompleted())
		msg += "Actual level: " + self.getPercentTaskCompleted() + "%\n\n"

        msg += "Status Report:\n"
        allJobs = self.root.getElementsByTagName( Job().getJobTagName() )
        statusStat = {}
        for status in Job().getAllowedStates():
            statusStat[ status ] = 0
            
        for job in allJobs:
            statusStat[ job.getAttribute( Job().getStatusTagName() ) ] += 1
            
        for status in statusStat.keys():
            if statusStat[ status ] == 0:
                continue
            msg += str(statusStat[ status ]) + " Job(s) in status " + status + "\n"
        
	#msg += "% completed. Please see the detailed report in the following\n\n"
##	for key in self.statusHash.keys():
##		msg += self.statusHash[key] + ":\t" + key +"\n"
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
	xml.dom.ext.PrettyPrint(self.doc, file)
	file.close()
	os.rename(filename_tmp, filename) # this should be an atomic operation thread-safe and multiprocess-safe

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
	
##	for element in self.doc.childNodes[0].childNodes:
##		#print element.nodeValue
##		if not element.localName in self.ALLOWED_STATES:
##			continue
##		if  not element.hasAttribute(self.COUNT):
##			continue
			
##		self.statusHash[ element.localName ] = element.getAttribute(self.COUNT)
        
	self.init = True
       
if __name__=="__main__":
	c = CreateXmlJobReport()
	#c.initialize("taskname alvise", "dorigoa@pd.infn.it tdluigi@yahoo.it moreno@pd.infn.it", "dorigoa", 76, 60, 21)
	c.fromFile("prova.xml")

#        print "%s\n" % c.toXml()
        
	#c.addStatusCount("JobFailed",1)
        #c.addStatusCount("JobSuccess", "2")
	#c.addEmailAddress("pippo2@pd.infn.it")
	#c.printMe()
	#c.printStates()
	#print "%s\n" % c.getTaskReport()
	#print "To: %s\n" % str(c.getUserMail())
        J1 = Job()
        J2 = Job()
        J3 = Job()
        J1.initialize("Cream01", "JobFailed", 1, 0)
        J2.initialize("Cream02", "JobFailed", 1, 0)
        J3.initialize("Cream03", "JobRunning", 1, 0)

        c.addJob( J1 )
        c.addJob( J2 )
        c.addJob( J3 )

        print "%s\n" % c.getTaskReport()
        
        #print "%s" % c.toXml()
#	c.toFile("prova2.xml")
