#!/usr/bin/env python
"""
_CreateXmlJobReport_


"""
import xml.dom.minidom
import xml.dom.ext
import string
import os
import re

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
	self.ALLOWED_STATES = ("JobRunning","JobSuccess","JobFailed","JobAborted","JobCancelled","JobCleared","JobInProgress")
	self.COUNT          = 'count'
	
	self.doc            = xml.dom.minidom.Document()
	self.root           = self.doc.createElement( self.ROOTNAME )
	self.init           = False
	
	self.statusHash     = {}

    #------------------------------------------------------------------------
    def initialize(self, tname, email, owner, percent_ended, threshold):
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
        taskrep.setAttribute(self.ENDED,        str(percent_ended))
	taskrep.setAttribute(self.THRESHOLDREQ, str(threshold))
	taskrep.setAttribute(self.OWNER,	owner)
	
        self.root.appendChild(taskrep)
        self.doc.appendChild(self.root)
	self.init = True

    #------------------------------------------------------------------------
    def addStatusCount(self, status_name, status_count):
	if not self.init:
		raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
	allowed = status_name in self.ALLOWED_STATES
	
	if not allowed:
		errmsg = "Status [" + status_name + "] not allowed"
		raise RuntimeError, errmsg
		
	newStatus = self.doc.createElement(status_name)
	newStatus.setAttribute(self.COUNT,str(status_count))
	self.doc.childNodes[0].appendChild(newStatus)

    #------------------------------------------------------------------------
    def addEmailAddress(self, email):
    	if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"

        reg = re.compile('^[\w\.-]+@[\w\.-]+$')
        if not reg.match( email ):
            errmsg = "Error parsing email address; address ["+email+"] has "
            errmsg += "an invalid format"
            raise RuntimeError, errmsg

        
	
	currentMail = self.doc.childNodes[0].childNodes[0].getAttribute(self.EMAIL)
	currentMail += " " + email
	currentMail = currentMail.strip(  )
	self.doc.childNodes[0].childNodes[0].setAttribute( self.EMAIL, currentMail )

    #------------------------------------------------------------------------
    def toXml(self):
	return self.doc.toxml()

    #------------------------------------------------------------------------
    def printMe(self):
    	if not self.init:
		raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
	xml.dom.ext.PrettyPrint(self.doc)

    #------------------------------------------------------------------------
    def printStates( self ):
    	for key in self.statusHash.keys():
		#i = int(self.statusHash[key])
		print "%s %s" % (key, self.statusHash[key])
		#print msg

    #------------------------------------------------------------------------
    def getTaskname( self ):
    	if not self.init:
		raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
    	return self.doc.childNodes[0].childNodes[1].getAttribute(self.TASKNAME)
	
    #------------------------------------------------------------------------
    def getUserMail( self ):
    	if not self.init:
		raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
    	mailArray = self.doc.childNodes[0].childNodes[1].getAttribute(self.EMAIL).split( " " )
	return mailArray
    
    #------------------------------------------------------------------------
    def getPercentTaskCompleted( self ):
    	if not self.init:
		raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
    	return self.doc.childNodes[0].childNodes[1].getAttribute(self.ENDED)
	   
    #------------------------------------------------------------------------
    def getTotalStatusCount( self ):
    	if not self.init:
		raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
	
	totalcount = 0;
	for key in self.statusHash.keys():
		totalcount += int(self.statusHash[key])
		
	return totalcount
	
    #------------------------------------------------------------------------
    def getTaskReport(self):
    	#msg=" Dear user " + self.getUserMail() + ", your task ["
	msg =  "The task '"
	msg += self.getTaskname() +"' owned by " + self.getOwner() + " and composed by " + str(self.getTotalStatusCount()) + " jobs\n"
	if(  int( self.getPercentTaskCompleted() ) == 100):
		msg+= "is completed at: 100%\n\n"
	else:
		msg += "Reached the requested threshold level "
		msg += str(self.getThresholdRequest()) + "%\n" #str(self.getPercentTaskCompleted())
		msg += "Actual level: " + self.getPercentTaskCompleted() + "%\n\n"
		
	#msg += "% completed. Please see the detailed report in the following\n\n"
	for key in self.statusHash.keys():
		msg += self.statusHash[key] + ":\t" + key +"\n"
	return msg
	
    #------------------------------------------------------------------------
    def getThresholdRequest(self):
	if not self.init:
                raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first" 
	return self.doc.childNodes[0].childNodes[1].getAttribute(self.THRESHOLDREQ)

    #------------------------------------------------------------------------
    def getOwner(self):
	if not self.init:
            raise RuntimeError, "Module CreateXmlJobReport is not initialized. Call CreateXmlJobReport.initialize(...) first"
        return self.doc.childNodes[0].childNodes[1].getAttribute(self.OWNER)

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
	root = self.doc.childNodes[0]
	if str(root.localName) != self.ROOTNAME:
		errmsg="Cannot find root node with name '" + self.ROOTNAME +"' in the xml document ["+filename+"]"
		raise RuntimeError, errmsg
	
	tr = self.doc.childNodes[0].childNodes[1]
	if str(tr.localName) != self.TASKREPORT:
		errmsg="Cannot find node with name '" + self.TASKREPORT +"' in the xml document ["+filename+"]"
		raise RuntimeError, errmsg
	
	if not tr.hasAttribute(self.EMAIL) or not tr.hasAttribute(self.TASKNAME) or not tr.hasAttribute(self.ENDED) or not tr.hasAttribute(self.THRESHOLDREQ) or not tr.hasAttribute(self.OWNER):
		errmsg="Missing one or more of the following attributes for TaskReport node: [email], [ended], [taskName], [thresholdRequested], [owner] in file ["+filename+"]"
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
	
	for element in self.doc.childNodes[0].childNodes:
		#print element.nodeValue
		if not element.localName in self.ALLOWED_STATES:
			continue
		if  not element.hasAttribute(self.COUNT):
			continue
			
		self.statusHash[ element.localName ] = element.getAttribute(self.COUNT)
		
	self.init = True
       
if __name__=="__main__":
	c = CreateXmlJobReport()
	#c.initialize("a", "b", 1)
	c.fromFile("xmlReportFile.xml")
	c.addStatusCount("JobFailed",1)
        c.addStatusCount("JobSuccess", "2")
	c.addEmailAddress("tdluigi@yahoo.it")
	#c.printMe()
	#c.printStates()
	print "%s\n" % c.getTaskReport()
	print "To: %s\n" % str(c.getUserMail())
	#c.toFile("prova.xml")
