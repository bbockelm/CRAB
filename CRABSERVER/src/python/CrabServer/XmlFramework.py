#!/usr/bin/env python

"""
_XmlFramework_


"""
import xml.dom.minidom
import xml.dom.ext
import string
import os
import re
import logging

class Event:
    """
    _Event_

    Represent the information to be published in the xml file (a NODE)
    """
    
    #------------------------------------------------------------------------
    def __init__(self):
        self._fields = { \
                         "date":   "", \
                         "ev":     "", \
                         "txt":    "", \
                         "reason": "", \
                         "code":   "", \
                         "time":   ""  \
                       }
        self._eventreport = "Event"
        self._report = None
        
        self.doc = xml.dom.minidom.Document()
        
    #------------------------------------------------------------------------
    def initialize(self, values): ## date, ev, txt, reason, code, time):
        self._fields = values

        eventrep = self.doc.createElement(self._eventreport)
        for key, value in self._fields.iteritems():
   #         if key == "txt":
             eventrep.setAttribute(str(key), str(value))

        self._report = eventrep
        return self

    #------------------------------------------------------------------------
    def getDoc(self):
        return self._report

    #------------------------------------------------------------------------
    def toXml(self):
        return self._report.toxml()


##-------------------------------------------------------------------------------------------------------


class XmlFramework:
    """
    _XmlFramework_

    """
    
    #------------------------------------------------------------------------
    def __init__(self):
    	self._rootname      = "InternalLogInfo"
	self._evereport     = "Report"
	self._owner         = "owner"
	self._taskname      = "taskName"
	
	self.doc            = xml.dom.minidom.Document()
	self.root           = self.doc.createElement( self._rootname )
	self.init           = False
	
    #------------------------------------------------------------------------
    def initialize(self, tname, owner=None):
	everep =  self.doc.createElement(self._evereport)

	everep.setAttribute(self._taskname, tname)
        if owner != None:
            everep.setAttribute(self._owner, owner)
        
        self.root.appendChild(everep)
        self.doc.appendChild(self.root)
	self.init = True

    #------------------------------------------------------------------------
    def addNode(self, node):
        self.root.appendChild( node.getDoc() )

    #------------------------------------------------------------------------
    def toXml(self):
	return self.doc.toxml()

    #------------------------------------------------------------------------
    def printMe(self):
        if not self.init:
            raise RuntimeError, "Module XmlFramework is not initialized. Call XmlFramework.initialize(...) first"
	
        xml.dom.ext.PrettyPrint(self.doc)

    #------------------------------------------------------------------------
    def toFile(self, filename):
    	if not self.init:
		raise RuntimeError, "Module XmlFramework is not initialized. Call XmlFramework.initialize(...) first"
	
	filename_tmp = filename+".tmp"
	file = open(filename_tmp, 'w')
	xml.dom.ext.PrettyPrint(self.doc, file)
	file.close()
        command_rename = "mv "+str(filename_tmp)+" "+str(filename)+";"
        os.popen( command_rename )  # this should be an atomic operation thread-safe and multiprocess-safe

    def fromFile(self, filename):
        if not os.path.exists(filename):
                errmeg = "Cannot open file [" + filename + "] for reading. File is not there."
                raise RuntimeError, errmeg
        file = open(filename, "r")
        self.doc = xml.dom.minidom.parse( filename )

        element = self.doc.getElementsByTagName( self._rootname )
        if len(element) == 0:
            errmsg="Cannot find root node with name '" + self._rootname +"' in the xml document ["+filename+"]"
            raise RuntimeError, errmsg

        ## assing to self.root the actual node in memory loaded from the file
        self.root = element[0]
        self.init = True


if __name__=="__main__":
    c = XmlFramework()
    """
    c.initialize("Mattia Cinquilli", "taskprova")
    ev = Event()
    infoEvento = { \
                   "date":   "ieri", \
                   "ev":     "prosdfdsva", \
                   "txt":    "Prima prova xml", \
                   "reason": "develop logging info", \
                   "code":   "00", \
                   "time":   "0.0", \
                   "author": "pippo" \
                 }

    ev.initialize(infoEvento)
    c.addNode(ev)
    c.addNode(ev)
    c.addNode(ev)
    c.printMe()
    c.toFile('test.xml')
    """
    c.fromFile('test.xml')
    ev = Event()
    infoEvento = { \
                   "date":   "ieri", \
                   "ev":     "prova", \
                   "txt":    "Prima prova xml", \
                   "reason": "develop logging info", \
                   "code":   "00", \
                   "time":   "0.0", \
                   "author": "pipposfdsfds" \
                 }

    ev.initialize(infoEvento)
    c.addNode(ev)
    c.printMe()
    c.toXml()
    c.toFile('test.xml')

