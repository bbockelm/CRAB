#!/usr/bin/env python

import os
import xml.dom.minidom
import xml.dom.ext

document = """\
<slideshow>
  <title attrib="new">Demo slideshow</title>
  <slide>
    <title>Slide title</title>
    <point>This is a demo</point>
    <point>Of a program for processing slides</point>
  </slide>
</slideshow>
"""
path = os.getcwd()
#fh = open(path+"/pippo.xml", "r")
#doc = xml.dom.minidom.parse(path+"/ProdAgentConfig.xml")
#doc = xml.dom.minidom.parse(path+"/xmlReportFile.xml")
doc = xml.dom.minidom.parse("pippo.xml")
#doc = xml.dom.minidom.parseString(document)
#fh.close()
dict = {}
for node in doc.documentElement.childNodes:
    if node.attributes:
#        dict = [ t for t in node.attributes.item ]
#        print "Task: ", dict['Task']
        for i in range(node.attributes.length):
            a = node.attributes.item(i)
            dict[a.name] = a.value
            print "%s = %s" % (a.name, a.value)
            task = dict['Task']
            print "Task: ", task
#nodes = doc.documentElement.childNodes
#print str(nodes)
