from IMProv.IMProvAddEntry import IMProvAddEntry, Event
from IMProv.IMProvNode import IMProvNode
from types import StringType

class InternalLoggingInfo:

   def __init__(self, filepath, rootname = "InternalLogInfo", entrname = "Event"):
       self.rootname = rootname
       self.entrname = entrname
       self.filepath = filepath
       self.keystag  = "Keys"

   def loadXml(self):
       """
       _loadXml
       """
       ## load file and xml
       loadxml = IMProvAddEntry( self.rootname )
       loadxml.fromFile( self.filepath )
       return loadxml

   def loadEventPkl(self, pkl):
       """
       _loadFromPkl_

       load an entry from a pickle file
       """
       eve = Event( self.entrname )
       ## loading from pickle
       import pickle
       output = open(pkl, 'r')
       eve = pickle.load(output)
       output.close()
       try:
           import os
           os.remove(pkl)
       except Exception, ex:
           os.system("rm -f %s"%str(pkl) )
           pass
       return eve

   def toFile(self, xmlobj):
       """
       _toFile_
       """
       xmlobj.toXml()
       xmlobj.toFile( self.filepath )


   def addEntry(self, values):
       """
       _addEntry_

       add an entry to the xml file
       """
       ## load file and xml
       loadxml = self.loadXml()

       ## prepare event
       eve = None
       if type(values) == StringType:
           ## load event from pickle file
           eve = self.loadEventPkl( values )
       else:
           ## create the event
           eve = Event( self.entrname )
           eve.initialize( values )

       ## updating keys of event entries
       keytag = eve.fields.keys()
       oldtag = loadxml.getFirstElementOf("Keys")
       oldkeytag = eval(oldtag.getAttribute("tags"))
       updtag = oldkeytag
       for i in keytag:
           if i not in updtag:
               updtag.append(i)
       try:
           loadxml.replaceEntry(loadxml.getFirstElementOf("Keys"), "Keys", {"tags" : str(updtag)})
       except Exception, exc:
           import logging, traceback
           logging.info(str(exc))
           logging.error( str(traceback.format_exc()) ) 
       ## add the event
       loadxml.addNode( eve )

       ## update the xml
       self.toFile( loadxml )


   def createLogger(self, values):
       """
       _createLogger_

       create the xml file with first entry
       """
       dictions = {}
       if type(values) == StringType: 
           eve = self.loadEventPkl(values)
           dictions = eve.fields
       else:
           dictions = values
       keytag = {"tags" : str(dictions.keys())}

       result = IMProvNode( self.rootname )
       key4report = IMProvNode( self.keystag, None, **keytag )
       report = IMProvNode( self.entrname, None, **dictions)

       result.addNode(key4report)
       result.addNode(report)

       outfile = file( self.filepath, 'w').write(str(result))

