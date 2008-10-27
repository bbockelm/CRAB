from IMProv.IMProvAddEntry import IMProvAddEntry, Event
from IMProv.IMProvNode import IMProvNode
from types import StringType

class InternalLoggingInfo:

   def __init__(self, filepath, rootname = "InternalLogInfo", entrname = "Event"):
       self.rootname = rootname
       self.entrname = entrname
       self.filepath = filepath

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

       result = IMProvNode( self.rootname )
       report = IMProvNode( self.entrname, None, **dictions)

       result.addNode(report)

       outfile = file( self.filepath, 'w').write(str(result))

