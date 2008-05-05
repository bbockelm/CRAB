import os
import logging

class UtilSubject:

    def __init__(self, dropBox, taskN, uid):
        self.location = dropBox
        self.taskName = taskN
        self.share = "share/"
        self.userSubj = "userSubj"
        self.path = self.location + "/" + self.taskName + "/" + self.share + self.userSubj
        self.userName = ""
	self.uuid = uid

        pass

    def parseUserSubj( self, text ):
        org = ""
        name = ""
        for line in text.split("\n"):
            line = line.strip()
            for line2 in line.split("/"):
                line2 = line2.strip()
                tag = line2.split("=")
                if len(tag) > 0:
                    if tag[0] == "O":
                        if len(tag) > 1:
                            org = tag[1]
                    elif tag[0] == "CN":
		        #logging.info(" " + str(tag))
                        if len(tag) > 1:
			    if tag[1] != "proxy":
                                name = tag[1]
        return org, name

    def invert(self, invertable_object):
        try:
            return invertable_object[::-1]
        except Exception, e:
            logging.error('Object not invertable: ' + str(e))
        return 1

    def getInfos(self):
        """
        /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli/CN=proxy
        """
        tName = self.taskName
        if self.uuid == "":
           tName = tName.split("_",1)[1]
           tName = self.invert( self.invert(tName).split("_",1)[1] )
        name = "Unknown"
        if os.path.exists( self.path ):
            org, self.userName = self.parseUserSubj( open(self.path).read() )
	    return self.getOriginalTaskName2(), self.userName
        else:
           name = self.taskName.split("_",1)[0]
        return tName, name

    def getOriginalTaskName2( self ):
        newName = self.taskName
        newName = newName.split("_",1)[1]
	if self.uuid != '' and self.uuid != None:
  	    newName = newName.split(self.uuid,1)[0]
	else:
	    newName = newName.split("_",1)[1]
        newName = newName[:len(newName)-1]
	return newName
	
if __name__=="__main__":

    obj = UtilSubject("/flatfiles/cms", "crab_crab_0_070410_185513_fb9f93a1-fad4-4f14-9bc5-dc83de7eceb2",  "fb9f93a1-fad4-4f14-9bc5-dc83de7eceb2")
    par1, par2 = obj.getInfos()
    print str(par1), str(par2)
