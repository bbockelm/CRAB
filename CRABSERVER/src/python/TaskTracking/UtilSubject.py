import os
import logging

class UtilSubject:

    def __init__(self, dropBox, taskN):
        self.location = dropBox
        self.taskName = taskN
        self.share = "share/"
        self.userSubj = "userSubj"
        self.path = self.location + "/" + self.taskName + "/" + self.share + self.userSubj
        self.userName = ""

        pass

    def parseUserSubj( self, text ):
        org = ""
        name = ""
        found = 0
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
                        if len(tag) > 1 and not found:
                            name = tag[1]
                            found = 1
        return org, name

    def invert(self, invertable_object):
        try:
            return invertable_object[::-1]
        except:
            print 'Object not invertable'
        return 1

    def getInfos(self):
        """
        /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli/CN=proxy
        """
        org = "NotSpecified"
        name = "Unknown"
        if os.path.exists( self.path ):
            org, name = self.parseUserSubj( open(self.path).read() )
            ##return org, name
            return self.getOriginalTaskName(org, name)
        return org, name

    def getOriginalTaskName(self, org, name):
        newName = self.taskName
        newName = newName.split("_",1)[1]
        org = self.invert(org)
        self.userName = name
        name = name.replace(" ", "_")
        name = self.invert(name)
        newName = self.invert(newName)
        newName = newName.split(org,1)[1]
        newName = newName.split(name,1)[1]
        newName = newName[1:]
        return self.invert(newName), self.userName

if __name__=="__main__":

    obj = UtilSubject("/data/SEDir", "crab_crab_0_070209_143745_Mattia_Cinquilli_INFN_1171028265")
    par1, par2 = obj.getInfos()
    print obj.getOriginalTaskName(par1, par2)
