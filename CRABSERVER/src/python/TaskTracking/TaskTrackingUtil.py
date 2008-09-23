import os
import commands
import logging
import re

class TaskTrackingUtil:

    def __init__(self, auth):
        self.allow_anonymous = str(auth)

    def convertStatus( self, status ):
        """
        _convertStatus_
        U  : undefined
        C  : created
        S  : submitted
        SR : enqueued by the scheduler
        R  : running
        A  : Aborted
        D  : Done
        K  : killed
        E  : erased from the scheduler queue (also disappeared...)
        DA : finished but with some failures (aka Done Failed in GLite or Held for condor)
        UE : user ended (retrieved by th user)
        """
        stateConverting = { \
                    'R': 'Running', 'SD': 'Done', 'DA': 'Done (Failed)', \
                    'E': 'Done', 'SR': 'Ready', 'A': 'Aborted', \
                    'SS': 'Scheduled', 'U': 'Unknown', 'SW': 'Waiting', \
                    'K': 'Killed', 'S': 'Submitted', 'SU': 'Submitted', \
                    'NotSubmitted': 'NotSubmitted', 'C': 'Created', \
                    'UE': 'Cleared'
                          }
        if status in stateConverting:
            return stateConverting[status]
        return 'Unknown'

    def getNameFromProxy(self, path):
        """
        _getNameFromProxy_
        """

        cmd="voms-proxy-info -file "+path+" -subject"
        if self.allow_anonymous != "1" and \
           os.path.exists(path) == True:
            import commands
            return commands.getstatusoutput(cmd)
        else:
            raise("Path not found or anonymous enabled")

    def cnSplitter(self, proxy):
        """
        _cnSplitter_
        """
        tmp = string.split(str(proxy[1]),'/')
        cn=[]
        for t in tmp:
            if t[:2]== 'CN':
                if t[3:] != 'proxy':
                    cn.append(t[3:])
        return cn

    def invert(self, invertable_object):
        try:
            return invertable_object[::-1]
        except Exception, e:
            logging.error('Object not invertable: ' + str(e))
        return invertable_object

    def getOriginalTaskName( self, taskname, uuid ):
        """
        _getOriginalTaskName_

        get original task name
        from: user_taskname_uuid
        to:   taskname
        """
        newName = taskname
        newName = newName.split("_",1)[1]
        if uuid != '' and uuid != None:
            newName = newName.split(uuid,1)[0]
            newName = newName[:len(newName)-1]
        else:
            newName = self.invert( self.invert(newName).split("_",1)[1])
        return newName

    def getListEl(self, lista, el):
        try:
            return str(lista[el])
        except Exception, ex:
            logging.debug(" problems reading info.")
            return None

    def getMoreMails ( self, eMail ):
        """
        _getMoreMails_

        prepares a list of eMails from str "eMail"
        """

        eMaiList2 = []
        if eMail != None:
            eMaiList = eMail.split(";")
            for index in xrange(len(eMaiList)):
                temp = eMaiList[index].replace(" ", "")
                if self.checkEmail( temp ):
                    eMaiList2.append( temp )

        return eMaiList2


    def checkEmail ( self, eMail ):
        """
        _checkEmail_
        
        check the eMail with a regular expression
        """

        reg = re.compile('^[\w\.-_]+@(?:[\w-]+\.)+[\w]{2,4}$', re.IGNORECASE)
        if not reg.match( eMail ):
            errmsg = "Error parsing e-mail address; address ["+eMail+"] has "
            errmsg += "an invalid format;"
            logging.debug("WARNING: " + errmsg)
            logging.debug("         this e-mail address will be ignored.")
            return False
        return True

