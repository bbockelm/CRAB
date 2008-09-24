import os
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
        stateconverting = { \
                    'R': 'Running', 'SD': 'Done', 'DA': 'Done (Failed)', \
                    'E': 'Done', 'SR': 'Ready', 'A': 'Aborted', \
                    'SS': 'Scheduled', 'U': 'Unknown', 'SW': 'Waiting', \
                    'K': 'Killed', 'S': 'Submitted', 'SU': 'Submitted', \
                    'NotSubmitted': 'NotSubmitted', 'C': 'Created', \
                    'UE': 'Cleared'
                          }
        if status in stateconverting:
            return stateconverting[status]
        return 'Unknown'

    def getNameFromProxy(self, path):
        """
        _getNameFromProxy_
        """

        cmd = "voms-proxy-info -file "+path+" -subject"
        if self.allow_anonymous != "1" and \
           os.path.exists(path) == True:
            import commands
            return commands.getstatusoutput(cmd)
        else:
            raise Exception("Path not found or anonymous enabled")

    def cnSplitter(self, proxy):
        """
        _cnSplitter_
        """
        tmp = str(proxy[1]).split('/')
        cnproxy = []
        for field in tmp:
            if field[:2] == 'CN':
                if field[3:] != 'proxy':
                    cnproxy.append(field[3:])
        return cnproxy

    def invert(self, invertable_object):
        try:
            return invertable_object[::-1]
        except Exception, exc:
            logging.error('Object not invertable: ' + str(exc))
        return invertable_object

    def getOriginalTaskName( self, taskname, uuid ):
        """
        _getOriginalTaskName_

        get original task name
        from: user_taskname_uuid
        to:   taskname
        """
        newname = taskname
        newname = newname.split("_", 1)[1]
        if uuid != '' and uuid != None:
            newname = newname.split(uuid, 1)[0]
            newname = newname[:len(newname)-1]
        else:
            newname = self.invert( self.invert(newname).split("_", 1)[1])
        return newname

    def getListEl(self, lista, elem):
        """
        _getListEl_
        """
        try:
            return str(lista[elem])
        except Exception, ex:
            logging.debug(" problems reading info "+str(ex))
            return None

    def getMoreMails ( self, email ):
        """
        _getMoreMails_

        prepares a list of eMails from str "email"
        """
        emailist2 = []
        if email != None:
            emailist = email.split(";")
            for index in xrange(len(emailist)):
                temp = emailist[index].replace(" ", "")
                if self.checkEmail( temp ):
                    emailist2.append( temp )
        return emailist2


    def checkEmail ( self, email ):
        """
        _checkEmail_
        
        check the email with a regular expression
        """

        reg = re.compile('^[\w\.-_]+@(?:[\w-]+\.)+[\w]{2,4}$', re.IGNORECASE)
        if not reg.match( email ):
            errmsg = "Error parsing e-mail address; address ["+email+"] has "
            errmsg += "an invalid format;"
            logging.debug("WARNING: " + errmsg)
            logging.debug("         this e-mail address will be ignored.")
            return False
        return True

