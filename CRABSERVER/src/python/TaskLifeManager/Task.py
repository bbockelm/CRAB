class Task:

    def __init__(
                  self, taskName, owner, mail, lifetime, endedtime,\
                  heretime = None, size = -1, notified = False\
                ):
        ## unique task name
        self.__taskName  = taskName
        ## name-surname of the user
        self.__owner     = owner
        ## size of the task (subdirs included)
        self.__size      = size
        ## time to stay after ended
        self.__lifetime  = lifetime
        ## time when ended
        self.__endedtime = int(endedtime)
        ## time when arrived
        if heretime == None:
            self.__heretime  = self.__getTime()
        else:
            self.__heretime  = heretime
        ## user e-mail address
        self.__mail      = mail
        ## advice e-mail sent or not
        self.__notified = notified
        ### NEW ###
        ## proxy
        import os
        self.__proxy = os.path.join(self.__taskName + '_spec', 'userProxy')
        ## list of files to delete
        self.__sandboxes = []

    #########################
    ##  utility functions  ##
    #########################

    def __getTime(self):
        import time
        return int(str(time.time()).split(".")[0])

    #########################
    ## read-only functions ##
    #########################

    def getAll(self):
        return {\
                 "taskName":  self.__taskName, \
                 "owner":     self.__owner, \
                 "mail":      self.__mail, \
                 "size":      self.__size, \
                 "lifetime":  self.__lifetime, \
                 "endedtime": self.__endedtime, \
                 "heretime":  self.__heretime, \
                 "notified":  self.__notified, \
                 "proxy":     self.__proxy,\
                 "sandboxes": self.__sandboxes,\
               }

    def getName(self):
        return self.__taskName

    def getOwner(self):
        return self.__owner

    def getSize(self):
        return self.__size

    def getLife(self):
        return self.__lifetime

    def getEnded(self):
        return self.__getTime() - self.__endedtime

    def getLived(self):
        return self.__getTime() - self.__heretime

    def toLive(self):
        if self.__endedtime > 0:
            return ( self.getLife() - self.getEnded() )
        return 10000000000

    def getOwnerMail(self):
        return self.__mail

    def getNotified(self):
        return self.__notified

    def getProxy(self):
        return self.__proxy

    def getSBList(self):
        return self.__sandboxes

    ##########################
    ## read-write functions ##
    ##########################

    def updateSize(self, size):
        self.__size = size

    def updateEndedTime(self, time = None ):
        if time is None:
            self.__endedtime = self.__getTime()
        else:
            self.__endedtime = time

    def updateLifeTime(self, time):
        self.__lifetime = time

    def notify(self, booleo = True):
        self.__notified = booleo

    def addSBFile(self, path):
        self.__sandboxes.append(str(path))

    def flushSBFile(self, path):
        if str(path) in self.__sandboxes:
            self.__sandboxes.remove(str(path))
    
    def addSB(self, listsb):
        for sb in listsb:
            if sb not in self.__sandboxes:
                self.__sandboxes.append(sb)

    def delSB(self, listsb):
        for sb in listsb:
            if sb in self.__sandboxes:
                self.__sandboxes.remove(sb)
