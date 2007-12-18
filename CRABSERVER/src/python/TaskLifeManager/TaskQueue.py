class TaskQueue:

    def __init__(self, queueName = None):
        self.__queueList = []
        self.__queueName = queueName
        self.__index = 0

###########
# PRIVATE #
###########
    def __add(self, element):
        if element not in self.__queueList:
            #for el in self.__queueList:
            #    if el.getName() != element.getName():
            self.__queueList.append(element)

    def __delete(self, element):
        if element in self.__queueList:
            item_number = self.__queueList.index(element)
            del self.__queueList[item_number]
            self.__resetIndex()

    def __resetIndex(self):
        try:
            obj = self.__queueList[self.__index]
        except IndexError:
            if self.__index > 0:
                self.__index -= 1
                self.__resetIndex()
        return 

    def __getByObject(self, element):
        if element in self.__queueList:
            item_number = self.__queueList.index(element)
            return self.__queueList[item_number]
        return None

    def __getByName(self, name):
        for element in self.__queueList:
            if name == element.getName():
                return self.__getByObject(element)
        return None

    def __incrId(self):
        if self.getHowMany() > (self.__index + 1):
            self.__index += 1
        else:
            self.__index = 0
        return self.__index

    def __decrId(self):
        if self.__index > 0:
            self.__index -= 1
        else:
            self.__index = 0
        return self.__index

##########
# PUBLIC #
##########
    def exists(self, name):
        if self.__getByName(name) == None:
            return False
        if self.__getByName(name.split(".")[0]) == None:
            return False
        return True

    def insert(self, element):
        self.__add(element)

    def remove(self, element):
        self.__delete(element)

    def getbyName(self, name):
        return self.__getByName(name)

    def getbyObject(self, element):
        return self.__getByObject(element)

    def getCurrent(self):
        return self.__queueList[self.__index]

    def getCurrentSwitch(self):
        obj = self.__queueList[self.__index]
        self.__incrId()
        return obj

    def getNext(self):
        return self.__queueList[self.__incrId()]

    def getPrevious(self):
        return self.__queueList[self.__decrId()]

    def getAll(self):
        diction = {}
        indexer = 0
        for task in self.__queueList:
            diction.setdefault( indexer, task.getAll() )
            indexer += 1
        return diction
 
    def getHowMany(self):
        return len(self.__queueList)

