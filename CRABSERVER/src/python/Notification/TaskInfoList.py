
#import mutex
import threading

class TaskInfoList:

    def __init__(self):
        self.taskinfolist = []
        self.mtx = threading.Semaphore(1)

    def pushTask(self,task):
        self.taskinfolist.append( task )

    def getTaskList(self):
        return self.taskinfolist
 
    def getTaskNameList(self):
    	toReturn = []
	for item in self.taskinfolist:
		toReturn.append( item.getTaskName() )
	return toReturn
    
    def clearList(self):
        self.taskinfolist =[]

    def lock(self):
        self.mtx.acquire()

    def unlock(self):
        self.mtx.release()

    def removeTasks( self, list ):
        for task in list:
	    if task in self.taskinfolist:
            	self.taskinfolist.remove( task )
