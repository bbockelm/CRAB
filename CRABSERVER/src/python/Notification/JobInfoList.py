
#import mutex
import threading

class JobInfoList:
    def __init__(self):
        self.jobinfolist = []
        self.mtx = threading.Semaphore(1)
        pass

    def pushJob(self,jobinfo):
        self.jobinfolist.append( jobinfo )

    def getJobList(self):
        return self.jobinfolist
    
#    def getJobInfoList(self):
#        toReturn = []
#        for item in self.jobinfolist.keys():
#            toReturn.append( self.jobinfolist[item] )
#        return toReturn
 
    def getJobIDList(self):
    	toReturn = []
	for item in self.jobinfolist:
		toReturn.append( item.getJobID() )
	return toReturn
    
    def clearList(self):
        self.jobinfolist =[]

#    def getJobInfo(self,jobid):
#        return self.jobinfolist[jobid]

    def lock(self):
        self.mtx.acquire()

    def unlock(self):
        self.mtx.release()

    def removeJobs( self, list ):
        for job in list:
	    if job in self.jobinfolist:
            	self.jobinfolist.remove( job )
        
#    def printJobs(self):
#        if len(self.jobinfolist.keys()) == 0:
#            pass
#            print "No jobs to notify"
#        else:
#            for job in self.jobinfolist.keys():
#                print "[%s] -> %s" % (job, self.jobinfolist[job].getOwner())
