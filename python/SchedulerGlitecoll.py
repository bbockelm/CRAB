#from Scheduler import Scheduler
from SchedulerGlite import SchedulerGlite

class SchedulerGlitecoll(SchedulerGlite):
    def __init__(self):
        SchedulerGlite.__init__(self)

    def tOut(self, list):
        if list != None:
            return len(list[1])*60
        else:
            return 180

