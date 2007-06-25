#from Scheduler import Scheduler
from SchedulerGlite import SchedulerGlite

class SchedulerGlitecoll(SchedulerGlite):
    def __init__(self):
        SchedulerGlite.__init__(self)

    def submitTout(self, list):
        return len(list[1])*20


