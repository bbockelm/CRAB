from Actor import *

class Submitter(Actor):
    def __init__(self, cfg_params, nsjobs):
        self.cfg_params = cfg_params
        self.njobs = nsjobs
        return
    
    def run(self):
        print "I am submitting %d jobs" % self.njobs
        return
    
