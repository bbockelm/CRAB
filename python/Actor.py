#
#  This is an abstract class for all CRAB actions.
#

from crab_exceptions import *

class Actor:
    def run(self):
        raise CrabException, "Actor::run() must be implemented"
    
