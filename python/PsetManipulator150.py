from PsetManipulator import *

class PsetManipulator(PsetManipulator) :
    def __init__(self, pset):
        """
        Convert Pset in Python format  and initialize
        To be used with CMSSW version >150
        """

        self.pset = pset
        #convert Pset
        self.pyPset = os.path.basename(pset)  
        # Check wether edmConfigToPython or EdmConfigToPython is the right command

        cmdEdmToPy = ''
        cmdUp = 'which EdmConfigToPython > /dev/null 2>&1'
        cmdLow = 'which edmConfigToPython > /dev/null 2>&1'
        if os.system(cmdUp) == 0:
            cmdEdmToPy = 'EdmConfigToPython'
        elif os.system(cmdLow) == 0:
            cmdEdmToPy = 'edmConfigToPython'
        else:
            msg = 'Could not find EdmConfigToPython nor edmConfigToPython in the path\m'
            raise CrabException(msg)
            
        cmd = cmdEdmToPy+' > '+common.work_space.shareDir()+self.pyPset+'py < '+ self.pset
        exit_code = os.system(cmd)
        if exit_code != 0 : 
            msg = 'Could not convert '+self.pset+' into a python Dictionary \n'
            msg += 'Failed to execute \n'+cmd+'\n'
            msg += 'Exit code : '+str(exit_code)

            raise CrabException(msg)
            pass
        
        self.par = file(common.work_space.shareDir()+self.pyPset+'py').read()

        # get PSet
        self.cfg = CfgInterface(self.par,True)

    def maxEvent(self, maxEv):
        """ Set max event in the standalone untracked module """ 
        self.cfg.hackMaxEvents(maxEv)
        return
