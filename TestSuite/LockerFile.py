import os, fcntl

class LockerFile:

    LOCK_EX = fcntl.LOCK_EX  # exclusive lock of default
    LOCK_SH = fcntl.LOCK_SH  # shared lock specified with the parameter "flags"
    LOCK_NB = fcntl.LOCK_NB  # don't block when blocking
    LOCK_UN = fcntl.LOCK_UN

    def __init__(self):
	return

    def lock_F( self, file, flags ):
	
	LOCK = self.LOCK_EX

	if flags == 1:
	    LOCK = self.LOCK_SH
	 
	fcntl.flock(file.fileno(), LOCK)

    def unlock_F( self, file ):
	fcntl.flock(file.fileno(), self.LOCK_UN)
