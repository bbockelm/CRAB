import os
import sys

class InternalLoggingInfo:

    __logFile__ = "loggingTaskInfo"

    def __thereIs__( self, path ):
        return os.path.exists( path )

    def __writeFile__( self, filePath, message ):
        """
        __writeFile__
        write on the log file
        """
        f = None
        if self.__thereIs__( filePath ):
            f = open(filePath, 'a')
        else:
            try:
                f = open(filePath, 'w')
            except:
                return
        import fcntl
        fcntl.flock( f.fileno(), fcntl.LOCK_EX )
        ### start zona protetta ###
        try:
            f.write( str(message) + "\n" )
        finally:
            ### end zona protetta ###
            fcntl.flock(  f.fileno(), fcntl.LOCK_UN )
            f.close()


    def appendLoggingInfo( self, path, mess ):
        return self.__writeFile__( os.path.join( path, self.__logFile__ ), mess )

