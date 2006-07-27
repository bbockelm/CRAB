#!/usr/bin/env python2.2
from InteractCrab import InteractCrab
import sys, os

class RoboCrab:

    nCreate = -1
    nSubmit = -1
    set = 1
    ok = 0
    debug = 0
    wait = 60
    cfgFileName = "crab.cfg"

    def __init__(self):
        typeRunning = sys.argv[1:]
        self.checkPar(typeRunning)

    def printMan(self):
        print ""
        print "Sample to lanch TestSuite:"
        print "\t $TestSuite [n] [m] [-debug N] [-wait s] [-cfg FILE]\n"
        print "\t Options:\n"
        print "\t  n m:         n = number of jobs to create"
        print "\t               m = number of jobs to submit"
        print ""
        print "\t  -debug N:    N = positive number that indicates the level of debug"
        print "\t                    0 -> no debug"
        print "\t                    1 -> shows the commands launched from the TestSuite"
        print ""
        print "\t  -wait s:     s = positive number that indicates the number of seconds to" 
        print "\t                   wait between cycles; the default value is 60"
        print ""
        print "\t  -cfg FILE:   FILE = name of the file containing the configurations for"
        print "\t                      running Crab (e.g. crab.cfg)\n"
        return

    def optTest1(self, par1, par2):

       try:

           self.nCreate = int(par1)
           #print par1
           self.nSubmit = int(par2)
           #print par2
           self.set = 1
           self.ok = 1
       except ValueError:
           msg = 'TestSuite: "Error: arguments related to the number of jobs to create and to submit must be numbers"'
           print msg
           self.printMan()
           sys.exit(1)

    def optDebug(self, nDebug):
       try:
           self.debug = int( nDebug )
       except ValueError:
           msg = 'TestSuite: Error -debug argument must be positive number'
           print msg
           self.printMan()
           sys.exit(1)

    def optWait(self, lWait):
       try:
           self.wait = int( lWait )
       except ValueError:
           msg = 'TestSuite: Error -wait argument must be positive number'
           print msg
           self.printMan()
           sys.exit(1)

    def optConfig(self, cfgFile):
       try:
           self.cfgFileName = cfgFile
       except:
           msg = 'TestSuite: Error in -cfg argument options'
           print msg
           self.printMan()
           sys.exit(1)

    def checkPar(self, parameters):

        nPar = len(parameters)

        if nPar == 2:
           opt = parameters[0]
           if opt == "-debug":
               self.optDebug( parameters[1] )
           elif opt == "-wait":                                # #
               self.optWait( parameters[1] )
           elif opt == "-cfg":
               self.optConfig( parameters[1] )
           else:
               self.optTest1(parameters[0], parameters[1])
           

        elif nPar == 4:
           opt = parameters[0]
           #if opt == "-test1":
           if opt == "-debug":
               self.optDebug( parameters[1] )
               #if parameters[2] == "-test1":
               if parameters[2] == "-wait":
                   self.optWait( parameters[3] )
               elif parameters[2] == "-cfg":
                   self.optConfig( parameters[3] )
               else:
                   self.optTest1(parameters[2], parameters[3])
           elif opt == "-wait":                                # #
               self.optWait( parameters[1] )                   # WAIT
               #if parameters[2] == "-test1":                   #
               if parameters[2] == "-debug":
                   self.optDebug( parameters[3] )
               elif parameters[2] == "-cfg":
                   self.optConfig( parameters[3] )
               else:
                   self.optTest1(parameters[2], parameters[3])
           elif opt == "-cfg":
               self.optConfig( parameters[1] )
               #if parameters[2] == "-test1":
               if parameters[2] == "-debug":
                   self.optDebug( parameters[3] )
               elif parameters[2] == "-wait":
                   self.optWait( parameters[3] )
               else:
                   self.optTest1(parameters[2], parameters[3])
           else:
               self.optTest1(parameters[0], parameters[1])
               if parameters[2] == "-debug":
                   self.optDebug( parameters[3] )
               elif parameters[2] == "-wait":                  # #
                   self.optWait( parameters[3] )               # WAIT
               elif  parameters[2] == "-cfg":                 # #
                   self.optConfig( parameters[3] )


        elif nPar == 6:                                        # #
           opt = parameters[0]                                 #
#           if opt == "-test1":                                 #
           if opt == "-debug":                               #
               self.optDebug( parameters[1] )                  #
               ##if parameters[2] == "-test1":                   #
               if parameters[2] == "-wait":                  
                   self.optWait( parameters[3] )               
                   ##if parameters[4] == "-test1":
                   if parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
                   else:
                       self.optTest1(parameters[4], parameters[5])
               elif parameters[2] == "-cfg":
                   self.optConfig( parameters[3] )
                   ##if parameters[4] == "-test1":
                   if parameters[4] == "-wait":
                       self.optWait( parameters[5] )
                   else:
                       self.optTest1(parameters[4], parameters[5])
               else:
                   self.optTest1(parameters[2], parameters[3]) #
                   if parameters[4] == "-wait":                #
                       self.optWait( parameters[5] )           #
                   elif parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
           elif opt == "-wait":                                #
               self.optWait( parameters[1] )                   #
               #if parameters[2] == "-test1":                   #
               if parameters[2] == "-debug":
                   self.optDebug( parameters[3] )
                   #if parameters[4] == "-test1":               #
                   if parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
                   else:
                       self.optTest1(parameters[4], parameters[5])
               elif parameters[2] == "-cfg":
                   self.optConfig( parameters[3] )
                   #if parameters[4] == "-test1":
                   if parameters[4] == "-debug":
                       self.optDebug( parameters[5] )
                   else:
                       self.optTest1(parameters[4], parameters[5])
               else:
                   self.optTest1(parameters[2], parameters[3]) # WAIT
                   if parameters[4] == "-debug":               #
                       self.optDebug( parameters[5] )          #
                   elif parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
           elif opt == "-cfg":                                #
               self.optWait( parameters[1] )                   #
               #if parameters[2] == "-test1":                   #
               if parameters[2] == "-debug":                 #
                   self.optDebug( parameters[3] )              #
                   #if parameters[4] == "-test1":               #
                   if parameters[4] == "-wait":                #
                       self.optWait( parameters[5] )
                   else:
                       self.optTest1(parameters[4], parameters[5])
               elif parameters[2] == "-wait":
                   self.optWait( parameters[3] )
                   #if parameters[4] == "-test1":
                   if parameters[4] == "-debug":
                       self.optDebug( parameters[5] )
                   else:
                       self.optTest1(parameters[4], parameters[5])
               else:
                   self.optTest1(parameters[2], parameters[3]) # WAIT
                   if parameters[4] == "-debug":               #
                       self.optDebug( parameters[5] )          #
                   elif parameters[4] == "-wait":
                       self.optWait( parameters[5] )
           else:
               self.optTest1(parameters[0], parameters[1])
               if parameters[2] == "-debug":
                   self.optDebug( parameters[3] )              #
                   if parameters[4] == "-wait":                #
                       self.optWait( parameters[5] )           # WAIT
                   elif parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
               elif parameters[2] == "-wait":                  #
                   self.optWait( parameters[3] )               #
                   if parameters[4] == "-debug":               #
                       self.optDebug( parameters[5] )          #
                   elif parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
               elif parameters[2] == "-cfg":
                   self.optConfig( parameters[3] )
                   if parameters[4] == "-wait":
                       self.optWait( parameters[5] )
                   elif parameters[4] == "-debug":
                       self.optDebug( parameters[5] )


        elif nPar == 8:
           opt = parameters[0]                                 #
           #if opt == "-test1":                                 #
           if opt == "-debug":                               
               self.optDebug( parameters[1] )                  
               #if parameters[2] == "-test1":                   
               if parameters[2] == "-wait":
                   self.optWait( parameters[3] )               
                   #if parameters[4] == "-test1":               
                   if parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
                       #if parameters[6] == "-test1":
                       self.optTest1(parameters[6],parameters[7])
                   else:
                       self.optTest1(parameters[4],parameters[5])
                       if parameters[6] == "-cfg":
                           self.optConfig( parameters[7] )
               elif parameters[2] == "-cfg":
                   self.optConfig( parameters[3] )
                   #if parameters[4] == "-test1":
                   if parameters[4] == "-wait":                #
                       self.optWait( parameters[5] )           #
                       #if parameters[6] == "-test1":
                       self.optTest1(parameters[6],parameters[7])
                   else:
                       self.optTest1(parameters[4],parameters[5])
                       if parameters[6] == "-wait":
                           self.optWait( parameters[7] )
               else:
                   self.optTest1(parameters[2], parameters[3]) #
                   if parameters[4] == "-wait":                #
                       self.optWait( parameters[5] )           #
                       if parameters[6] == "-cfg":
                           self.optConfig( parameters[7] )
                   elif parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
                       if parameters[6] == "-wait":
                           self.optWait( parameters[7] )

           elif opt == "-wait":                                #
               self.optWait( parameters[1] )                   #
               #if parameters[2] == "-test1":                   #
               if parameters[2] == "-debug":                 
                   self.optDebug( parameters[3] )              
                   #if parameters[4] == "-test1":              
                   if parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
                       #if parameters[6] == "-test1":
                       self.optTest1(parameters[6],parameters[7])
                   else:
                       self.optTest1(parameters[4],parameters[5])
                       if parameters[6] == "-cfg":
                           self.optConfig( parameters[7] )
               elif parameters[2] == "-cfg":
                   self.optConfig( parameters[3] )
                   #if parameters[4] == "-test1":
                   if parameters[4] == "-debug":               #
                       self.optDebug( parameters[5] )
                       #if parameters[6] == "-test1":
                       self.optTest1(parameters[6],parameters[7])
                   else:
                       self.optTest1(parameters[4],parameters[5])
                       if parameters[6] == "-debug":               #
                           self.optDebug( parameters[7] )
               else: 
                   self.optTest1(parameters[2], parameters[3]) # WAIT
                   if parameters[4] == "-debug":               #
                       self.optDebug( parameters[5] )          #
                       if parameters[6] == "-cfg":
                           self.optConfig( parameters[7] )
                   elif parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
                       if parameters[6] == "-wait":
                           self.optWait( parameters[7] )
           elif opt == "-cfg":                                #
               self.optWait( parameters[1] )                   #
               #if parameters[2] == "-test1":                   #
               if parameters[2] == "-debug":                 #
                   self.optDebug( parameters[3] )              #
                   #if parameters[4] == "-test1":               #
                   if parameters[4] == "-wait":
                       self.optWait( parameters[5] )
                       #if parameters[6] == "-test1":
                       self.optTest1(parameters[6],parameters[7])
                   else:
                       self.optTest1(parameters[4],parameters[5])
                       if parameters[6] == "-wait":
                           self.optWait( parameters[7] )
               elif parameters[2] == "-wait":
                   self.optWait( parameters[3] )
                   #if parameters[4] == "-test1":
                   if parameters[4] == "-debug":               #
                       self.optDebug( parameters[5] )
                       #if parameters[6] == "-test1":
                       self.optTest1(parameters[6],parameters[7])
                   else:
                       self.optTest1(parameters[4],parameters[5])
                       if parameters[6] == "-debug":               #
                           self.optDebug( parameters[7] )
               else:         
                   self.optTest1(parameters[2], parameters[3])
                   if parameters[4] == "-debug":
                       self.optDebug( parameters[5] )
                       if parameters[6] == "-wait":
                           self.optWait( parameters[7] )
                   elif parameters[4] == "-wait":
                       self.optWait( parameters[5] )
                       if parameters[6] == "-debug":          
                           self.optDebug( parameters[7] )
           else:
               self.optTest1(parameters[0], parameters[1])     #
               if parameters[2] == "-debug":                   #
                   self.optDebug( parameters[3] )              #
                   if parameters[4] == "-wait":                #
                       self.optWait( parameters[5] )           # WAIT
                       if parameters[6] == "-cfg":
                           self.optConfig( parameters[7] )
                   elif parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
                       if parameters[6] == "-wait":
                           self.optWait( parameters[7] )
               elif parameters[2] == "-wait":                  #
                   self.optWait( parameters[3] )               #
                   if parameters[4] == "-debug":               #
                       self.optDebug( parameters[5] )          #
                       if parameters[6] == "-cfg":
                           self.optConfig( parameters[7] )
                   elif parameters[4] == "-cfg":
                       self.optConfig( parameters[5] )
                       if parameters[6] == "-debug":               #
                           self.optDebug( parameters[7] )
               elif parameters[2] == "-cfg":
                   self.optConfig( parameters[3] )
                   if parameters[4] == "-wait":
                       self.optWait( parameters[5] )
                       if parameters[6] == "-debug":               #
                           self.optDebug( parameters[7] )
                   elif parameters[4] == "-debug":
                       self.optDebug( parameters[5] )
                       if parameters[6] == "-wait":
                           self.optWait( parameters[7] )

        else:
           print('TestSuite: "Error in the number of parameters"')   ## M.
           self.printMan()
           sys.exit(1)
           
        return

if __name__=='__main__':

    roboStart = RoboCrab()
    #print roboStart.set, roboStart.nCreate, roboStart.nSubmit
    if not roboStart.ok:
        roboStart.nCreate = 1
        roboStart.nSubmit = 1
        print '\n ****   TestSuite: "Warning: no number of jobs to create and to submit specified"   ****'
        #print "debug = ", roboStart.debug
        #print "wait = ", roboStart.wait
    crabber = InteractCrab(roboStart.set, roboStart.nCreate, roboStart.nSubmit, roboStart.debug, roboStart.wait, roboStart.cfgFileName)
    crabber.crabRunner()
#    else:
#        print('CrabRob: "Error in the paramaters"') #M.
#        roboStart.printMan()

    pass
