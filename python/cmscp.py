#!/usr/bin/env python

import sys, getopt, string
import os, popen2
from ProdCommon.Storage.SEAPI.SElement import SElement, FullPath 
from ProdCommon.Storage.SEAPI.SBinterface import *



class cmscp:
    def __init__(self, argv):
        """
        cmscp

        safe copy of local file in current directory to remote SE via lcg_cp/srmcp, 
        including success checking  version also for CAF using rfcp command to copy the output to SE
        input:
           $1 middleware (CAF, LSF, LCG, OSG)
           $2 local file (the absolute path of output file or just the name if it's in top dir)
           $3 if needed: file name (the output file name)
           $5 remote SE (complete endpoint)
           $6 srm version 
        output:
             return 0 if all ok
             return 60307 if srmcp failed
             return 60303 if file already exists in the SE
        """
        #set default
        self.debug = 0
        self.source = ''
        self.destination = '' 
        self.file_to_copy = []
        self.remote_file_name = []
        self.protocol = ''
        self.middleware = ''
        self.srmv = ''
        self.lcgOpt='-b -D srmv2 --vo cms -t 2400 --verbose'  
        self.opt=''
        try:
            opts, args = getopt.getopt(argv, "", ["source=", "destination=", "inputFileList=", "outputFileList=", \
                                                  "protocol=", "middleware=", "srm_version=", "debug", "help"])
        except getopt.GetoptError:
            print self.usage()
            sys.exit(2)
 
        self.setAndCheck(opts)  
         
        return

    def setAndCheck( self, opts ): 
        """
        Set and check command line parameter
        """
        if not opts :
            print self.usage()
            sys.exit()
        for opt, arg in opts :
            if opt  == "--help" :
                print self.usage()
                sys.exit()
            elif opt == "--debug" :
                self.debug = 1
            elif opt == "--source" :
                self.source = arg
            elif opt == "--destination":
                self.destination = arg 
            elif opt == "--inputFileList":
                infile = arg
            elif opt == "--outputFileList":
                out_file
            elif opt == "--protocol":
                self.protocol = arg
            elif opt == "--middleware":
                self.middleware = arg
            elif opt == "--srm_version":
                self.srmv = arg
 
        # source and dest cannot be undefined at same time 
        if self.source == '' and self.destination == '':
            print self.usage()
            sys.exit()
        # if middleware is not defined --> protocol cannot be empty  
        if self.middleware == '' and self.protocol == '':
            print self.usage()
            sys.exit()
        # input file must be defined  
        if infile == '':
            print self.usage()
            sys.exit()
        else:
            if infile.find(','):
                [self.file_to_copy.append(x.strip()) for x in infile.split(',')]
            else: 
                self.file_to_copy.append(infile)
         
        ## TO DO:
        #### add check for outFiles
        #### add map {'inFileNAME':'outFileNAME'} to change out name

        return

    def run( self ):   
        """
        Check if running on UI (no $middleware) or 
        on WN (on the Grid), and take different action  
        """
        if self.middleware :  
           results = self.stager() 
        else:
           results = self.copy( self.file_to_copy, self.protocol , self.opt)

        self.finalReport(results,self.middleware) 

        return 
    
    def setProtocol( self ):    
        """
        define the allowed potocols based on $middlware
        which depend on scheduler 
        """
        if self.middleware.lower() in ['osg','lcg']:
            supported_protocol = {'srm-lcg': self.lcgOpt }#,
                               #   'srmv2' : '' }
        elif self.middleware.lower() in ['lsf','caf']:
            supported_protocol = {'rfio': '' }
        else:
            ## here we can add support for any kind of protocol, 
            ## maybe some local schedulers need something dedicated
            pass
        return supported_protocol

    def stager( self ):              
        """
        Implement the logic for remote stage out
        """
        protocols = self.setProtocol()  
        count=0 
        list_files = self.file_to_copy
        results={}   
        for prot in protocols.keys():
            if self.debug: print 'Trying stage out with %s utils \n'%prot 
            copy_results = self.copy( list_files, prot, protocols[prot] )
            list_retry = [] 
            list_existing = [] 
            list_ok = [] 
            for file, dict in copy_results.iteritems():
                er_code = dict['erCode']
                if er_code == '60307': list_retry.append( file )
                elif er_code == '60303': list_existing.append( file )
                else:
                    list_ok.append(file)
                    reason = 'Copy succedeed with %s utils'%prot
                    upDict = self.updateReport(file, er_code, reason)
                    copy_results.update(upDict) 
            results.update(copy_results)
            if len(list_ok) != 0:  
                msg = 'Copy of %s succedeed with %s utils\n'%(str(list_ok),prot)
               # print msg
            if len(list_ok) == len(list_files) : 
                break
            else:
         #       print 'Copy of files %s failed using %s...\n'%(str(list_retry)+str(list_existing),prot) 
                if len(list_retry): list_files = list_retry
                else: break 
            count =+1   
 
        #### TODO Daniele 
        #check is something fails and created related dict
  #      backup = self.analyzeResults(results) 
  
  #      if backup :  
  #          msg = 'WARNING: backup logic is under implementation\n'
  #          #backupDict = self.backup()
  #          ### NOTE: IT MUST RETURN a DICT contains also LFN and SE Name  
  #          results.update(backupDict)
  #          print msg
        return results

    def initializeApi(self, protocol ):
        """
        Instantiate storage interface  
        """
        source_prot = protocol 
        dest_prot = protocol 
        if self.source == '' : source_prot = 'local'
        Source_SE  = self.storageInterface( self.source, source_prot )
        if self.destination == '' : dest_prot = 'local'
        Destination_SE = self.storageInterface( self.destination, dest_prot )

        if self.debug :
            print '(source=%s,  protocol=%s)'%(self.source, source_prot) 
            print '(destination=%s,  protocol=%s)'%(self.destination, dest_prot) 

        return Source_SE, Destination_SE

    def copy( self, list_file, protocol, opt):
        """
        Make the real file copy using SE API 
        """
        if self.debug :
            print 'copy(): using %s protocol'%protocol 
        Source_SE, Destination_SE = self.initializeApi( protocol )

        # create remote dir 
        if protocol in ['gridftp','rfio']:
            self.createDir( Destination_SE, protocol )

        ## prepare for real copy  ##
        sbi = SBinterface( Source_SE, Destination_SE )
        sbi_dest = SBinterface(Destination_SE) 

        results = {}
        ## loop over the complete list of files 
        for filetocopy in list_file: 
            if self.debug : print 'start real copy for %s'%filetocopy
            ErCode, msg = self.checkFileExist( sbi_dest, os.path.basename(filetocopy) ) 
            if ErCode == '0': 
                ErCode, msg = self.makeCopy( sbi, filetocopy , opt)
            if self.debug : print 'Copy results for %s is %s'%( os.path.basename(filetocopy) ,ErCode)
            results.update( self.updateReport(filetocopy, ErCode, msg))
        return results
    
    def updateReport(self, file, erCode, reason, lfn='', se='' ):
        """ 
        Update the final stage out infos 
        """ 
        jobStageInfo={} 
        jobStageInfo['erCode']=erCode
        jobStageInfo['reason']=reason
        jobStageInfo['lfn']=lfn 
        jobStageInfo['se']=se 

        report = { file : jobStageInfo}
        return report

    def finalReport( self , results, middleware ):
        """
        It should return a clear list of LFNs for each SE where data are stored.
        allow "crab -copyLocal" or better "crab -copyOutput". TO_DO.  
        """
        if middleware:
            outFile = open('cmscpReport.sh',"a")
            cmscp_exit_status = 0
            txt = '' 
            for file, dict in results.iteritems():
                if dict['lfn']=='':
                    lfn = '$LFNBaseName/'+os.path.basename(file)
                    se  = '$SE'
                else:
                    lfn = dict['lfn']+os.pat.basename(file)
                    se = dict['se']       
                #dict['lfn'] # to be implemented
                txt +=  'echo "Report for File: '+file+'"\n' 
                txt +=  'echo "LFN: '+lfn+'"\n'   
                txt +=  'echo "StorageElement: '+se+'"\n'   
                txt += 'echo "StageOutExitStatusReason ='+dict['reason']+'" | tee -a $RUNTIME_AREA/$repo\n'
                txt += 'echo "StageOutSE = '+se+'" >> $RUNTIME_AREA/$repo\n'
                if dict['erCode'] != '0':
                    cmscp_exit_status = dict['erCode']
            txt += '\n'
            txt += 'export StageOutExitStatus='+str(cmscp_exit_status)+'\n'
            txt +=  'echo "StageOutExitStatus = '+str(cmscp_exit_status)+'" | tee -a $RUNTIME_AREA/$repo\n'
            outFile.write(str(txt))
            outFile.close()
        else: 
            for file, code in results.iteritems():
                print 'error code = %s for file %s'%(code,file) 
        return

    def storageInterface( self, endpoint, protocol ):
        """
        Create the storage interface. 
        """
        try:
            interface = SElement( FullPath(endpoint), protocol )
        except Exception, ex:
            msg = '' 
            if self.debug : msg = str(ex)+'\n'
            msg += "ERROR : Unable to create interface with %s protocol\n"%protocol  
            print msg

        return interface

    def checkDir(self, Destination_SE, protocol):
        '''
        ToBeImplemented NEEDED for castor
        ''' 
        return
 
    def createDir(self, Destination_SE, protocol):
        """
        Create remote dir for gsiftp/rfio REALLY TEMPORARY 
        this should be transparent at SE API level. 
        """
        ErCode = '0'
        msg_1 = ''
        try:
            action = SBinterface( Destination_SE )
            action.createDir()
            if self.debug: print "The directory has been created using protocol %s\n"%protocol
        except Exception, ex:
            msg = '' 
            if self.debug : msg = str(ex)+'\n'
            msg_1 = "ERROR: problem with the directory creation using %s protocol \n"%protocol 
            msg += msg_1
            ErCode = '60316'  
            #print msg

        return ErCode, msg_1 

    def checkFileExist(self, sbi, filetocopy):
        """
        Check if file to copy already exist  
        """ 
        try:
            check = sbi.checkExists(filetocopy)
        except Exception, ex:
            msg = '' 
            if self.debug : msg = str(ex)+'\n'
            msg += "ERROR: problem with check File Exist using %s protocol \n"%protocol 
           # print msg
        ErCode = '0'
        msg = ''
        if check : 
            ErCode = '60303' 
            msg = "file %s already exist"%filetocopy
            print msg

        return ErCode,msg  

    def makeCopy(self, sbi, filetocopy, opt ):  
        """
        call the copy API.  
        """
        path = os.path.dirname(filetocopy)  
        file_name =  os.path.basename(filetocopy)
        source_file = filetocopy
        dest_file = file_name ## to be improved supporting changing file name  TODO
        if self.source == '' and path == '':
            source_file = os.path.abspath(filetocopy)
        elif self.destination =='': 
            dest_file = os.path.join(os.getcwd(),file_name)
        elif self.source != '' and self.destination != '' :
            source_file = file_name  
        ErCode = '0'
        msg = ''
 
        try:
            pippo = sbi.copy( source_file , dest_file , opt)
            if self.protocol == 'srm' : self.checkSize( sbi, filetocopy ) 
        except Exception, ex:
            msg = '' 
            if self.debug : msg = str(ex)+'\n'
            msg = "Problem copying %s file with %s command"%( filetocopy, protocol )
            ErCode = '60307'
            #print msg

        return ErCode, msg
  
    '''
    def checkSize()
        """
        Using srm needed a check of the ouptut file size.  
        """
    
        echo "--> remoteSize = $remoteSize"
        ## for local file
        localSize=$(stat -c%s "$path_out_file")
        echo "-->  localSize = $localSize"
        if [ $localSize != $remoteSize ]; then
            echo "Local fileSize $localSize does not match remote fileSize $remoteSize"
            echo "Copy failed: removing remote file $destination"
                srmrm $destination
                cmscp_exit_status=60307
       
       
                echo "Problem copying $path_out_file to $destination with srmcp command"
                StageOutExitStatusReason='remote and local file dimension not match'
                echo "StageOutReport = `cat ./srmcp.report`"
    ''' 
    def backup(self): 
        """
        Check infos from TFC using existing api obtaining:
        1)destination
        2)protocol
        """
        return

    def usage(self):

        msg=""" 
        required parameters:
        --source        :: REMOTE           :       
        --destination   :: REMOTE           :   
        --debug             :
        --inFile :: absPath : or name NOT RELATIVE PATH
        --outFIle :: onlyNAME : NOT YET SUPPORTED
 
        optional parameters       
        """
        return msg 

if __name__ == '__main__' :
    try:
        cmscp_ = cmscp(sys.argv[1:])
        cmscp_.run()
    except:
        pass

