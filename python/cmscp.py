#!/usr/bin/env python

import sys, string
import os, popen2
from ProdCommon.Storage.SEAPI.SElement import SElement, FullPath
from ProdCommon.Storage.SEAPI.SBinterface import *


class cmscp:
    def __init__(self, args):
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
        self.params = {"source":'', "destination":'', "inputFileList":'', "outputFileList":'', \
                           "protocol":'', "option":'', "middleware":'', "srm_version":''}
        self.debug = 0  
 
        self.params.update( args )

        return

    def processOptions( self ):
        """
        check command line parameter
        """

        if 'help' in self.params.keys(): HelpOptions()        
        if 'debug' in self.params.keys(): self.debug = 1        

        # source and dest cannot be undefined at same time
        if not self.params['source']  and not self.params['destination'] :
            HelpOptions()

        # if middleware is not defined --> protocol cannot be empty
        if not self.params['middleware'] and not self.params['protocol'] :
            HelpOptions()

        # input file must be defined
        if not self.params['inputFileList'] : HelpOptions()
        else:
            file_to_copy=[]
            if self.params['inputFileList'].find(','): 
                [file_to_copy.append(x.strip()) for x in self.params['inputFileList'].split(',')]
            else:
                file_to_copy.append(self.params['inputFileList'])
            self.params['inputFileList'] = file_to_copy

        ## TO DO:
        #### add check for outFiles
        #### add map {'inFileNAME':'outFileNAME'} to change out name

        return

    def run( self ):
        """
        Check if running on UI (no $middleware) or
        on WN (on the Grid), and take different action
        """

        self.processOptions()
        # stage out from WN
        if self.params['middleware'] :
           results = self.stager(self.params['middleware'],self.params['inputFileList'])
           self.finalReport(results)
        # Local interaction with SE
        else:
           results = self.copy(self.params['inputFilesList'], self.params['protocol'], self.protocols['option'] )
           return results

    def setProtocol( self, middleware ):
        """
        define the allowed potocols based on $middlware
        which depend on scheduler
        """
        # default To be used with "middleware"
        lcgOpt='-b -D srmv2 --vo cms -t 2400 --verbose'
        srmOpt='-debug=true -report ./srmcp.report -retry_timeout 480000 -retry_num 3'
        rfioOpt=''

        if middleware.lower() in ['osg','lcg']:
            supported_protocol = [('srm-lcg',lcgOpt),\
                                  ('srmv2',srmOpt)]
        elif middleware.lower() in ['lsf','caf']:
            supported_protocol = [('rfio',rfioOpt)] 
        else:
            ## here we can add support for any kind of protocol,
            ## maybe some local schedulers need something dedicated
            pass
        return supported_protocol

    def stager( self, middleware, list_files ):
        """
        Implement the logic for remote stage out
        """
        count=0
        results={}
        for prot, opt in self.setProtocol( middleware ):
            if self.debug: print 'Trying stage out with %s utils \n'%prot
            copy_results = self.copy( list_files, prot, opt )
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
                if self.debug : print msg
            if len(list_ok) == len(list_files) :
                break
            else:
                if self.debug : print 'Copy of files %s failed using %s...\n'%(str(list_retry)+str(list_existing),prot)
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
        if not self.params['source'] : source_prot = 'local'
        Source_SE  = self.storageInterface( self.params['source'], source_prot )
        if not self.params['destination'] : dest_prot = 'local'
        Destination_SE = self.storageInterface( self.params['destination'], dest_prot )

        if self.debug :
            print '(source=%s,  protocol=%s)'%(self.params['source'], source_prot)
            print '(destination=%s,  protocol=%s)'%(self.params['destination'], dest_prot)

        return Source_SE, Destination_SE

    def copy( self, list_file, protocol, options ):
        """
        Make the real file copy using SE API
        """
        if self.debug :
            print 'copy(): using %s protocol'%protocol
        Source_SE, Destination_SE = self.initializeApi( protocol )

        # create remote dir
        if protocol in ['gridftp'.'rfio']:
            self.createDir( Destination_SE, protocol )

        ## prepare for real copy  ##
        try :
            sbi = SBinterface( Source_SE, Destination_SE )
            sbi_dest = SBinterface(Destination_SE)
        except Exception, ex:
            msg = ''
            if self.debug : msg = str(ex)+'\n'
            msg += "ERROR : Unable to create SBinterface with %s protocol\n"%protocol
            raise msg

        results = {}
        ## loop over the complete list of files
        for filetocopy in list_file:
            if self.debug : print 'start real copy for %s'%filetocopy
            ErCode, msg = self.checkFileExist( sbi_dest, os.path.basename(filetocopy) )
            if ErCode == '0':
                ErCode, msg = self.makeCopy( sbi, filetocopy , options )
            if self.debug : print 'Copy results for %s is %s'%( os.path.basename(filetocopy), ErCode)
            results.update( self.updateReport(filetocopy, ErCode, msg))
        return results


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
            raise msg

        return interface

    def checkDir(self, Destination_SE, protocol):
        '''
        ToBeImplemented NEEDED for castor
        '''
        return

    def createDir(self, Destination_SE, protocol):
        """
        Create remote dir for gsiftp REALLY TEMPORARY
        this should be transparent at SE API level.
        """
        ErCode = '0'
        msg = '' 
        try:
            action = SBinterface( Destination_SE )
            action.createDir()
            if self.debug: print "The directory has been created using protocol %s\n"%protocol
        except Exception, ex:
            msg = ''
            if self.debug : msg = str(ex)+'\n'
            msg = "ERROR: problem with the directory creation using %s protocol \n"%protocol
            ErCode = '60316'

        return ErCode, msg

    def checkFileExist( self, sbi, filetocopy ):
        """
        Check if file to copy already exist
        """
        ErCode = '0'
        msg = ''
        try:
            check = sbi.checkExists(filetocopy)
        except Exception, ex:
            msg = ''
            if self.debug : msg = str(ex)+'\n'
            msg +='problems checkig if file already exist'
            raise msg
        if check :    
            ErCode = '60303'
            msg = "file %s already exist"%filetocopy

        return ErCode,msg

    def makeCopy(self, sbi, filetocopy, option ):
        """
        call the copy API.
        """
        path = os.path.dirname(filetocopy)
        file_name =  os.path.basename(filetocopy)
        source_file = filetocopy
        dest_file = file_name ## to be improved supporting changing file name  TODO
        if self.params['source'] == '' and path == '':
            source_file = os.path.abspath(filetocopy)
        elif self.params['destination'] =='':
            dest_file = os.path.join(os.getcwd(),file_name)
        elif self.params['source'] != '' and self.params['destination'] != '' :
            source_file = file_name

        ErCode = '0'
        msg = ''

        try:
            sbi.copy( source_file , dest_file , opt = option)
            #if self.protocol == 'srm' : self.checkSize( sbi, filetocopy ) ## TODO
        except Exception, ex:
            msg = ''
            if self.debug : msg = str(ex)+'\n'
            msg = "Problem copying %s file" % filetocopy
            ErCode = '60307'

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

    def finalReport( self , results ):
        """
        It a list of LFNs for each SE where data are stored.
        allow "crab -copyLocal" or better "crab -copyOutput". TO_DO.
        """
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
        return


def usage():

    msg="""
    required parameters:
    --source        :: REMOTE           :
    --destination   :: REMOTE           :
    --debug             :
    --inFile :: absPath : or name NOT RELATIVE PATH
    --outFIle :: onlyNAME : NOT YET SUPPORTED

    optional parameters
    """
    print msg 

    return 

def HelpOptions(opts=[]):
    """
    Check otps, print help if needed
    prepare dict = { opt : value }  
    """
    dict_args = {}
    if len(opts):
        for opt, arg in opts:
            dict_args[opt.split('--')[1]] = arg 
            if opt in ('-h','-help','--help') :
                usage()
                sys.exit(0)
        return dict_args
    else:
        usage()
        sys.exit(0)

if __name__ == '__main__' :

    import getopt 

    allowedOpt = ["source=", "destination=", "inputFileList=", "outputFileList=", \
                  "protocol=","option=", "middleware=", "srm_version=", "debug", "help"]
    try:    
        opts, args = getopt.getopt( sys.argv[1:], "", allowedOpt ) 
    except getopt.GetoptError, err:
        print err
        HelpOptions()
        sys.exit(2)
 
    dictArgs = HelpOptions(opts)
    try:
        cmscp_ = cmscp(dictArgs)
        cmscp_.run()
    except:
        pass

