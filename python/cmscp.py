##!/usr/bin/env python

import sys, os
from ProdCommon.Storage.SEAPI.SElement import SElement, FullPath
from ProdCommon.Storage.SEAPI.SBinterface import *
from ProdCommon.Storage.SEAPI.Exceptions import *


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
        self.params = {"source":'', "destination":'','destinationDir':'', "inputFileList":'', "outputFileList":'', \
                           "protocol":'', "option":'', "middleware":'', "srm_version":'srmv2'}
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
        if self.debug: print 'calling run() : \n'
        # stage out from WN
        if self.params['middleware'] :
           results = self.stager(self.params['middleware'],self.params['inputFileList'])
           self.finalReport(results)
        # Local interaction with SE
        else:
           results = self.copy(self.params['inputFileList'], self.params['protocol'], self.params['option'] )
           return results

    def setProtocol( self, middleware ):
        """
        define the allowed potocols based on $middlware
        which depend on scheduler
        """
        # default To be used with "middleware"
        lcgOpt={'srmv1':'-b -D srmv1  -t 2400 --verbose',
                'srmv2':'-b -D srmv2  -t 2400 --verbose'}
        srmOpt={'srmv1':' -report ./srmcp.report -retry_timeout 480000 -retry_num 3 -streams_num=1 ',
                'srmv2':' -report ./srmcp.report -retry_timeout 480000 -retry_num 3 '}
        rfioOpt=''

        supported_protocol = None
        if middleware.lower() in ['osg','lcg','condor']:
            supported_protocol = [('srm-lcg',lcgOpt[self.params['srm_version']]),\
                                 (self.params['srm_version'],srmOpt[self.params['srm_version']])]
        elif middleware.lower() in ['lsf','caf']:
            supported_protocol = [('rfio',rfioOpt)]
        else:
            ## here we can add support for any kind of protocol,
            ## maybe some local schedulers need something dedicated
            pass
        return supported_protocol

 #   def checkCopy(self, copy_results, list_files):
        """
        #results={}
        list_retry = []
        list_existing = []
        list_ok = []
        if copy_results.keys() == '':
            self.results.update(copy_results)
        else:
            for file, dict in copy_results.iteritems():
                er_code = dict['erCode']
                if er_code == '0':
                    list_ok.append(file)
                    reason = 'Copy succedeed with %s utils'%prot
                    upDict = self.updateReport(file, er_code, reason)
                    copy_results.update(upDict)
                elif er_code == '60303': list_existing.append( file )
                else: list_retry.append( file )
            results.update(copy_results)
            if len(list_ok) != 0:
                msg = 'Copy of %s succedeed with %s utils\n'%(str(list_ok),prot)
                if self.debug : print msg
            if len(list_ok) == len(list_files) :
                msg = 'Copy of  all files succedeed\n'
                #break
            else:
                if self.debug : print 'Copy of files %s failed using %s...\n'%(str(list_retry)+str(list_existing),prot)
                #if len(list_retry): list_files = list_retry
        return list_retry, results        
        
        """ 
    def stager( self, middleware, list_files ):
        """
        Implement the logic for remote stage out
        """
 
        if self.debug: print 'stager() :\n'
        results={}
        for prot, opt in self.setProtocol( middleware ):
            if self.debug: print '\tTrying the stage out with %s utils \n'%prot
            copy_results = self.copy( list_files, prot, opt )
            ######## to define a new function checkCopy ################
            #list_retry, self.results = self.checkCopy(copy_results, list_files)
        #def checkCopy (self, copy_results):
        #    """
        #    """
        #    results={}
            list_retry = []
            list_existing = []
            list_ok = []
            if copy_results.keys() == [''] or copy_results.keys() == '' :
                results.update(copy_results)
            else:
                for file, dict in copy_results.iteritems():
                    er_code = dict['erCode']
                    if er_code == '0':
                        list_ok.append(file)
                        reason = 'Copy succedeed with %s utils'%prot
                        upDict = self.updateReport(file, er_code, reason)
                        copy_results.update(upDict)
                    elif er_code in ['60303','60302']: list_existing.append( file )
                    else: list_retry.append( file )
                results.update(copy_results)
                msg = ''
                if len(list_ok) != 0:
                    msg += '\tCopy of %s succedeed with %s utils\n'%(str(list_ok),prot)
                if len(list_ok) == len(list_files) :
                    break
                else:
                    if self.debug: msg += '\tCopy of %s failed using %s...\n'%(str(list_retry)+str(list_existing),prot)
                    if len(list_retry): list_files = list_retry
                    else: break
                if self.debug : print msg
            """
            if len(list_retry):
               list_files = list_retry
            #def backupCopy(list_retry)
               print "in backup"
               self.params['inputFilesList']=list_files
               ### copy backup
               from ProdCommon.FwkJobRep.SiteLocalConfig import loadSiteLocalConfig
               siteCfg = loadSiteLocalConfig()
               #print siteCfg
               seName = siteCfg.localStageOut.get("se-name", None)
               #print  "seName = ", seName
               self.params['destination']=seName
               #catalog = siteCfg.localStageOut.get("catalog", None)
               #print "catalog = ", catalog
               implName = siteCfg.localStageOut.get("command", None)
               print "implName = ", implName
               if (implName == 'srm'):
                  implName='srmv2'
               self.params['protocol']=implName
               tfc = siteCfg.trivialFileCatalog()
               #print "tfc = ", tfc
               print " self.params['inputFilesList'] = ", self.params['inputFilesList']
               file_backup=[]
               for input in self.params['inputFilesList']:
                   ### to add the correct lfn, passed as argument of cmscp function (--lfn xxxx)
                   file = '/store/'+input
                   pfn = tfc.matchLFN(tfc.preferredProtocol, file)
                   print "pfn = ", pfn
                   file_backup.append(pfn)
               self.params['inputFilesList'] = file_backup
               print "#########################################"
               print "self.params['inputFilesList'] = ", self.params['inputFilesList'] 
               print "self.params['protocol'] = ", self.params['protocol'] 
               print "self.params['option'] = ", self.params['option'] 
               self.copy(self.params['inputFilesList'], self.params['protocol'], self.params['option'])
               print "#########################################"
               ###list_retry, self.results = checkCopy(copy_results)
                   #check is something fails and created related dict
                   #        backup = self.analyzeResults(results)
                   #        if backup :
                   #            msg = 'WARNING: backup logic is under implementation\n'
                   #            #backupDict = self.backup()
                   #            ### NOTE: IT MUST RETURN a DICT contains also LFN and SE Name
                   #            results.update(backupDict)
                   #            print msg
            """
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
        if self.debug : print 'initializeApi() :\n'  
        self.source_prot = protocol
        self.dest_prot = protocol
        if not self.params['source'] : self.source_prot = 'local'
        Source_SE  = self.storageInterface( self.params['source'], self.source_prot )
        if not self.params['destination'] : self.dest_prot = 'local'
        Destination_SE = self.storageInterface( self.params['destination'], self.dest_prot )

        if self.debug :
            msg  = '\t(source=%s,  protocol=%s)'%(self.params['source'], self.source_prot)
            msg += '\t(destination=%s,  protocol=%s)'%(self.params['destination'], self.dest_prot)
            print msg

        return Source_SE, Destination_SE

    def copy( self, list_file, protocol, options ):
        """
        Make the real file copy using SE API
        """
        if self.debug :
            msg  = 'copy() :\n'
            msg += '\tusing %s protocol\n'%protocol
            print msg
        try:
            Source_SE, Destination_SE = self.initializeApi( protocol )
        except Exception, ex:
            return self.updateReport('', '-1', str(ex))

        # create remote dir
        if Destination_SE.protocol in ['gridftp','rfio','srmv2']:
            try:
                self.createDir( Destination_SE, Destination_SE.protocol )
            except Exception, ex:
                return self.updateReport('', '60316', str(ex))

        ## prepare for real copy  ##
        try :
            sbi = SBinterface( Source_SE, Destination_SE )
            sbi_dest = SBinterface(Destination_SE)
            sbi_source = SBinterface(Source_SE)
        except ProtocolMismatch, ex:
            msg  = "ERROR : Unable to create SBinterface with %s protocol"%protocol
            msg += str(ex)
            return self.updateReport('', '-1', msg)

        results = {}
        ## loop over the complete list of files
        for filetocopy in list_file:
            if self.debug : print '\tStart real copy for %s'%filetocopy
            try :
                ErCode, msg = self.checkFileExist( sbi_source, sbi_dest, filetocopy )
            except Exception, ex:
                ErCode = -1
                msg = str(ex)  
            if ErCode == '0':
                ErCode, msg = self.makeCopy( sbi, filetocopy , options, protocol,sbi_dest )
            if self.debug : print '\tCopy results for %s is %s'%( os.path.basename(filetocopy), ErCode)
            results.update( self.updateReport(filetocopy, ErCode, msg))
        return results


    def storageInterface( self, endpoint, protocol ):
        """
        Create the storage interface.
        """
        if self.debug : print 'storageInterface():\n'
        try:
            interface = SElement( FullPath(endpoint), protocol )
        except ProtocolUnknown, ex:
            msg  = "ERROR : Unable to create interface with %s protocol"%protocol
            msg += str(ex)
            raise Exception(msg)

        return interface

    def createDir(self, Destination_SE, protocol):
        """
        Create remote dir for gsiftp REALLY TEMPORARY
        this should be transparent at SE API level.
        """
        if self.debug : print 'createDir():\n'
        msg = ''
        try:
            action = SBinterface( Destination_SE )
            action.createDir()
            if self.debug: print "\tThe directory has been created using protocol %s"%protocol
        except TransferException, ex:
            msg  = "ERROR: problem with the directory creation using %s protocol "%protocol
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            raise Exception(msg)
        except OperationException, ex:
            msg  = "ERROR: problem with the directory creation using %s protocol "%protocol
            msg += str(ex)
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        except MissingDestination, ex:
            msg  = "ERROR: problem with the directory creation using %s protocol "%protocol
            msg += str(ex)
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        except AlreadyExistsException, ex:
            if self.debug: print "\tThe directory already exist"
            pass             
        return msg

    def checkFileExist( self, sbi_source, sbi_dest, filetocopy ):
        """
        Check both if source file exist AND 
        if destination file ALREADY exist. 
        """
        if self.debug : print 'checkFileExist():\n'
        ErCode = '0'
        msg = ''
        f_tocopy=filetocopy
        if self.source_prot != 'local':f_tocopy = os.path.basename(filetocopy) 
        try:
            checkSource = sbi_source.checkExists( f_tocopy )
            if self.debug : print '\tCheck for local file %s exist succeded \n'%f_tocopy  
        except OperationException, ex:
            msg  ='ERROR: problems checkig if source file %s exist'%filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            raise Exception(msg)
        except WrongOption, ex:
            msg  ='ERROR problems checkig if source file % exist'%filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            raise Exception(msg)
        except MissingDestination, ex:
            msg  ='ERROR problems checkig if source file % exist'%filetocopy
            msg += str(ex)
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        if not checkSource :
            ErCode = '60302'
            msg = "ERROR file %s do not exist"%os.path.basename(filetocopy)
            return ErCode, msg
        f_tocopy=filetocopy
        if self.dest_prot != 'local':f_tocopy = os.path.basename(filetocopy) 
        try:
            check = sbi_dest.checkExists( f_tocopy )
            if self.debug : print '\tCheck for remote file %s exist succeded \n'%f_tocopy  
        except OperationException, ex:
            msg  = 'ERROR: problems checkig if file %s already exist'%filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg
            raise Exception(msg)
        except WrongOption, ex:
            msg  = 'ERROR problems checkig if file % already exist'%filetocopy
            msg += str(ex)
            if self.debug :
                msg += '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                msg += '\t'+str(ex.output)+'\n'
            raise Exception(msg)
        except MissingDestination, ex:
            msg  ='ERROR problems checkig if source file % exist'%filetocopy
            msg += str(ex)
            if self.debug : print '\t'+msg+'\n\t'+str(ex.detail)+'\n'
            raise Exception(msg)
        if check :
            ErCode = '60303'
            msg = "file %s already exist"%os.path.basename(filetocopy)

        return ErCode, msg

    def makeCopy(self, sbi, filetocopy, option, protocol, sbi_dest ):
        """
        call the copy API.
        """
        if self.debug : print 'makeCopy():\n'
        path = os.path.dirname(filetocopy)
        file_name =  os.path.basename(filetocopy)
        source_file = filetocopy
        dest_file = file_name ## to be improved supporting changing file name  TODO
        if self.params['source'] == '' and path == '':
            source_file = os.path.abspath(filetocopy)
        elif self.params['destination'] =='':
            destDir = self.params.get('destinationDir',os.getcwd())
            dest_file = os.path.join(destDir,file_name)
        elif self.params['source'] != '' and self.params['destination'] != '' :
            source_file = file_name

        ErCode = '0'
        msg = ''

        try:
            sbi.copy( source_file , dest_file , opt = option)
        except TransferException, ex:
            msg  = "Problem copying %s file" % filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg 
            ErCode = '60307'
        except WrongOption, ex:
            msg  = "Problem copying %s file" % filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbsmsg 
            ErCode = '60307'
        if ErCode == '0' and protocol.find('srmv') == 0:
            remote_file_size = -1 
            local_file_size = os.path.getsize( source_file ) 
            try:
                remote_file_size = sbi_dest.getSize( dest_file )
                if self.debug : print '\t Check of remote size succeded for file %s\n'%dest_file
            except TransferException, ex:
                msg  = "Problem checking the size of %s file" % filetocopy
                msg += str(ex)
                if self.debug :
                    dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                    dbgmsg += '\t'+str(ex.output)+'\n'
                    print dbgmsg
                ErCode = '60307'
            except WrongOption, ex:
                msg  = "Problem checking the size of %s file" % filetocopy
                msg += str(ex)
                if self.debug :
                    dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                    dbgmsg += '\t'+str(ex.output)+'\n'
                    print dbgmsg
                ErCode = '60307'
            if local_file_size != remote_file_size:
                msg = "File size dosn't match: local size = %s ; remote size = %s " % (local_file_size, remote_file_size)
                ErCode = '60307'

        if ErCode != '0':
            try :
                self.removeFile( sbi_dest, dest_file )
            except Exception, ex:
                msg += '\n'+str(ex)  
        return ErCode, msg

    def removeFile( self, sbi_dest, filetocopy ):
        """  
        """  
        if self.debug : print 'removeFile():\n'
        f_tocopy=filetocopy
        if self.dest_prot != 'local':f_tocopy = os.path.basename(filetocopy)
        try:
            sbi_dest.delete( f_tocopy )
            if self.debug : '\t deletion of file %s succeeded\n'%str(filetocopy)
        except OperationException, ex:
            msg  ='ERROR: problems removing partially staged file %s'%filetocopy
            msg += str(ex)
            if self.debug :
                dbgmsg  = '\t'+msg+'\n\t'+str(ex.detail)+'\n'
                dbgmsg += '\t'+str(ex.output)+'\n'
                print dbgmsg
            raise Exception(msg)

        return 

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
            if file:
                if dict['lfn']=='':
                    lfn = '$LFNBaseName/'+os.path.basename(file)
                    se  = '$SE'
                else:
                    lfn = dict['lfn']+os.path.basename(file)
                    se = dict['se']
                #dict['lfn'] # to be implemented
                txt +=  'echo "Report for File: '+file+'"\n'
                txt +=  'echo "LFN: '+lfn+'"\n'
                txt +=  'echo "StorageElement: '+se+'"\n'
                txt += 'echo "StageOutExitStatusReason ='+dict['reason']+'" | tee -a $RUNTIME_AREA/$repo\n'
                txt += 'echo "StageOutSE = '+se+'" >> $RUNTIME_AREA/$repo\n'
                if dict['erCode'] != '0':
                    cmscp_exit_status = dict['erCode']
                    cmscp_exit_status = dict['erCode']
            else:
                txt += 'echo "StageOutExitStatusReason ='+dict['reason']+'" | tee -a $RUNTIME_AREA/$repo\n'
                cmscp_exit_status = dict['erCode']
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
                  "protocol=","option=", "middleware=", "srm_version=", \
                  "destinationDir=","debug", "help"]
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
    except Exception, ex :
        print str(ex)

