import os
import logging
from logging.handlers import RotatingFileHandler
import  ProdAgentCore.LoggingUtils as LoggingUtils

class Outputting:

    tempxmlReportFile = "./.tempxmlReportFileName"
    xmlReportFileName = "./xmlReportFile.xml"
    tempTar = "./.done.tar"
    jobEndedCache = "./.TTcache"
    tempTarball = "./.done.tar.gz"
    tarball = "./done.tar.gz"
 

    def __init__( self, xmlReportFile, TEMPxmlReportFile ):
        self.tempxmlReportFile = TEMPxmlReportFile
        self.xmlReportFileName = xmlReportFile


    def add2Tar( self, path, fileList ):
        import TaskTracking.itarfile as tarfile
        flag = 0
        if not os.path.exists( path + self.tempTar ):
            fout = open( path + self.tempTar, "w" )
            fout.write("")
            fout.close()
            flag = 1
        try:
            if flag:
                tar = tarfile.open( path + self.tempTar, "w" )
            else:
                tar = tarfile.open( path + self.tempTar, "a" )
        except:
            logging.error ("Error: Unable to open tar file.")
            raise
        try:
            for name in fileList:
                import traceback
                try:
                    tar.add(name)
                    logging.info("   -> " +str(name))
                except ValueError, ex:
                    logging.error ('Warning: Unable to store: '+ str(name) + ' - ' + str(ex) )
                    logging.error ( str(traceback.format_exc()) + '\n')
                    pass
                except OSError, ex:
                    logging.error ('Warning: Unable to store: ' + str(name) )
                    logging.error ( str(traceback.format_exc()) + '\n')
                except IOError, ex:
                    try:
                        tar.add(name)
                    except:
                        logging.error ('Warning: Unable to store: ' + str(name) )
                        logging.error ( str(traceback.format_exc()) + '\n')
                        raise 
        finally:
            tar.close()
            self.cleanTmpDir( path )
        """
        try:
            if flag:
                tar = tarfile.open( path + self.tempTar, "w" )
            else:
                tar = tarfile.open( path + self.tempTar, "a" )
        except:
            logging.error ("Error: Unable to open tar file.")
            raise
        """

    def prepareTempDir( self, path, jobs2Add ):
        
        cmdMkCache = 'mkdir .tmpDone;'
        os.popen( cmdMkCache )
        ## if note created try once more the cache dir
        if not os.path.exists('.tmpDone'):
            cmd = 'mkdir .tmpDone;'
            os.system( cmd )

        ## if exists copy in cache
        if os.path.exists(path + ".tmpDone"):
            logging.info("Working on jobs....")
            for i in jobs2Add: #range(1, nJob+1):
                logging.info(" JOB [ "+str(i)+" ]: ")
                ## Add parametric indexes for failed and successful jobs # Fabio
                jtResDir = 'job'+str(i)+'/JobTracking'

                cmdCopyXml = 'cp '+self.tempxmlReportFile+' .tmpDone/'+self.xmlReportFileName+' ;'
                xmlCopied = os.popen(cmdCopyXml).readlines()
                if not os.path.exists( os.path.join ( '.tmpDone/', self.xmlReportFileName ) ):
                    logging.info("   Impossible to copy the file " +self.tempxmlReportFile+ ": "+str(xmlCopied))

                cmdListAll = "ls -Rd "+jtResDir+"/Success/Submission_*/*/*"
                logging.info ("   Adding files to cache...")
                flagFailed = 1
                for file2Copy in os.popen(cmdListAll).readlines():
                    logging.info ("      -> " +str( os.path.join(path, file2Copy[:-1]) ))
                    cmdCopy = "cp " +str( os.path.join(path, file2Copy[:-1]) ) + " " + str( os.path.join(path, ".tmpDone/") ) +";"
                    #logging.info("\n\n\nCopying: "+ str( cmdCopy ) )
                    os.popen(cmdCopy).readlines()
                    flag = 0
                if flagFailed:
                    if os.path.exists('./'+jtResDir+'/Failed/'):
                        if len(os.listdir('./'+jtResDir+'/Failed/')) > 0:
                            try:
                                failIndex = max( [ int(s.split('Submission_')[-1]) for s in os.listdir('./'+jtResDir+'/Failed/') ] )
                            except Exception, ex:
                                logging.info( str(ex) )
                            file2Copy = None
                            cmdListAll = "ls -Rd "+jtResDir+"/Failed/Submission_"+str(failIndex)+"/*/*"
                            for file2Copy in os.popen(cmdListAll).readlines():
                                logging.info ("      -> " +str( os.path.join(path, file2Copy[:-1]) ))
                                cmdCopy = "cp " +str( os.path.join(path, file2Copy[:-1]) ) + " " + str( os.path.join(path, ".tmpDone/") ) +";"
                                os.popen(cmdCopy).readlines()

            listFileTemp = []
            try:
                for file2Add in os.listdir(path+ '.tmpDone'):
                    listFileTemp.append( str( os.path.join( '.tmpDone', str(file2Add) ) ) )
            except OSError:
                import traceback
                logging.info("problema listing the dir: " + str(traceback.format_exc()) )
                raise

            return listFileTemp

    def cleanTmpDir( self, path ):
        if os.path.exists(path+'/.tmpDone/'):
            #pass
            cmd = 'rm -drf '+path+'/.tmpDone/;'
            os.system( cmd )

    def cleanjobEndedCache( self, path ):
        if os.path.exists( os.path.join(path, self.jobEndedCache) ):
            cmd = 'rm -f ' + str( os.path.join(path, self.jobEndedCache) )
            os.system( cmd )
         
    def deleteTempTar( self, path ):
        if os.path.exists(path+self.tempTar):
            cmd = 'rm -f '+path+self.tempTar
            os.system( cmd )

    def prepareGunzip( self, path ):
        cmd = 'gzip -c ' + path + '/' + self.tempTar + ' > ' + path + self.tempTarball + '; '
        cmd += 'mv ' + path + self.tempTarball + ' ' + path + self.tarball + ' ;'
        os.system( cmd )

    def getList ( self, path, fileName ):
        if os.path.exists( path + fileName ):
            infile = file( path + fileName , 'r').readline()
            if len(infile) > 0: 
                jobList = infile.split(":")
                jobList[len(jobList)-1] = jobList[len(jobList)-1].split("\n")[0]
                listInt = []
                for job in jobList:
                    if job != "":
                        listInt.append(int(job))
                return listInt #jobList
        return []

    def add2List ( self, path, fileName, jobList ):
        if os.path.exists(path + fileName):
            stringa = ''
            for job in jobList:
                stringa += ':' + str(job)
            infile = file( path + fileName , 'r').readline()
            infile = infile.split("\n")[0]
            file( path + fileName , 'w').write(infile+stringa)
        else:
            stringa = ''
            for job in jobList:
                stringa += str(job) + ':'
            stringa = stringa[0:len(stringa)-1]
            file(path + fileName , 'w').write(stringa)

    def prepare( self, path, taskName, nJob, jobs2Add ):
        work_dir = os.getcwd()
        os.chdir( path )
        
        jobsAdded = self.getList( path, self.jobEndedCache )
        for job in jobsAdded:
            if str(job) in jobs2Add:
                jobs2Add[str(job)] = 0
        jobs2Write = []
        for job, flag in jobs2Add.iteritems():
            if flag == 1 :
                jobs2Write.append(job)
        logging.info("  Ended jobs: " + str(jobs2Write))

        self.add2List( path, self.jobEndedCache, jobs2Write )
        logging.info("path = " +str(path)+ " - taskName: " +str(taskName)+ " - nJob: " +str(nJob)+ " - jobs2Write: " +str(jobs2Write) )
#        writingPatch = []
#        for jobber in range(1, nJob+1):
#            writingPatch.append(jobber)
        fileList = self.prepareTempDir( path, jobs2Write ) #writingPatch) #jobs2Write )

        if fileList != None:
            logging.info("Adding to tar file...")
            self.add2Tar( path, fileList )
            logging.info("Gunzipping tar file...")
            self.prepareGunzip( path )
        else:
            logging.error("problems manipulating job-id")

        os.chdir( work_dir )


if __name__=="__main__":
    obj = Outputting("./.tempxmlReportFileName", "xmlReportFileName.xml")
    cazzona = { 1:1, 2:1, 3:1, 4:1, 5:1, 6:1, 7:1, 8:1, 9:1, 10:1 }
    #obj.prepare( "/home/serverAdmin/test/TESTA/crab_testClearedDone_92efb295-538f-4a7c-bd1f-f5f6895ce222/res/", "crab_testClearedDone_92efb295-538f-4a7c-bd1f-f5f6895ce222", 10, cazzona )
    #obj.prepare( "/flatfiles/cms/crab_test_8e3b228b-c300-4867-af21-732b4e419dcc/res/", "crab_test_8e3b228b-c300-4867-af21-732b4e419dcc", 3, {1:1,2:1,3:1} )
#    obj.prepare( "/flatfiles/cms/crab_crab_0_070717_160939_45572c02-7e0f-457e-9f94-0a3b1ba14583/res/","crab_crab_0_070717_160939_45572c02-7e0f-457e-9f94-0a3b1ba14583", 5, {'1': 1, '3': 1, '2': 1, '5': 1, '4': 1} )
    obj.prepare( "/flatfiles/cms/crab_crab_0_070718_190911_ae583a0d-de24-481e-acab-a7952568e78f/res/","crab_crab_0_070718_190911_ae583a0d-de24-481e-acab-a7952568e78f", 3, {'1': 1, '3': 1, '2': 1} )
