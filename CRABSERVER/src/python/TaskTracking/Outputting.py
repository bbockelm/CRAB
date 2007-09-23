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
        import tarfile
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
                try:
                    tar.add(name)
                except ValueError:
                    logging.error ('Warning: Unable to store: ')
                    logging.error (str(name))
                    logging.error ('\n')
        finally:
            tar.close()
            self.cleanTmpDir( path )

    def prepareTempDir( self, path, taskName, nJob, jobs2Add ):
        cmd = 'mkdir .tmpDone;'
        os.system( cmd )
        if os.path.exists('.tmpDone'):
            for i in jobs2Add: #range(1, nJob+1):
                ## Add parametric indexes for failed and successful jobs # Fabio
                jtResDir = 'job'+str(i)+'/JobTracking'
                cmd = 'cp '+self.tempxmlReportFile+' .tmpDone/'+self.xmlReportFileName+' ;' ## SP
                ## Get the most recent failure and copy that to tmp # Fabio
                failIndex = 4
                if os.path.exists('./'+jtResDir+'/Failed/'):
                    if len(os.listdir('./'+jtResDir+'/Failed/')) > 0:
                        failIndex = max( [ int(s.split('Submission_')[-1]) for s in os.listdir('./'+jtResDir+'/Failed/') ] )
                    cmd += 'cp -r '+ jtResDir +'/Failed/Submission_'+str(failIndex)+'/log/edgLoggingInfo.log .tmpDone/edgLoggingInfo_'+str(i)+'.log ;'
                    cmd += 'cp -r '+ jtResDir +'/Failed/Submission_'+str(failIndex)+'/std*/* .tmpDone/;'
                cmd += 'cp -r '+jtResDir+'/Success/Submission_*/*/* .tmpDone;'
                cmd += 'cp -r '+jtResDir+'/Success/Submission_*/log/edgLoggingInfo.log .tmpDone/edgLoggingInfo_'+str(i)+'.log ;'
                cmd += 'rm .tmpDone/BossChainer.log .tmpDone/BossProgram_1.log .tmpDone/edg_getoutput.log .tmpDone/edgLoggingInfo.log;'
                logging.debug(  cmd )
                os.system( cmd )
            listFileTemp = []
            for file in os.listdir(path+ '.tmpDone'):
                listFileTemp.append( '.tmpDone/' + str(file) )
            return listFileTemp

    def cleanTmpDir( self, path ):
        if os.path.exists(path+'/.tmpDone/'):
            cmd = 'rm -drf '+path+'/.tmpDone/;'
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
            outfile = file( path + fileName , 'w').write(infile+stringa)
        else:
            stringa = ''
            for job in jobList:
                stringa += str(job) + ':'
            stringa = stringa[0:len(stringa)-1]
            outfile = file(path + fileName , 'w').write(stringa)

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
        fileList = self.prepareTempDir( path, taskName, nJob, jobs2Write )
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
