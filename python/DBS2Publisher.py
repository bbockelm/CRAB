import sys, getopt, string
import common
import time 
from FwkJobRep.ReportParser import readJobReport
from crab_util import *
from crab_logger import Logger
from crab_exceptions import *
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects



class Publisher:
    def __init__(self, cfg_params, username, dataname):
        """
        Publisher class: 

        - parses CRAB FrameworkJobReport on UI
        - returns <file> section of xml in dictionary format for each xml file in crab_0_xxxx/res directory
        - publishes output data on DBS and DLS
        """

        self.username = username
        self.dataname = dataname
        self.resDir = common.work_space.resDir()
        common.logger.message('self.resDir = '+self.resDir)
        #### this value can be put in the crab.cfg file 
        ####### da passare dal cfg di crab
        #old api self.DBSURL='http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet'
        self.DBSURL=cfg_params['CMSSW.dbs_url_for_publication']
        #self.DBSURL='http://cmssrv17.fnal.gov:8989/DBS_1_0_4_pre2/servlet/DBSServlet'
        common.logger.message('self.DBSURL = '+self.DBSURL)
        self.datasetpath=cfg_params['CMSSW.datasetpath']
        common.logger.message('self.datasetpath = '+self.datasetpath)
        self.SEName=''
        self.CMSSW_VERSION=''
        self.exit_status=''
        self.time = time.strftime('%y%m%d_%H%M%S',time.localtime(time.time()))
        
    
    def importParentDataset(self,globalDBS, datasetpath):
       """
       """ 
       dbsWriter = DBSWriter(self.DBSURL,level='ERROR')
       try:
           #dbsWriter.importDataset(globalDBS, datasetpath, self.DBSURL)
           dbsWriter.importDataset(globalDBS, self.datasetpath, self.DBSURL)
       except DBSWriterError, ex:
           msg = "Error importing dataset to be processed into local DBS\n"
           msg += "Source Dataset: %s\n" % datasetpath
           msg += "Source DBS: %s\n" % globalDBS
           msg += "Destination DBS: %s\n" % self.DBSURL
           msg += "%s"%ex
           common.logger.message(msg)
           return 1
          
    def publishDataset(self,f):
        """
        """
        try:   
            jobReport = readJobReport(self.resDir+f)[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: Problem with "+self.resDir + f+" file"  
            raise CrabException(msg)
        ##### for parents    
        try:
            globalDBS="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
            self.importParentDataset(globalDBS, self.datasetpath)
        except:
            common.logger.message('Problem with parent import from the global DBS '+globalDBS+ 'to the local one '+self.DBSURL)
        ########################3    
        #  //
        # // DBS to contact
        #//
        dbswriter = DBSWriter(self.DBSURL,level='ERROR')                        
        #
        # publish a dataset : it should be done only once for the task
        #                     and not for all the JobReport
        try:   
            fileinfo= jobReport.files[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: No file to publish in xml file"+self.resDir + f+" file"  
            raise CrabException(msg)
            return self.exit_status

        common.logger.message("FileInfo = " + str(fileinfo))
        #print "FileInfo %s "%fileinfo
        datasets=fileinfo.dataset 
        common.logger.message("DatasetInfo = " + str(datasets))
        #print "DatasetInfo %s "%datasets
        for dataset in datasets:
            #### FEDE overwrites some info contained in the xml file
            #### if we want we can change these infos directly in the ModifyJobReport   
            dataset['ProcessedDataset']=self.username+self.dataname
            ##############################################
            #### to better understand how to fill ....
            cfgMeta = {'name' : 'usercfg' , 'Type' : 'user' , 'annotation': 'user cfg', 'version' : 'private version'} # add real name of user cfg
            common.logger.message("PrimaryDataset = %s"%dataset['PrimaryDataset'])
            common.logger.message("ProcessedDataset = %s"%dataset['ProcessedDataset'])
            common.logger.message("Inserting primary: %s processed : %s"%(dataset['PrimaryDataset'],dataset['ProcessedDataset']))
            primary = DBSWriterObjects.createPrimaryDataset( dataset, dbswriter.dbs)
            algo = DBSWriterObjects.createAlgorithm(dataset, cfgMeta, dbswriter.dbs)

            processed = DBSWriterObjects.createProcessedDataset(primary, algo, dataset, dbswriter.dbs)
            common.logger.message("Inserted primary %s processed %s"%(primary,processed))

    def publishAJobReport(self,f,procdataset):
        """
        """
        try:
            jobReport = readJobReport(self.resDir+f)[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: Problem with "+self.resDir + f+" file"
            raise CrabException(msg)

        # overwite ProcessedDataset with user defined value
        for file in jobReport.files:
            for ds in file.dataset:
                ds['ProcessedDataset']=procdataset
        #  //
        # // DBS to contact
        #//
        dbswriter = DBSWriter(self.DBSURL,level='ERROR')
        #
        # insert files
        #
        Blocks=None
        try:
            Blocks=dbswriter.insertFiles(jobReport)
            print  "in publishAJobReport------>>>> Blocks = %s"%Blocks
        except DBSWriterError, ex:
            print "%s"%ex
        return Blocks

    def publish(self):
        """
        parse of all xml file on res dir and creation of distionary
        """
        common.logger.message("Starting data publish")
        file_list = os.listdir(self.resDir)
        common.logger.debug(6, "file_list = "+str(file_list))
        common.logger.debug(6, "len(file_list) = "+str(len(file_list)))
        #print "file_list = "+str(file_list)
        # FIXME: 
        #   do the dataset publication self.publishDataset here
        #
        if (len(file_list)>0):
            BlocksList=[]
            for f in file_list:
                #common.logger.message("file = "+f)
                if str(f).find('xml') == -1:
                    continue
                else:
                    common.logger.message("file = "+f)
                    common.logger.message("Publishing dataset")
                    self.publishDataset(f)
                    common.logger.message("Publishing files")
                    Blocks=self.publishAJobReport(f,self.username+self.dataname)
                    if Blocks:
                        [BlocksList.append(x) for x in Blocks]
            #
            # close the blocks
            #       
            dbswriter = DBSWriter(self.DBSURL,level='ERROR')
            for BlockName in BlocksList:
                try:   
                    closeBlock=dbswriter.manageFileBlock(BlockName,maxFiles= 1)
                    #print "closeBlock closeBlock closeBlock closeBlock %s"%closeBlock
                    #dbswriter.dbs.closeBlock(BlockName)
                except DBSWriterError, ex:
                    common.logger.message("%s"%ex)
            return self.exit_status


        else:
            common.logger.message(self.resDir+" empty --> No file to publish on DBS/DLS")
            self.exit_status = '1'
            return self.exit_status
    
