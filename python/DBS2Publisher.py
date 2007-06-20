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
        #self.DBSURL='http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet'
        self.DBSURL='http://cmssrv17.fnal.gov:8989/DBS_1_0_4_pre2/servlet/DBSServlet'
        common.logger.message('self.DBSURL = '+self.DBSURL)
        self.SEName=''
        self.CMSSW_VERSION=''
        self.exit_status=''
        self.time = time.strftime('%y%m%d_%H%M%S',time.localtime(time.time()))

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
        print "FileInfo %s "%fileinfo
        datasets=fileinfo.dataset 
        common.logger.message("DatasetInfo = " + str(datasets))
        print "DatasetInfo %s "%datasets
        # 
        #dataset['ApplicationFamily']="Userfamily" 
        #dataset['ApplicationVersion']=dataset['CMSSW_VERSION'] # why are cmssw version and  
        #dataset['PSetHash']=dataset['PSETHASH']                # psethash named differently
        #dataset['PrimaryDataset']="testAF"
        #dataset['PSetContent']="FIXME" # add here real cfg file content 
        #dataset['DataTier']='USER'
        for dataset in datasets:
            #### FEDE overwrites some info contained in the xml file
            #### if we want we can change these infos directly in the ModifyJobReport   
            dataset['ProcessedDataset']=self.username+self.dataname
            ##############################################
            #### to better understand how to fill ....
            cfgMeta = {'Name' : 'usercfg' , 'Type' : 'user' , 'Annotation': 'user cfg', 'Version' : 'private version'} # add real name of user cfg
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
        try:
            Blocks=dbswriter.insertFiles(jobReport)
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
                    [BlocksList.append(x) for x in Blocks]
            #
            # close the blocks
            #       
            dbswriter = DBSWriter(self.DBSURL,level='ERROR')
            for BlockName in BlocksList:
                dbswriter.manageFileBlock(BlockName,maxFiles= 1)
            
            return self.exit_status


        else:
            common.logger.message(self.resDir+" empty --> No file to publish on DBS/DLS")
            self.exit_status = '1'
            return self.exit_status
    
