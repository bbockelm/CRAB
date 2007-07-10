#!/bin/sh

### $1 is the tag value CRAB_X_X_X ###
### the second argument is the BOSS version: 4_3_4-sl3-sl4 (for DBS2 publication)###
if [ $# -lt 1 ]; then
  echo "Usage: `basename $0` <CRAB_X_Y_Z> <BOSS X_Y_Z>"
  exit 1
fi
tag=$1
boss_version=$2
echo "tag = $tag"

#CRABdir="${tag}_dbsdls"
CRABdir="$tag"
echo "CRABDIR = $CRABDIR"
CRABtag=$tag
DBSAPItag="DBS_1_0_5"
DLSAPItag="DLS_1_0_0"
#PAAPItag="HEAD"
PAAPItag="PRODAGENT_0_2_1"
PRODCOMMONtag="PRODCOMMON_0_3_0"

CVSrepo=":pserver:anonymous@cmscvs.cern.ch:/cvs_server/repositories"
export CVSROOT=${CVSrepo}"/CMSSW"

#configure_dbsdls=configure_dbsdls
## download CRAB from CVS and cleanup the code a bit
echo ">> downloading CRAB tag $CRABtag from CVS CRAB"
#echo ">> downloading CRAB HEAD from CVS CRAB"
#cvs co -r $CRABtag -d $CRABdir CRAB
cvs co -d $CRABdir CRAB
cd $CRABdir
cvs up -P python/BossScript 
chmod -x python/crab.py
rm python/crab.*sh
rm python/tar*
rm python/zero
rm -rf CRABSERVER
mv python/configure .
mv python/configureBoss .

## download Boss
echo ">> downloading BOSS version ${boss_version} from http://boss.bo.infn.it/boss-v$2-bin.tar.gz "
mkdir Boss
cd Boss
wget http://boss.bo.infn.it/BOSS_${boss_version}-bin.tar.gz
# wget http://www.bo.infn.it/~codispot/BOSS/BOSS-bin.tgz
# mv BOSS-bin.tgz boss-v${boss_version}-bin.tar.gz

## download DBS API
echo ">> downloading DBS API tag ${DBSAPItag} from CVS DBS/Clients/PythonAPI"
cd ..
cvs co -r ${DBSAPItag} -d DBSAPI COMP/DBS/Clients/Python
# add this dirs to the PYTHONPATH
## download DLS API
echo ">> downloading DLS CLI tag ${DLSAPItag} from CVS DLS/Client"
cvs co -r ${DLSAPItag} DLS/Client
cd DLS/Client
## creating library
make
cd ../..
## move to the CRAB standard location for DLSAPI
mv DLS/Client/lib DLSAPI
rm -r DLS
# add this dir to PATH

## download PA API
echo ">> downloading PA API tag ${DBSAPItag} from CVS DBS/Clients/PythonAPI"
mkdir -p ProdAgentApi
cd ProdAgentApi
cvs co -r ${PAAPItag} -d IMProv COMP/PRODAGENT/src/python/IMProv
cvs co -r ${PAAPItag} -d FwkJobRep COMP/PRODAGENT/src/python/FwkJobRep
cd ..

### FEDE FOR DBS2 PUBLICATION
### add modify report file
echo "mv ModifyJobReport.py file from python to ProdAgentApi/FwkJobRep"
mv python/ModifyJobReport.py ProdAgentApi/FwkJobRep/ModifyJobReport.py

## download PRODCOMMON
echo ">> downloading PRODCOMMON tag ${PRODCOMMONtag} from CVS PRODCOMMON"
mkdir -p ProdCommon
cd ProdCommon
cvs co -r ${PRODCOMMONtag} -d ProdCommon COMP/PRODCOMMON/src/python/ProdCommon
cd ..
##################

cd ..
tar zcvf $CRABdir.tgz $CRABdir
echo ""
echo " tarball prepared : $CRABdir.tgz " 
