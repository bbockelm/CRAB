#!/bin/sh

### $1 is the tag value CRAB_X_X_X ###
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
DBSAPItag="v00_00_10"
DLSAPItag="DLS_0_1_1"
PAAPItag="HEAD"

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
mkdir DBSAPI
cd DBSAPI
cvs co -r ${DBSAPItag} -d DBS1API COMP/DBS/Clients/PythonAPI
cvs co -r ${DBSAPItag} -d DBS2API COMP/DBS/Clients/Python
#rm -r DBSAPI/CVS
rm -r DBS1API/lib  #remove SOAPy etc...
rm -r DBS2API/UnitTests
rm -r DBS2API/UserExamples
touch DBS1API/__init__.py
touch DBS2API/__init__.py
cd ..
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


cd ..
tar zcvf $CRABdir.tgz $CRABdir
echo ""
echo " tarball prepared : $CRABdir.tgz " 
