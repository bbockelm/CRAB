#!/bin/sh

### $1 is the tag value CRAB_X_X_X ###
### the second argument is the BOSS version: 4_3_4-sl3-sl4 (for DBS2 publication)###
if [ $# -lt 1 ]; then
  echo "Usage: `basename $0` <CRAB_X_Y_Z> "
  exit 1
fi
tag=$1
echo "tag = $tag"

CRABdir=$tag
echo "CRABDIR = $CRABdir"
CRABtag=$tag
DBSAPItag="DBS_1_1_6"
DLSAPItag="DLS_1_1_0_pre7"
PRODCOMMONtag="PRODCOMMON_0_12_3_CRAB_2"

CVSrepo=":pserver:anonymous@cmscvs.cern.ch:/cvs_server/repositories"
export CVSROOT=${CVSrepo}"/CMSSW"
repo_url="https://cmsweb.cern.ch/crabconf/"

## download CRAB from CVS and cleanup the code a bit
echo ">> downloading CRAB tag $CRABtag from CVS CRAB"
cvs co -r $CRABtag -d $CRABdir CRAB

#echo ">> downloading CRAB HEAD from CVS CRAB"
#echo ">> NOTE: Temporary Use of HEAD "
#cvs co -d $CRABdir CRAB

cd $CRABdir
cvs up -P python/BossScript 
chmod -x python/crab.py
rm python/crab.*sh
rm python/tar*
rm python/zero
rm -rf CRABSERVER
rm -rf PsetCode
mv python/configure .

## create etc subdir for admin config file
mkdir -p etc
## create empty config file
touch etc/crab.cfg

## create external subdir  for dependeces
mkdir -p external
cd external

## download sqlite
echo ">> downloading sqlite from CRAB web page"
wget --no-check-certificate $repo_url/sqlite.tgz

## download py2-sqlite
echo ">> downloading py2-sqlite from CRAB web page"
wget --no-check-certificate  $repo_url/py2-pysqlite.tgz 

## download pyOpenSSL
echo ">> downloading pyOpenSSL CRAB web page"
wget --no-check-certificate  $repo_url/pyOpenSSL-0.6-python2.4.tar.gz

## download DBS API
echo ">> downloading DBS API tag ${DBSAPItag} from CVS DBS/Clients/PythonAPI"
cvs co -r ${DBSAPItag} -d DBSAPI COMP/DBS/Clients/Python
# add this dirs to the PYTHONPATH

## download DLS API
echo ">> downloading DLS PHEDeX API tag ${DLSAPItag} from CVS DLS/Client/LFCClient"
cvs co -r ${DLSAPItag} DLS/Client/LFCClient
cd DLS/Client/LFCClient
## creating library
make PREFIX=../../../DLSAPI
cd -
## move to the CRAB standard location for DLSAPI
#mv DLS/Client/lib DLSAPI
rm -r DLS
# add this dir to PATH

## download PRODCOMMON
echo ">> downloading PRODCOMMON tag ${PRODCOMMONtag} from CVS PRODCOMMON"
#mkdir -p ProdCommon
#cd ProdCommon
cvs co -r ${PRODCOMMONtag} -d ProdCommon COMP/PRODCOMMON/src/python/ProdCommon
cvs co -r ${PRODCOMMONtag} -d IMProv COMP/PRODCOMMON/src/python/IMProv
#cvs co -d ProdCommon COMP/PRODCOMMON/src/python/ProdCommon
#cvs co -d IMProv COMP/PRODCOMMON/src/python/IMProv
#cd ..
## exit from external
cd ../..

tar zcvf $CRABdir.tgz $CRABdir
echo ""
echo " tarball prepared : $CRABdir.tgz " 
