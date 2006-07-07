#!/bin/sh

### $1 is the tag value CRAB_X_X_X ###
if [ $# -lt 1 ]; then
  echo "Usage: `basename $0` <CRAB_X_Y_Z> <BOSS version: X_Y_Z>"
  exit 1
fi
tag=$1
boss_version=$2
echo "tag = $tag"

#CRABdir="${tag}_dbsdls"
CRABdir="$tag"
echo "CRABDIR = $CRABDIR"
CRABtag=$tag
DBSAPItag="DBS_0_0_1"
DLSAPItag="DLS_0_1_0"

CVSrepo=":pserver:anonymous@cmscvs.cern.ch:/cvs_server/repositories"
export CVSROOT=${CVSrepo}"/CMSSW"

#configure_dbsdls=configure_dbsdls
## download CRAB from CVS and cleanup the code a bit
#echo ">> downloading CRAB tag $CRABtag from CVS CRAB"
echo ">> downloading CRAB HEAD from CVS CRAB"
#cvs co -r $CRABtag -d $CRABdir CRAB
cvs co -d $CRABdir CRAB
cvs up -P BossScript 
cd $CRABdir
#rm -r CVS
#rm -r python/CVS
rm python/crab.*sh
rm python/tar*
rm python/zero
mv python/configure .
mv python/configureBoss .

#rm configure 
#mv $configure_dbsdls configure
#less python/crab.cfg | sed -e "s?jobtype = orca?jobtype = orca_dbsdls?" >  crab.cfg.tmp
#mv crab.cfg.tmp python/crab.cfg
#echo "--> configure crab.cfg to set by default the jobtype to orca_dbsdls"

## download Boss
echo ">> downloading BOSS from http://boss.bo.infn.it/boss-v$2-bin.tar.gz "
mkdir Boss
cd Boss
wget http://boss.bo.infn.it/boss-v${boss_version}-bin.tar.gz
## remove doc from Boss
gunzip boss-v${boss_version}-bin.tar.gz
tar --delete boss-v${boss_version}/doc/note03_005.rtf -vf boss-v${boss_version}-bin.tar
tar --delete boss-v${boss_version}/doc/note03_005.pdf -vf boss-v${boss_version}-bin.tar
tar --delete boss-v${boss_version}/doc/BOSS-UserGuide-3.6.2.rtf -vf boss-v${boss_version}-bin.tar
tar --delete boss-v${boss_version}/doc/BOSS-UserGuide-3.6.2.pdf -vf boss-v${boss_version}-bin.tar
gzip boss-v${boss_version}-bin.tar

## download DBS API
echo ">> downloading DBS API tag $DBSAPItag from CVS DBS/Clients/PythonAPI"
cd ..
cvs co -r ${DBSAPItag} -d DBSAPI COMP/DBS/Clients/PythonAPI
rm -r DBSAPI/CVS
rm -r DBSAPI/lib  #remove SOAPy etc...
# add this dir to the PYTHONPATH
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

cd ..
tar zcvf $CRABdir.tgz $CRABdir
echo ""
echo " tarball prepared : $CRABdir.tgz " 
