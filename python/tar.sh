#!/bin/sh

### $1 is the tag value CRAB_X_X_X ###

tag=$1
echo "tag = $tag"

cp -r CRAB $tag

cd $tag
echo "cd $tag"

rm -rf CVS
rm -rf python/crab.sh
rm -rf python/crab.csh
rm -rf python/CVS
rm -rf python/BossScript
echo "removed CVS, python/crab.(c)sh, python/CVS and python/BossScript"

rm -rf python/crab_0_*
rm -rf python/*.pyc
rm -rf python/*.log
rm -rf python/*.history
echo "removed python/crab_0_*, python/*.pyc, python/*.log and python/*.history"

mv python/configure .
mv python/configureBoss .
echo "moved python/configure and python/configureBoss in $tag"

mkdir Boss
echo "created Boss dir"
cd Boss
wget http://boss.bo.infn.it/boss-v3_6_3-bin.tar.gz
echo "retrieved boss.tar.gz"

cd ../..

tar zcvf $tag.tgz $tag
echo "created $tag.tgz"
echo "bye"


