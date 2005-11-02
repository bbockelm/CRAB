#!/bin/sh
for file in `ls `
do
 echo $file
 cat $file | sed -e "s?CRABSCRIPT?CRABSCRIPT?g" > ${file}_tmp
 mv ${file}_tmp $file
done
