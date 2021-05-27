#!/bin/bash

#DSPACE_HOME=/dspace

echo "DSPACE_HOME=$DSPACE_HOME "

# get absolute path of file arguments
in_mrc=`readlink -f $1`
out_mrc=`readlink -f $2`

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

DJARS=`echo $DSPACE_HOME/lib/*.jar | sed 's/ /\:/g'`
LJARS=`echo lib/*.jar | sed 's/ /\:/g'`

/usr/bin/env java -classpath "build:$LJARS:$DJARS:$DSPACE_HOME/config" edu.princeton.dspace.etds.ETDMARCProcessor -v -d $DSPACE_HOME  -i $in_mrc -o $out_mrc

echo "DONE "
