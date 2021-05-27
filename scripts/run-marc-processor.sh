#!/bin/bash

/usr/bin/env java \
  -classpath "build:$JAVA_LOCAL_LIB:$DSPACE_LIB:$DSPACE_HOME/config" \
  edu.princeton.dspace.etds.ETDMARCProcessor \
  -v \
  -d $DSPACE_HOME \
  -i $MARC_INPUT_PATH \
  -o $MARC_RECORDS_PATH
