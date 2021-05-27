#!/bin/bash

MARC_IMPORT_PATH = $1
MARC_IMPORT_LOG = $2

echo "enhanced MARC records: `basename $MARC_IMPORT_PATH`"
echo ''
echo -n 'number of records: ' ; strings $MARC_IMPORT_PATH | fgrep 'Princeton University,' | wc -l
echo -n 'number of matched records: ' ; egrep 'ARK.*http' $MARC_IMPORT_LOG | wc -l
echo -n 'number of unmatched records: ' ; fgrep 'NO ARK FOR' $MARC_IMPORT_LOG | wc -l
echo ""
echo "details:"
echo ""
egrep '^ARK' $MARC_IMPORT_LOG | sort
