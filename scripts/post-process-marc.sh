#!/bin/bash

echo "enhanced MARC records: `basename $MARC_IMPORT_PATH`"
echo ''
echo -n 'number of records: ' ; cat $MARC_IMPORT_PATH | fgrep 'Princeton University,' | wc -l
echo -n 'number of matched records: ' ; egrep 'ARK.*http' $MARC_LOG_PATH | wc -l
echo -n 'number of unmatched records: ' ; fgrep 'NO ARK FOR' $MARC_LOG_PATH | wc -l
echo ""
echo "details:"
echo ""
egrep '^ARK' $MARC_LOG_PATH | sort
