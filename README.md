# DSpace Java Utilities

## ETD Utilities

### Requirements

- bash (Release 4.4)
- Java (OpenJDK 1.8)

### MARC Processor

This enhances the MARC records delivered from ProQuest with Handle IDs for
graduate and doctoral electronic theses and dissertations (ETDs).

As this must be invoked from a server environment in which DSpace is installed, please ensure that `$DSPACE_HOME` is defined.

#### Running the Processor

To import to production:
```bash
export MARC_IMPORT_PATH = "/dspace/marc_imports/proquest_import.zip"
```

# remove Line Feeds from between records in the ProQuest file
```bash
cat $MARC_IMPORT_PATH |    tr -d '\012' > $MARC_IMPORT_PATH.new
```

# give resulting file .mrc extension
# the database settings are in config/config.xml
```
time scripts/run-marc-processor.sh -d $DSPACE_HOME -i $MARC_IMPORT_PATH.new -o $MARC_IMPORT_PATH.mrc >& log
```

# check log for matched and unmatched records
time scripts/post-process-marc.sh $MARC_IMPORT_PATH log

# Please provide the resulting MARC file to colleagues on the Digital Repository and Discovery Services team.
