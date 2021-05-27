# DSpace Java Utilities

## ETD Utilities

### Requirements

- bash
- grep
- rsync
- Java Development Kit (OpenJDK 1.8 releases)
- DSpace 5.5 (Installed and running on a remote server environment)

### MARC Processor

#### Building

```bash
export DSPACE_HOME=/dspace
export DSPACE_LIB=`find $DSPACE_HOME/lib -name *.jar | tr '\n' ' '  | sed 's/ /\:/g'`
export JAVA_LOCAL_LIB=`find ./lib -name *.jar | tr '\n' ' '  | sed 's/ /\:/g'`
export JAVA_SOURCE_PATH=`find ./src -name *.java`

/usr/bin/env javac -d ./build -classpath "$DSPACE_LIB:$JAVA_LOCAL_LIB" $JAVA_SOURCE_PATH
```

This enhances the MARC records delivered from ProQuest with Handle IDs for graduate and doctoral electronic theses and dissertations (ETDs).

As this must be invoked from a server environment in which DSpace is installed, please ensure that `$DSPACE_HOME` is defined.

#### Running the Processor

Please set the environment variables:
```bash
export MARC_IMPORT_PATH="$HOME/marc_imports/proquest_import.mrc"
export MARC_INPUT_PATH="$HOME/marc_imports/proquest_import.bin"
export MARC_LOG_PATH="$HOME/marc_imports/proquest_import.log"
export MARC_RECORDS_PATH="$HOME/marc_imports/marc_export.mrc"
```

Transfer the batch of files over the SSH using `rsync` by first tunneling through the bastion host:

```bash
export DSPACE_PROXY_HOST="0.0.0.0" # This is only an example value
export DSPACE_PROXY_PORT=8080 # Example value
export DSPACE_HOST="127.0.0.1" # Example value
ssh -L $DSPACE_PROXY_PORT:$DSPACE_HOST:22 pulsys@$DSPACE_PROXY_HOST
```

...and then invoking `rsync` Within another terminal:
```bash
rsync --archive --update --verbose --compress --progress --rsh="ssh -p $DSPACE_PROXY_PORT" ./proquest_import.zip dspace@localhost:~/marc_imports/
```

Then, please remove line feed characters delimiting each records in the ProQuest binary file:
```bash
cat $MARC_IMPORT_PATH | tr -d '\012' > $MARC_INPUT_PATH
```

Following this, generate the MARC record batch file:
```bash
time scripts/run-marc-processor.sh >& $MARC_LOG_PATH
```

Please run the last post-processing procedures the MARC record batch:
```bash
time scripts/post-process-marc.sh
```

This will provide one with a report of the ARK URLs which may need to be manually inserted into the MARC records. One may then transfer the MARC records to one's local environment:
```bash
rsync --archive --update --verbose --compress --progress --rsh="ssh -p $DSPACE_PROXY_PORT" dspace@localhost:~/marc_imports/marc_export.mrc .
```

Please provide the resulting MARC record batch file to colleagues on the Digital Repository and Discovery Services team for import into the Integrated Library System (ILS) platform.
