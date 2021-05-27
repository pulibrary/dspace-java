package edu.princeton.dspace.etds;

import org.apache.commons.cli.*;
import org.w3c.dom.*;

import java.text.ParseException;
import java.util.*;
import java.text.*;
import java.util.zip.*;
import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

// IUSW_CHANGE - new class for Drop Box Processing

/**
 * This class can be run from the command-line to batch import a number of items from a drop box location. An XML
 * file inside the drop box location controls several parameters of the process.
 */
public class ETDImport {

    static Logger logger = Logger.getLogger(ETDImport.class);

    private static final String INCOMING_DIR_NAME = "incoming";
    private static final String INCOMING_UPDATE_DIR_NAME = "incoming_update";
    private static final String STAGING_DIR_NAME = "staging";
    private static final String HOLDING_DIR_NAME = "holding";
    private static final String REJECT_DIR_NAME = "reject";
    private static final String DUBLIN_CORE_FILE = "dublin_core.xml";

    private static final String PROQUEST_EMBARGO_CODE = "4";

    private static void usage(PrintStream out, String args[]) {
        logger.fatal("java " + ETDImport.class.getName() + "could not parse args: " + args.toString());
        out.println("java " + ETDImport.class.getName());
        out.println("\t[-c config_file] [-d dspace_home] -l location [-h] [-t] [-v] [-y]");
    }

    public static void main(String[] argv) {

        boolean verbose = false;
        boolean dryRun = false;
        File dropBoxFolder = null;
        File incomingFolder = null;
        File incomingUpdateFolder = null;
        File stagingFolder = null;
        File holdingFolder = null;
        File rejectFolder = null;
        int stopStep = -1;
        int step = 0;
        Vector<File> batches = new Vector<File>();
        Vector<File> updatebatches = new Vector<File>();
        Vector<File> rejects = new Vector<File>();
        Vector<File> existbutshouldnt = new Vector<File>();
        Vector<File> dontexistbutshould = new Vector<File>();

        //config.xml values
        String metadataTransformXSL = "";
        String schemaLocation = "";
        String filelistTransformXSL = "";
        String bitstreamTransformXSL = "";
        String bitstreamExtension = "";
        String authorlistTransformXSL = "";
        String eperson = "";
        String projectgrantnumber = "";
        Vector<String> adminEmails = new Vector<String>();
        String dspaceLoc = "";
        String dspaceHome = "/dspace";
        String configFile = "./config/etds/config.xml";

        // Hashtable for translating between department name sent by ProQuest and the
        //  collection ID in DSpace
        HashMap<String, String> collectionIDLookup = new HashMap<String, String>();

        /******************************************************************/
        /*  STEP 1 - process command line arguments and confirm folders   */
        /******************************************************************/
        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        options.addOption("c", "config", true, "config file - default " + configFile);
        options.addOption("d", "dspace_home", true, "full path of dspace_home directory, default " + dspaceHome);
        options.addOption("h", "help", false, "print help message");
        options.addOption("l", "location", true, "full path to drop box location (required)");
        options.addOption("s", "step", true, "stop at given step");
        options.addOption("v", "verbose", false, "print debug info (optional)");
        options.addOption("y", "dryRun", false, "dryRun - do not import items");
        CommandLine line = null;
        try {
            line = parser.parse(options, argv);
        } catch (Exception err) {
            logger.fatal("Error parsing command-line options.", err);
            usage(System.err, argv);
            System.exit(1);
        }
        if (line.hasOption('h')) {
            usage(System.out, argv);
            System.exit(0);
        }
        if (!line.hasOption('l')) {
            logger.fatal("No location parameter \"-l\" supplied. Program cannot continue.");
            System.exit(1);
        }
        if (line.hasOption('d')) {
            dspaceHome = line.getOptionValue('d');
        }
        if (line.hasOption('c')) {
            configFile = line.getOptionValue('c');
        }
        if (line.hasOption('s')) {
            System.out.println(line.getOptionValue('s'));
            stopStep = Integer.parseInt(line.getOptionValue('s'));
        }
        if (line.hasOption('v')) {
            verbose = true;
        }
        if (line.hasOption('y')) {
            dryRun = true;
        }
        String location = line.getOptionValue('l');

        logger.info("Starting the dropbox processor...");
        logger.info("Drop box folder location: " + location);

        //confirm folders
        dropBoxFolder = new File(location);
        if ((!dropBoxFolder.exists()) || (!dropBoxFolder.isDirectory()) || (!dropBoxFolder.canWrite())) {
            logger.fatal("Trouble reading drop box folder location (" + location + ")");
            System.exit(1);
        }
        incomingFolder = new File(location + File.separator + INCOMING_DIR_NAME);
        if ((!incomingFolder.exists()) || (!incomingFolder.isDirectory()) || (!incomingFolder.canWrite())) {
            logger.fatal("Trouble confirming incoming folder " + incomingFolder.getPath());
            System.exit(1);
        }
        incomingUpdateFolder = new File(location + File.separator + INCOMING_UPDATE_DIR_NAME);
        if ((!incomingUpdateFolder.exists()) || (!incomingUpdateFolder.isDirectory()) || (!incomingUpdateFolder.canWrite())) {
            logger.fatal("Trouble confirming incoming update folder " + incomingUpdateFolder.getPath());
            System.exit(1);
        }
        stagingFolder = new File(location + File.separator + STAGING_DIR_NAME);
        if ((!stagingFolder.exists()) || (!stagingFolder.isDirectory()) || (!stagingFolder.canWrite())) {
            logger.fatal("Trouble confirming staging folder" + stagingFolder.getPath());
            System.exit(1);
        }
        File stagingDeptFolder = new File(location + File.separator + STAGING_DIR_NAME + File.separator + "bydept");
        if (!stagingDeptFolder.exists()) {
            logger.info("Making directory " + stagingDeptFolder.getPath());
            stagingDeptFolder.mkdirs();
        }
        holdingFolder = new File(location + File.separator + HOLDING_DIR_NAME);
        if ((!holdingFolder.exists()) || (!holdingFolder.isDirectory()) || (!holdingFolder.canWrite())) {
            logger.fatal("Trouble confirming holding folder" + holdingFolder.getPath());
            System.exit(1);
        }
        rejectFolder = new File(location + File.separator + REJECT_DIR_NAME);
        if ((!rejectFolder.exists()) || (!rejectFolder.isDirectory()) || (!rejectFolder.canWrite())) {
            logger.fatal("Trouble confirming reject folder" + rejectFolder.getPath());
            System.exit(1);
        }

        /******************************************************/
        /*      STEP 2 - Read and parse config.xml file    */
        /******************************************************/
        step = checkStep(step, stopStep);
        try {
            Configuration config = new Configuration(configFile, dspaceHome);

            //config.xml values
            metadataTransformXSL = config.getMetadataTransformXSL();
            schemaLocation = config.getSchemaLocation();
            filelistTransformXSL = config.getFilelistTransformXSL();
            bitstreamTransformXSL = config.getBitstreamTransformXSL();
            bitstreamExtension = config.getBitstreamExtension();
            authorlistTransformXSL = config.getAuthorlistTransformXSL();
            eperson = config.getEperson();
            projectgrantnumber = config.getProjectgrantnumber();
            adminEmails = config.getAdminEmails();
            dspaceLoc = config.getDspaceLoc();

            // Hashtable for translating between department name sent by ProQuest and the
            //  collection ID in DSpace
            collectionIDLookup = config.getCollectionIDLookup();
        } catch (Exception err) {
            logger.fatal("Trouble reading collection.xml file", err);
            System.exit(1);
        }

        /*************************************/
		/*  STEP 3 - Read master mapfile     */
        /*************************************/
        step = checkStep(step, stopStep);
        logger.info("Reading master mapfile");

        File masterMapFile = new File(holdingFolder.getPath() + File.separator + "mapfile");
        if (!masterMapFile.exists()) {

            logger.info("Master mapfile does not exist; creating it");

            //create it from scratch
            try {
                masterMapFile.createNewFile();
            } catch (Exception err) {
                logger.error("Error creating master map list", err);
            }
        }
        //read lines from master mapfile to generate list of previously ingested folders

        logger.info("generating list of previously ingested folders");

        FileReader fr;
        BufferedReader br;
        String s;
        Vector<String> prevIngested = new Vector<String>();
        Vector<String> prevIngested2 = new Vector<String>();
        try {
            fr = new FileReader(masterMapFile);
            br = new BufferedReader(fr);
            while ((s = br.readLine()) != null) {
                if (!(s.trim().equals(""))) {
                    String[] xx = s.split(" ");
                    if (xx.length > 1) {
                        prevIngested.add(xx[0]);
                        prevIngested2.add(xx[1]);
                    }
                }
            }
            br.close();
            fr.close();
        } catch (Exception err) {
            logger.fatal("Error reading master mapfile", err);
            System.exit(1);
        }

        /*******************************************************/
		/*   STEP 4 - Unzip as needed in both incoming folders */
        /*******************************************************/
        step = checkStep(step, stopStep);
        logger.info("Examining drop box incoming folder for batches");
        unzipAsNeeded(incomingFolder, verbose);
        unzipAsNeeded(incomingUpdateFolder, verbose);

        /********************************************************/
		/* STEP 5 - Create vectors for new and existing batches */
        /********************************************************/
        step = checkStep(step, stopStep);
        logger.info("Create batch vectors");
        File[] allF = incomingFolder.listFiles();
        int counter = 0;
        while (counter < allF.length) {
            File x = allF[counter];
            if (x.isDirectory()) {

                logger.info("Folder found: " + x.getName());

                batches.add(x);
            }
            counter = counter + 1;
        }
        allF = incomingUpdateFolder.listFiles();
        counter = 0;
        while (counter < allF.length) {
            File x = allF[counter];
            if (x.isDirectory()) {

                logger.info("Update folder found: " + x.getName());

                updatebatches.add(x);
            }
            counter = counter + 1;
        }

        /***********************************************************/
		/* STEP 6 - Move (nonexisting) batches to staging directory */
        /***********************************************************/
        step = checkStep(step, stopStep);
        logger.info("Moving batches to staging directory");
        moveToStaging(batches, verbose, stagingFolder);

        /*********************************************************/
		/* STEP 7 - Process nonexisting batches from staging dir */
        /*********************************************************/
        step = checkStep(step, stopStep);
        logger.info("Processing (nonexisting) batches");
        counter = 0;
        while (counter < batches.size()) {
            File batch = batches.get(counter);
            File xmlFile = null;

            logger.info("Examing batch #" + (counter + 1) + ": " + batch.getName());

            //Check to see if batch already exists
            int subcounter = 0;
            boolean batchExists = false;
            while (subcounter < prevIngested.size()) {
                String folder = prevIngested.get(subcounter);
                if (batch.getName().equals(folder)) {
                    batchExists = true;
                }
                subcounter = subcounter + 1;
            }
            //in this case, batch shouldn't already exist
            if (batchExists) {
                logger.warn("Folder already exists: " + batch.getName());
                existbutshouldnt.add(batch);
                rejectBatch(batch, rejectFolder, verbose);
                counter = counter + 1;
                continue;
            }
            xmlFile = findXMLFileFor(batch);
            if (xmlFile == null) {
                logger.warn("ERROR: No xml file in batch " + batch.getName());
                rejects.add(batch);
                rejectBatch(batch, rejectFolder, verbose);
                counter = counter + 1;
                continue;
            }

            logger.info("Base batch XML file found: " + xmlFile.getName());


            //move schema files
            if (!schemaLocation.equals("")) {

                logger.info("Moving schema files");

                try {
                    moveSchema(schemaLocation, batch, dropBoxFolder);
                } catch (Exception err) {
                    logger.error("ERROR: Schema files cannot be read", err);
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
            }
            //metadata transformer to create dublin_core.xml file
            if (!metadataTransformXSL.equals("")) {
                try {
                    createDCFile(xmlFile, metadataTransformXSL, dropBoxFolder);
                } catch (Exception err) {
                    logger.error("ERROR generating dublin core.xml", err);
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
            } else {
                if (!(xmlFile.getName().equals(DUBLIN_CORE_FILE))) {
                    xmlFile.renameTo(new File(xmlFile.getParent() + File.separator + DUBLIN_CORE_FILE));
                }

                logger.info("Skipping metadata transform - XML is already in DC format");

            }

            // Create the metadata_pu.xml file that holds the project grant number
            createMetaDataPUFile(batch, projectgrantnumber);

            //next generate bitstream if neccesary
            if (!(bitstreamTransformXSL.equals(""))) {

                logger.info("Transforming " + xmlFile.getName() + " into bitstream file using " + bitstreamTransformXSL);

                try {
                    createBitstream(xmlFile, bitstreamTransformXSL, dropBoxFolder, bitstreamExtension);
                } catch (Exception err) {
                    System.out.println("ERROR generating bitstream");
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
            }
            //next generate authorlist if neccesary
            if (!(authorlistTransformXSL.equals(""))) {

                logger.info("Transforming " + xmlFile.getName() + " into authorlist file using " + authorlistTransformXSL);

                try {
                    createAuthorlist(xmlFile, authorlistTransformXSL, dropBoxFolder);
                } catch (Exception err) {
                    logger.error("ERROR generating bitstream", err);
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
            }
            //next, generate 'contents' file

            logger.info("Generating contents file");

            File contentsFile = new File(xmlFile.getParent() + File.separator + "contents");
            if (!contentsFile.exists()) {
                //assumes that if contents file is present it's already correct
                try {
                    contentsFile.createNewFile();
                } catch (Exception err) {

                    logger.error("ERROR generating contents file: " + contentsFile.getPath(), err);
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
                if (filelistTransformXSL.equals("")) {
                    //no transform specified, so just list everything in the folder except hidden files, the xml, dtd, and ent files, and the contents file itself

                    logger.info("No file transform specified; reading folder contents");

                    File[] batchFiles = batch.listFiles();
                    subcounter = 0;
                    FileWriter fwriter = null;
                    BufferedWriter writer = null;
                    boolean error = false;
                    try {
                        fwriter = new FileWriter(contentsFile);
                        writer = new BufferedWriter(fwriter);
                        while (subcounter < batchFiles.length) {
                            File file = batchFiles[subcounter];
                            if (!((file.getName().startsWith(".")) ||
                                    (file.getName().toLowerCase().endsWith("xml")) ||
                                    (file.getName().toLowerCase().endsWith("dtd")) ||
                                    (file.getName().toLowerCase().endsWith("ent")) ||
                                    (file.getName().toLowerCase().equals("contents")))) {
                                if (file.isDirectory()) {
                                    File[] subfiles = file.listFiles();
                                    int subsubcounter = 0;
                                    while (subsubcounter < subfiles.length) {
                                        //add file to contents file
                                        //TODO currently can only go in one level!
                                        if (subfiles[subsubcounter].isDirectory()) {
                                            error = true;
                                        } else {
                                            writer.write(file.getName() + File.separator + subfiles[subsubcounter].getName());
                                            writer.newLine();
                                            writer.flush();
                                        }
                                        subsubcounter = subsubcounter + 1;
                                    }
                                } else {
                                    //add file to contents file
                                    writer.write(file.getName());
                                    writer.newLine();
                                    writer.flush();
                                }
                            }
                            subcounter = subcounter + 1;
                        }
                        writer.close();
                        fwriter.close();
                    } catch (Exception err) {
                        if (writer != null) {
                            try {
                                writer.close();
                            } catch (Exception er) {
                            }
                        }
                        if (fwriter != null) {
                            try {
                                fwriter.close();
                            } catch (Exception er) {
                            }
                        }

                        logger.error("ERROR writing content file", err);
                        rejects.add(batch);
                        rejectBatch(batch, rejectFolder, verbose);
                        counter = counter + 1;
                        continue;
                    }
                    if (error) {
                        logger.error("ERROR batch contains nonempty subfolders");
                        rejects.add(batch);
                        rejectBatch(batch, rejectFolder, verbose);
                        counter = counter + 1;
                        continue;
                    }
                } else {

                    logger.info("generating contents file using XSL");

                    //do the transformation to generate the contents list
                    File xslFile = null;
                    try {
                        xslFile = openFile(filelistTransformXSL, dropBoxFolder.getPath());
                        if ((!xslFile.isFile())) {
                            logger.error("ERROR: not a file: " + xslFile.getPath());
                            rejects.add(batch);
                            rejectBatch(batch, rejectFolder, verbose);
                            counter = counter + 1;
                            continue;
                        }
                    } catch (Exception err) {
                        logger.error("ERROR: XSL file cannot be read: " + xslFile.getPath(), err);
                        rejects.add(batch);
                        rejectBatch(batch, rejectFolder, verbose);
                        counter = counter + 1;
                        continue;
                    }

                    logger.info("Transforming " + xmlFile.getName() + " into contents file using " + xslFile.getName());

                    //run the contents transformation
                    try {
                        ETDImport.doTransform(xslFile.getPath(), xmlFile.getPath(), xmlFile.getParent() + File.separator + "contents");
                    } catch (Exception err) {

                        logger.error("ERROR generating transformation for batch " + batch.getName(), err);
                        rejects.add(batch);
                        rejectBatch(batch, rejectFolder, verbose);
                        counter = counter + 1;
                        continue;
                    }
                }
            }
            counter = counter + 1;
        }

        if (dryRun) {
            logger.info("Exiting before importing items!");
            System.exit(2);
        }

        /*********************************************************/
		/* STEP 8 - Import Items */
        /*********************************************************/
        step = checkStep(step, stopStep);
        logger.info("Running ItemImport command on batches...");


        //Before importing we must group batches by department collection ID
        String[] collection_ids = groupBatchesByDepartment(batches, collectionIDLookup);

        PrintStream oldOut = System.out;
        PrintStream oldErr = System.err;

        //TODO: redirecting System.out/err to log4j appender
        System.setOut(new PrintStream(new LoggingOutputStream(logger, Level.INFO), true));
        System.setErr(new PrintStream(new LoggingOutputStream(logger, Level.ERROR), true));


        Vector<String> mappings = new Vector<String>();
        for (int i = 0; i < collection_ids.length; i++) {
            String collectionFolder = stagingFolder.getPath() + File.separator + "bydept" + File.separator + collection_ids[i].replace(File.separatorChar, '_');

            // remove mapfile in case it exists
            File mapfile = new File(collectionFolder + File.separator + "mapfile");
            if (mapfile.exists()) {
                if (!mapfile.delete()) {
                    System.out.println("ERROR: Mapfile cannot be deleted!");
                }
            }

            //run item import on batches
            String args[] = new String[]{"--add", "--eperson=" + eperson, "--collection=" + collection_ids[i],
                    "--source=" + collectionFolder,
                    "--mapfile=" + mapfile.getPath()};
            int result = executeCommand(dspaceHome + File.separator + "bin/dspace import", args, stopStep == step);
            if (0 != result) {
                logger.error("Failed batch import for collection " + collection_ids[i]);
            } else {

                //append to a master mapfile and to the overall map for email
                //Vector<String> mappings = new Vector<String>();
                File mapFile = new File(collectionFolder + File.separator + "mapfile");
                //insert new lines in master mapfile
                logger.info("Appending to mapFile " + mapFile.getAbsolutePath() + " to " + masterMapFile.getPath());
                try {
                    FileWriter fwriter = null;
                    BufferedWriter writer = null;
                    fwriter = new FileWriter(masterMapFile, true);
                    writer = new BufferedWriter(fwriter);
                    fr = new FileReader(mapFile);
                    br = new BufferedReader(fr);
                    while ((s = br.readLine()) != null) {
                        writer.append(s);
                        writer.newLine();
                        mappings.add(s);
                    }
                    br.close();
                    fr.close();
                    fwriter.flush();
                    writer.flush();
                    fwriter.close();
                    writer.close();
                } catch (Exception err) {
                    logger.error("Error appending to master mapfile " + mapFile.getAbsolutePath(), err);
                }
            }

            step = checkStep(step, stopStep);


        }


        //restoring original System.out/err
        System.setOut(oldOut);
        System.setErr(oldErr);

        /*********************************************************/
		/* STEP  -cleanup and archive*/
        /*********************************************************/
        step = checkStep(step, stopStep);

        //check staging dir for folders
        File[] batchFolders = stagingFolder.listFiles();
        counter = 0;
        while (counter < batchFolders.length) {
            File batchFolder = batchFolders[counter];
            if (batchFolder.isFile()) {
                counter = counter + 1;
                continue;
            }

            logger.info("Cleaning up " + batchFolders[counter].getName());

            //append to the master author list
            if (!authorlistTransformXSL.equals("")) {

                logger.info("Appending to the master author list");

                File masterAuthorList = new File(holdingFolder.getPath() + File.separator + "aulist.xml");
                if (!masterAuthorList.exists()) {

                    logger.info("Master list does not exist; creating it");

                    //create it from scratch
                    try {
                        masterAuthorList.createNewFile();
                        FileWriter fwriter = null;
                        BufferedWriter writer = null;
                        fwriter = new FileWriter(masterAuthorList);
                        writer = new BufferedWriter(fwriter);
                        writer.write("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?>");
                        writer.newLine();
                        writer.flush();
                        writer.close();
                        fwriter.close();
                    } catch (Exception err) {
                        logger.error("Error creating master author list", err);
                    }
                }
                //insert new lines in author list
                try {
                    FileWriter fwriter = null;
                    BufferedWriter writer = null;
                    fwriter = new FileWriter(masterAuthorList, true);
                    writer = new BufferedWriter(fwriter);
                    fr = new FileReader(batchFolder.getPath() + File.separator + "aulist.xml");
                    br = new BufferedReader(fr);
                    while ((s = br.readLine()) != null) {
                        writer.append(s);
                        writer.newLine();
                    }
                    br.close();
                    fr.close();
                    fwriter.flush();
                    writer.flush();
                    fwriter.close();
                    writer.close();
                } catch (Exception err) {

                    logger.error("Error appending to master author list", err);
                }
            }
            //create zip file

            logger.info("Creating zip file");

            File zipped = new File(holdingFolder.getPath() + File.separator + batchFolder.getName() + ".zip");
            try {
                ETDImport.zip(batchFolder, zipped);
            } catch (Exception err) {

                logger.error("Error creating zip file", err);
            }
            counter = counter + 1;
        }


        /***********************************************************/
		/* STEP - delete everything in staging dir */
        /***********************************************************/
        step = checkStep(step, stopStep);
        batches = null;
        //to ensure file refs are gone
        System.gc();
        deleteDirectory(stagingFolder);
        stagingFolder.mkdirs();

        if (false) {
            logger.info("Exiting before processing existing batches!");
            System.exit(1);
        }

        /***********************************************************/
		/* STEP 8 - Move (existing) batches to staging directory */
        /***********************************************************/
        step = checkStep(step, stopStep);
        //first confirm staging dir successfully deleted
        if (stagingFolder.listFiles().length != 0) {
            System.out.println("ERROR: Staging dir not successfully deleted");
            System.exit(1);
        }
        if (verbose) {
            System.out.println("Moving update batches to staging directory");
        }
        moveToStaging(updatebatches, verbose, stagingFolder);

        /*********************************************************/
		/* STEP 9 - Process existing batches from staging dir */
        /*********************************************************/
        step = checkStep(step, stopStep);
        if (verbose) {
            System.out.println("Processing (existing) batches");
        }
        counter = 0;
        Vector<String> existMappings = new Vector<String>();
        while (counter < updatebatches.size()) {
            File batch = updatebatches.get(counter);
            File xmlFile = null;
            if (verbose) {
                System.out.println("Examing batch #" + (counter + 1) + ": " + batch.getName());
            }
            //Check to see if batch already exists and create mapping file for email
            int subcounter = 0;
            boolean batchExists = false;
            while (subcounter < prevIngested.size()) {
                String folder = prevIngested.get(subcounter);
                if (batch.getName().equals(folder)) {
                    batchExists = true;
                    existMappings.add(prevIngested.get(subcounter) + " " + prevIngested2.get(subcounter));
                }
                subcounter = subcounter + 1;
            }
            //in this case, batch SHOULD already exist
            if (!batchExists) {
                System.out.println("Folder does not already exist: " + batch.getName());
                dontexistbutshould.add(batch);
                rejectBatch(batch, rejectFolder, verbose);
                counter = counter + 1;
                continue;
            }
            xmlFile = findXMLFileFor(batch);
            if (xmlFile == null) {
                System.out.println("ERROR: No xml file in batch " + batch.getName());
                rejects.add(batch);
                rejectBatch(batch, rejectFolder, verbose);
                counter = counter + 1;
                continue;
            }
            if (verbose) {
                System.out.println("Base batch XML file found: " + xmlFile.getName());
            }
            //move schema files
            if (!schemaLocation.equals("")) {
                if (verbose) {
                    System.out.println("Moving schema files");
                }
                try {
                    moveSchema(schemaLocation, batch, dropBoxFolder);
                } catch (Exception err) {
                    err.printStackTrace();
                    System.out.println("ERROR: Schema files cannot be read");
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
            }
            //metadata transformer to create dublin_core.xml file
            if (!metadataTransformXSL.equals("")) {
                try {
                    createDCFile(xmlFile, metadataTransformXSL, dropBoxFolder);
                } catch (Exception err) {
                    System.out.println("ERROR generating dublin core.xml");
                    err.printStackTrace();
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
            } else {
                if (!(xmlFile.getName().equals(DUBLIN_CORE_FILE))) {
                    xmlFile.renameTo(new File(xmlFile.getParent() + File.separator + DUBLIN_CORE_FILE));
                }
                if (verbose) {
                    System.out.println("Skipping metadata transform - XML is already in DC format");
                }
            }

            // Create the metadata_pu.xml file that holds the project grant number
            createMetaDataPUFile(batch, projectgrantnumber);

            //next generate bitstream if neccessary
            if (!(bitstreamTransformXSL.equals(""))) {
                if (verbose) {
                    System.out.println("Transforming " + xmlFile.getName() + " into bitstream file using " + bitstreamTransformXSL);
                }
                try {
                    createBitstream(xmlFile, bitstreamTransformXSL, dropBoxFolder, bitstreamExtension);
                } catch (Exception err) {
                    System.out.println("ERROR generating bitstream");
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
            }
            //no need to generate authorlist for existing records
            //next, generate 'contents' file
            if (verbose) {
                System.out.println("Generating contents file");
            }
            File contentsFile = new File(xmlFile.getParent() + File.separator + "contents");
            if (!contentsFile.exists()) {
                //assumes that if contents file is present it's already correct
                try {
                    contentsFile.createNewFile();
                } catch (Exception err) {
                    err.printStackTrace();
                    System.out.println("ERROR generating contents file: " + contentsFile.getPath());
                    rejects.add(batch);
                    rejectBatch(batch, rejectFolder, verbose);
                    counter = counter + 1;
                    continue;
                }
                if (filelistTransformXSL.equals("")) {
                    //no transform specified, so just list everything in the folder except the xml, dtd, and ent files, and the contents file itself
                    if (verbose) {
                        System.out.println("No file transform specified; reading folder contents");
                    }
                    File[] batchFiles = batch.listFiles();
                    subcounter = 0;
                    FileWriter fwriter = null;
                    BufferedWriter writer = null;
                    boolean error = false;
                    try {
                        fwriter = new FileWriter(contentsFile);
                        writer = new BufferedWriter(fwriter);
                        while (subcounter < batchFiles.length) {
                            File file = batchFiles[subcounter];
                            if (!((file.getName().startsWith(".")) ||
                                    (file.getName().toLowerCase().endsWith("xml")) ||
                                    (file.getName().toLowerCase().endsWith("dtd")) ||
                                    (file.getName().toLowerCase().endsWith("ent")) ||
                                    (file.getName().toLowerCase().equals("contents")))) {
                                if (file.isDirectory()) {
                                    File[] subfiles = file.listFiles();
                                    int subsubcounter = 0;
                                    while (subsubcounter < subfiles.length) {
                                        //add file to contents file
                                        //TODO currently can only go in one level!
                                        if (subfiles[subsubcounter].isDirectory()) {
                                            error = true;
                                        } else {
                                            writer.write(file.getName() + File.separator + subfiles[subsubcounter].getName());
                                            writer.newLine();
                                            writer.flush();
                                        }
                                        subsubcounter = subsubcounter + 1;
                                    }
                                } else {
                                    //add file to contents file
                                    writer.write(file.getName());
                                    writer.newLine();
                                    writer.flush();
                                }
                            }
                            subcounter = subcounter + 1;
                        }
                        writer.close();
                        fwriter.close();
                    } catch (Exception err) {
                        if (writer != null) {
                            try {
                                writer.close();
                            } catch (Exception er) {
                            }
                        }
                        if (fwriter != null) {
                            try {
                                fwriter.close();
                            } catch (Exception er) {
                            }
                        }
                        err.printStackTrace();
                        System.out.println("ERROR writing content file");
                        rejects.add(batch);
                        rejectBatch(batch, rejectFolder, verbose);
                        counter = counter + 1;
                        continue;
                    }
                    if (error) {
                        System.out.println("ERROR batch contains nonempty subfolders");
                        rejects.add(batch);
                        rejectBatch(batch, rejectFolder, verbose);
                        counter = counter + 1;
                        continue;
                    }
                } else {
                    if (verbose) {
                        System.out.println("generating contents file using XSL");
                    }
                    //do the transformation to generate the contents list
                    File xslFile = null;
                    try {
                        xslFile = openFile(filelistTransformXSL, dropBoxFolder.getPath());
                        if (!xslFile.isFile()) {
                            System.out.println("ERROR: not a file: " + xslFile.getPath());
                            rejects.add(batch);
                            rejectBatch(batch, rejectFolder, verbose);
                            counter = counter + 1;
                            continue;
                        }
                    } catch (Exception err) {
                        err.printStackTrace();
                        System.out.println("ERROR: XSL file cannot be read: " + xslFile.getPath());
                        rejects.add(batch);
                        rejectBatch(batch, rejectFolder, verbose);
                        counter = counter + 1;
                        continue;
                    }
                    if (verbose) {
                        System.out.println("Transforming " + xmlFile.getName() + " into contents file using " + xslFile.getName());
                    }
                    //run the contents transformation
                    try {
                        ETDImport.doTransform(xslFile.getPath(), xmlFile.getPath(), xmlFile.getParent() + File.separator + "contents");
                    } catch (Exception err) {
                        err.printStackTrace();
                        System.out.println("ERROR generating transformation for batch " + batch.getName());
                        rejects.add(batch);
                        rejectBatch(batch, rejectFolder, verbose);
                        counter = counter + 1;
                        continue;
                    }
                }
            }

            counter = counter + 1;
        }
        if (verbose) {
            System.out.println("Running ItemImport command on (existing) batches...");
        }
        //run item import on existing batches
        try {
            // ItemImport.main(new String[]{"--replace", "--eperson=" + eperson, "--collection=" + handle, "--source=" + stagingFolder, "--mapfile=" + masterMapFile.getPath(), "--embargo=" + embargoMonths});
        } catch (Exception err) {
            err.printStackTrace();
            System.out.println("ERROR running ItemImport for batches");
        }

        step = checkStep(step, stopStep);
        //////////////////////
        //cleanup and archive/
        //////////////////////
        //check staging dir for folders
        batchFolders = stagingFolder.listFiles();
        counter = 0;
        while (counter < batchFolders.length) {
            File batchFolder = batchFolders[counter];
            if (batchFolder.isFile()) {
                counter = counter + 1;
                continue;
            }
            if (verbose) {
                System.out.println("Cleaning up " + batchFolders[counter].getName());
            }
            //create new zip file
            if (verbose) {
                System.out.println("Creating zip file");
            }
            File zipped = new File(holdingFolder.getPath() + File.separator + batchFolder.getName() + ".zip");
            while (zipped.exists()) {
                String newone = zipped.getPath() + ".new";
                zipped = new File(newone);
            }
            try {
                ETDImport.zip(batchFolder, zipped);
            } catch (Exception err) {
                err.printStackTrace();
                System.out.println("Error creating zip file");
            }
            counter = counter + 1;
        }

        //delete everything in staging dir
        batches = null;
        //to ensure file refs are gone
        System.gc();
        deleteDirectory(stagingFolder);
        stagingFolder.mkdirs();

        ////////////////////////////////////////
        //email results to all participants, if there's anything to email
        if ((mappings.size() > 0) || (existMappings.size() > 0) || (rejects.size() > 0) || (existbutshouldnt.size() > 0) || (dontexistbutshould.size() > 0)) {
            String header = "Batch processing complete";
            String message = "Batch processing is complete\n\n";
            counter = 0;
            if (mappings.size() > 0) {
                message = message + "The following items have been imported and may be accessed using the links below:\n\n";
                while (counter < mappings.size()) {
                    String[] x = mappings.get(counter).split(" ");
                    message = message + dspaceLoc + x[1] + " (" + x[0] + ") \n";
                    counter = counter + 1;
                }
                message = message + "\n";
            }
            counter = 0;
            if (existMappings.size() > 0) {
                message = message + "The following items already exist in the repository and have been updated:\n\n";
                while (counter < existMappings.size()) {
                    String[] x = existMappings.get(counter).split(" ");
                    message = message + dspaceLoc + x[1] + " (" + x[0] + ") \n";
                    counter = counter + 1;
                }
                message = message + "\n";
            }
            counter = 0;
            if (rejects.size() > 0) {
                message = message + "NOTE: The following batches were rejected and were not processed\n\n";
                while (counter < rejects.size()) {
                    message = message + rejects.get(counter).getName() + "\n";
                    counter = counter + 1;
                }
                message = message + "\n";
            }
            counter = 0;
            if (existbutshouldnt.size() > 0) {
                message = message + "NOTE: The following batches were rejected because they may already be present in the repository (check the folder name)\n\n";
                while (counter < existbutshouldnt.size()) {
                    message = message + existbutshouldnt.get(counter).getName() + "\n";
                    counter = counter + 1;
                }
                message = message + "\n";
            }
            if (dontexistbutshould.size() > 0) {
                message = message + "NOTE: The following batches were rejected because they were placed in the update folder, but didn't already exist\n\n";
                while (counter < dontexistbutshould.size()) {
                    message = message + dontexistbutshould.get(counter).getName() + "\n";
                    counter = counter + 1;
                }
                message = message + "\n";
            }
            message = message + "Please send an email to " + adminEmails.get(0) + " if you have any questions or problems.\n\n";

            /* this used to be done by email - we are dumping to stdout */
            System.out.println("You should know:");
            System.out.println(message);
        }

        logger.info("Dropbox Processor finished successfully");

    }

    private static void createAuthorlist(File xmlFile, String authorlistTransformXSL, File dropBoxFolder) throws Exception {
        File xslFile = openFile(authorlistTransformXSL, dropBoxFolder.getPath());
        if (!xslFile.isFile()) {
            throw new Exception("nota file: " + authorlistTransformXSL);
        }
        //run the authorlist transformation
        ETDImport.doTransform(xslFile.getPath(), xmlFile.getPath(), xmlFile.getParent() + File.separator + "aulist.xml");
    }

    private static void createBitstream(File xmlFile, String bitstreamTransformXSL, File dropBoxFolder, String bitstreamExtension) throws Exception {
        File xslFile = openFile(bitstreamTransformXSL, dropBoxFolder.getPath());
        if (!xslFile.isFile()) {
            throw new Exception("nota file: " + bitstreamTransformXSL);
        }
        //run the bitstream transformation
        ETDImport.doTransform(xslFile.getPath(), xmlFile.getPath(), xmlFile.getParent() + File.separator + xmlFile.getName().substring(0, xmlFile.getName().length() - 4) + "." + bitstreamExtension);
    }


    private static void createDCFile(File xmlFile, String metadataTransformXSL, File dropBoxFolder) throws Exception {
        if (xmlFile.getName().equals(DUBLIN_CORE_FILE)) {
            throw new Exception("can't handle " + DUBLIN_CORE_FILE);
        }
        File xslFile = openFile(metadataTransformXSL, dropBoxFolder.getPath());
        if (!xmlFile.isFile()) {
            throw new Exception(metadataTransformXSL + " not a file");
        }
        //run the metadata transformation
        ETDImport.doTransform(xslFile.getPath(), xmlFile.getPath(), xmlFile.getParent() + File.separator + DUBLIN_CORE_FILE);
    }

    private static void moveSchema(String schemaLocation, File batch, File dropBoxFolder) throws Exception {
        File schemaFolder = openFile(schemaLocation, dropBoxFolder.getPath());
        if (!schemaFolder.isDirectory()) {
            throw new Exception(schemaLocation + " not a directory");
        }
        copyFolderContents(schemaFolder, batch);
    }

    private static File findXMLFileFor(File batch) {
        File[] files = batch.listFiles();
        if (files == null) {
            return null;
        }
        int subcounter = 0;
        while (subcounter < files.length) {
            File file = files[subcounter];
            if (file.isFile()) {
                //for now, we'll just assume that any XML file in the batch is the right one
                if (file.getName().toLowerCase().endsWith("xml") &&
                        !file.getName().equals("aulist.xml") &&
                        !file.getName().equals("dublin_core.xml") &&
                        !file.getName().equals("metadata_pu.xml")
                        ) {
                    return file;
                }
            }
            subcounter = subcounter + 1;
        }
        return null;
    }

    private static void moveToStaging(Vector<File> batches, boolean verbose, File stagingFolder) {
        int counter = 0;

        while (counter < batches.size()) {
            File batch = batches.get(counter);


            logger.info("Moving " + batch.getName());

            try {
                copyFolder(batch, stagingFolder);

                logger.info("Deleting " + batch.getName() + " from incoming folder");

                deleteDirectory(batch);
                batches.set(counter, new File(stagingFolder + File.separator + batch.getName()));
            } catch (Exception err) {
                logger.error("Error moving " + batch.getName() + " to staging directory", err);
            }
            counter = counter + 1;
        }

    }

    private static void unzipAsNeeded(File incomingFolder, boolean verbose) {
        File[] incomingFolderContents = incomingFolder.listFiles();
        int counter = 0;

        logger.info("Looking for zip files");

        while (counter < incomingFolderContents.length) {
            File x = incomingFolderContents[counter];
            if (x.isFile()) {
                if (ETDImport.getExtension(x.getName()).toLowerCase().equals("zip")) {
                    try {

                        logger.info("Zip file found: " + x.getName());

                        //file is zip file; parse it
                        ZipFile zipFile = new ZipFile(x, ZipFile.OPEN_READ);
                        //are there ONLY directories in the zip file?
                        boolean onlydirectories = true;
                        Enumeration zipFileEntries = zipFile.entries();
                        while (zipFileEntries.hasMoreElements()) {
                            ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();
                            String currentEntry = entry.getName();
                            if (!currentEntry.contains("/")) {
                                onlydirectories = false;
                            }
                        }
                        if (!onlydirectories) {
                            //assume that file is a group of files zipped directly, without a parent folder

                            logger.info("File has no directory structure; creating a new directory");

                            //make directory with same name as zip file
                            File newZipDir = new File(incomingFolder + File.separator + x.getName().substring(0, x.getName().length() - 4));
                            if (newZipDir.exists()) {
                                logger.error("Unable to unzip " + x.getName() + " since a folder with that name already exists");
                            } else {
                                //unzip files into newly created folder

                                logger.info("Unzipping " + x.getName() + " into newly created folder " + newZipDir.getPath());

                                ETDImport.unzip(x, newZipDir);
                                //delete original

                                logger.info("Deleting " + x.getName());

                                //gChMod(x);
                                System.gc();
                                if (!x.delete()) {
                                    logger.warn("warning; cannot delete " + x.getPath());
                                }
                            }
                        } else {
                            //file is one or more batches grouped together; unzip them all into drop box folder directly

                            logger.info("Unzipping " + x.getName() + " into " + incomingFolder.getPath());

                            ETDImport.unzip(x, incomingFolder);
                            //delete original

                            logger.info("Deleting " + x.getName());

                            //gChMod(x);
                            System.gc();
                            if (!x.delete()) {
                                logger.warn("warning; cannot delete " + x.getPath());
                            }
                        }
                    } catch (Exception err) {
                        //non-fatal exception; keep going
                        logger.error("Error attempting to unzip file " + x.getName(), err);
                    }
                }
            }
            counter = counter + 1;
        }
    }


    private static void rejectBatch(File batch, File rejectFolder, boolean verbose) {
        try {

            logger.info("Batch " + batch.getName() + " not accepted: moving to reject folder");

            copyFolder(batch, rejectFolder);
            deleteDirectory(batch);
        } catch (Exception err) {
            logger.error("Error moving " + batch.getName() + " to reject folder", err);
        }
    }

    private static void deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                } else {
                    //gChMod(files[i]);
                    System.gc();
                    if (!files[i].delete()) {
                        logger.warn("warning; cannot delete " + files[i].getPath());
                    }
                }
            }
        }
        //gChMod(path);
        System.gc();
        if (!path.delete()) {
            logger.warn("warning; cannot delete " + path.getPath());
        }
    }

    /*
     * Note this method calls an IU specific program, gchmod, which changes permissions
     * on items which may have been uploaded with the wrong permissions. If
     * this is being used at another instutution, or in situations where this program
     * isn't available, this step will not throw an error, but will simply do nothing
     */
    private static void gChMod(File x) {
        if (!x.exists()) {
            return;
        }
        File gchmodfile = new File("/usr/local/bin/gchmod");
        if (!gchmodfile.exists()) {
            return;
        }
        String command = "/usr/local/bin/gchmod +w " + x.getPath();
        try {
            Process p = Runtime.getRuntime().exec(command);
            p.waitFor();
            p.destroy();
        } catch (Exception er) {
            er.printStackTrace();
            System.out.println("Error changing permissions");
        }

    }

    private static void copyFolder(File folderToCopy, File newParent) throws IOException {
        if (!folderToCopy.isDirectory()) {
            throw new IOException("Source is not a directory");
        }

        if (!newParent.isDirectory()) {
            throw new IOException("Destination is not a directory");
        }
        File copiedFolder = new File(newParent.getPath() + File.separator + folderToCopy.getName());
        if (copiedFolder.exists()) {
            throw new IOException("Destination file already exists");
        }
        if (!copiedFolder.mkdirs()) {
            throw new IOException("Error creating destination folders");
        }
        int counter = 0;
        logger.info("Copying " + folderToCopy.getPath() + " to " + newParent.getPath());
        while (counter < folderToCopy.listFiles().length) {
            File x = folderToCopy.listFiles()[counter];
            if (x.isDirectory()) {
                copyFolder(x, copiedFolder);
            } else {
                File newX = new File(copiedFolder.getPath() + File.separator + x.getName());
                newX.createNewFile();
                InputStream in = new FileInputStream(x);
                OutputStream out = new FileOutputStream(newX);
                byte[] buf = new byte[2048];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
                in.close();
                out.close();
            }
            counter = counter + 1;
        }
    }

    private static void copyFolderContents(File folderToCopy, File newParent) throws IOException {
        logger.debug("copyFolder from " + folderToCopy.getPath() + " to " + newParent.getPath());
        if (!folderToCopy.isDirectory()) {
            throw new IOException("Source is not a directory");
        }
        if (!newParent.isDirectory()) {
            throw new IOException("Destination is not a directory");
        }
        int counter = 0;
        while (counter < folderToCopy.listFiles().length) {
            File x = folderToCopy.listFiles()[counter];
            if (x.isDirectory()) {
                copyFolder(x, newParent);
            } else {
                File newX = new File(newParent.getPath() + File.separator + x.getName());
                logger.debug("copy file to  " + newX.getPath());
                newX.createNewFile();
                InputStream in = new FileInputStream(x);
                OutputStream out = new FileOutputStream(newX);
                byte[] buf = new byte[2048];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
                in.close();
                out.close();
            }
            counter = counter + 1;
        }
    }

    private static void zip(File folderToZip, File zippedFolder) throws IOException {
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zippedFolder));
        addDir(folderToZip, out);
        out.close();
    }

    private static void addDir(File dirObj, ZipOutputStream out) throws IOException {
        File[] files = dirObj.listFiles();
        byte[] tmpBuf = new byte[1024];

        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                addDir(files[i], out);
                continue;
            }
            FileInputStream in = new FileInputStream(files[i].getAbsolutePath());
            out.putNextEntry(new ZipEntry(files[i].getAbsolutePath()));
            int len;
            while ((len = in.read(tmpBuf)) > 0) {
                out.write(tmpBuf, 0, len);
            }
            out.closeEntry();
            in.close();
        }
    }

    private static void unzip(File zippedFile, File locationForUnzip) throws IOException {
        ZipFile zipFile = new ZipFile(zippedFile, ZipFile.OPEN_READ);
        Enumeration zipFileEntries = zipFile.entries();
        while (zipFileEntries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();
            String currentEntry = entry.getName();
            File destFile = new File(locationForUnzip, currentEntry);
            File destinationParent = destFile.getParentFile();
            destinationParent.mkdirs();
            if (!entry.isDirectory()) {
                BufferedInputStream is = new BufferedInputStream(zipFile.getInputStream(entry));
                int currentByte;
                byte data[] = new byte[2048];
                FileOutputStream fos = new FileOutputStream(destFile);
                BufferedOutputStream dest = new BufferedOutputStream(fos, 2048);
                while ((currentByte = is.read(data, 0, 2048)) != -1) {
                    dest.write(data, 0, currentByte);
                }
                dest.flush();
                dest.close();
                is.close();
            }
        }
        zipFile.close();
    }

    private static String getExtension(String string) {
        String extension = "";
        int i = string.lastIndexOf('.');
        if ((i > 0) && (i < string.length() - 1)) {
            extension = string.substring(i + 1);
        }
        return extension;
    }

    private static void doTransform(String xslFile, String inputFile, String outputFile) throws Exception {
        TransformerFactory tFactory = TransformerFactory.newInstance();
        Transformer transformer = tFactory.newTransformer(new javax.xml.transform.stream.StreamSource(xslFile));
        FileOutputStream of = new FileOutputStream(outputFile);
        transformer.transform(new javax.xml.transform.stream.StreamSource(inputFile), new javax.xml.transform.stream.StreamResult(of));
        of.close();
    }

    private static String getDepartmentName(File xmlfile) {
        String deptName = null;

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlfile);

            NodeList inst_contact = doc.getElementsByTagName("DISS_inst_contact");
            deptName = inst_contact.item(0).getTextContent();
        } catch (Exception e) {
            logger.error("Trouble finding department name in xml file: " + xmlfile.getName(), e);
        }

        return deptName;
    }

    private static String getEmbargoCode(File xmlfile) {
        String embargoCode = null;

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlfile);

            NodeList root_node = doc.getElementsByTagName("DISS_submission");
            Node node = root_node.item(0);
            if (node instanceof Element) {
                Element el = (Element) node;
                embargoCode = el.getAttribute("embargo_code");
            }

        } catch (Exception e) {
            logger.error("Trouble finding embargo_code in xml file: " + xmlfile.getName(), e);
        }

        return embargoCode;
    }

    private static Date getEmbargoEnd(File xmlfile) {
        String endDateString = null;
        Date endDate = null;

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlfile);

            NodeList root_node = doc.getElementsByTagName("DISS_sales_restriction");
            Node node = root_node.item(0);
            if (node instanceof Element) {
                Element el = (Element) node;
                endDateString = el.getAttribute("remove");
                logger.info("Embargo endDate " + endDateString);
            }
            try {
                DateFormat format = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH);
                endDate = format.parse(endDateString);
                logger.info("Embargo endDate (parsed)" + endDate);

            } catch (ParseException e) {
                logger.error("Can't parse embargo endDate " + endDateString, e);
            }
            return endDate;
        } catch (Exception e) {
            logger.error("Can't find embargo endDate in xml file: " + xmlfile.getName(), e);
            return null;
        }
    }

    /**
     * Create the metadata_pu.xml file that contains the project grant number to charge
     *
     * @param batch The directory in which the file should be written
     */
    private static void createMetaDataPUFile(File batch, String projectgrantnumber) {
        // Define the embargo lift dates if applicable
        String embargo_fields = "";
        File xmlfile = findXMLFileFor(batch);
        String embargo_code = getEmbargoCode(xmlfile);
        if (embargo_code != null) {
            if (embargo_code.equals(PROQUEST_EMBARGO_CODE)) {
                logger.debug("This item will be placed under embargo, embargo_code=" + embargo_code);
                // lift embargo on date given in xmlfile
                Date endDate = getEmbargoEnd(xmlfile);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String todaystr = sdf.format(endDate);
                embargo_fields = "     <dcvalue element=\"embargo\" qualifier=\"terms\">" + todaystr + "</dcvalue>\n" +
                        "     <dcvalue element=\"embargo\" qualifier=\"lift\">" + todaystr + "</dcvalue>\n";
            } else if (!embargo_code.equals("0")) {
                logger.error("Unexpected embargo code found: embargo_code = " + embargo_code);
            }
        }
        String contents = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"no\"?>\n" +
                "<dublin_core schema=\"pu\">\n" +
                "     <dcvalue element=\"projectgrantnumber\" qualifier=\"none\">" + projectgrantnumber + "</dcvalue>\n" +
                embargo_fields +
                "</dublin_core>";

        String filename = batch.getPath() + File.separator + "metadata_pu.xml";

        try {
            FileWriter fstream = new FileWriter(filename);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(contents);
            out.close();
        } catch (Exception e) {
            logger.error("Problems creating file: " + filename, e);
        }
    }

    private static String[] groupBatchesByDepartment(Vector<File> batches, HashMap<String, String> collections) {
        HashSet<String> collection_ids = new HashSet<String>();
        //ArrayList<String> collection_ids = new ArrayList<String>();

        // For each batch, determine the department's ARK ID, then move the batch into a directory named after the collectionID
        int counter = 0;

        while (counter < batches.size()) {
            File batch = batches.get(counter);

            // Get the department name
            File xmlfile = findXMLFileFor(batch);
            String deptName = getDepartmentName(xmlfile);
            // Lookup the collectionID
            String collectionID = collections.get(deptName);
            // Move the batch into a directory named after the collectionID
            // First look to see if the collection directory already exists
            // If newParent does not exist, then create it
            try {
                String collectionIDDirName = collectionID.replace(File.separatorChar, '_');
                File collectionDir = new File(batch.getParent() + File.separator + "bydept" +
                        File.separator + collectionIDDirName);
                if (!collectionDir.exists()) {
                    logger.info("Making directory " + collectionDir.getPath());
                    collectionDir.mkdirs();
                }

                copyFolder(batch, collectionDir);

                // Do not delete the original batch so that the clean and archive
                //  functionality will continue to work.
                //deleteDirectory(batch);

                collection_ids.add(collectionID);
            } catch (IOException ioe) {
                logger.error("Problems copying folder: " + batch.getPath(), ioe);
            }
            counter = counter + 1;
        }

        return collection_ids.toArray(new String[0]);
    }

    private static File openFile(String metadataTransformXSL, String dropBoxPath) throws FileNotFoundException {
        File file = null;
        String path = metadataTransformXSL;
        if (!metadataTransformXSL.startsWith(File.separator) && !metadataTransformXSL.startsWith(".")) {
            path = dropBoxPath + File.separator + metadataTransformXSL;
        }
        file = new File(path);
        if (!file.canRead() || (!file.exists())) {
            logger.error("ERROR: Can't open : " + path);
            throw new FileNotFoundException("Can't open : " + path);
        }
        return file;
    }

    private static int checkStep(int step, int stop) {
        if (stop == step) {
            System.err.println("exiting at step " + step);
            System.exit(1);
        }
        System.out.println("At step " + step);
        logger.debug("At step " + step);
        return step + 1;
    }


    private static int executeCommand(String command, String[] args, Boolean dryRun) {
        String cmdLine = command;
        for (int i = 0; i < args.length; i++) {
            cmdLine += " " + args[i];
        }
        logger.info("Running " + cmdLine);

        Process p;
        try {
            p = Runtime.getRuntime().exec(cmdLine);
            p.waitFor();
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line = "";
            while ((line = reader.readLine()) != null) {
                logger.info(command + ": " + line);
            }

        } catch (Exception e) {
            logger.info(command + "failed with Exception" + e);
            e.printStackTrace();
            return 1;
        }
        return 0;
    }
}