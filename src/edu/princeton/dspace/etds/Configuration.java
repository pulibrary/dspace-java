package edu.princeton.dspace.etds;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.dspace.core.ConfigurationManager;
import org.dspace.storage.rdbms.DatabaseManager;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.apache.log4j.Logger;

/**
 * Class used to read configuration information for the ETD ingest processor and MARC record processor.
 *
 * @author Mark Ratliff, Princeton University
 */

public class Configuration {

    static Logger logger = Logger.getLogger(Configuration.class);

    //config.xml values
    private String parentCommunity = "";
    private String metadataTransformXSL = "";
    private String schemaLocation = "";
    private String filelistTransformXSL = "";
    private String bitstreamTransformXSL = "";
    private String bitstreamExtension = "";
    private String authorlistTransformXSL = "";
    private String eperson = "";
    private String handle = "";
    private String projectgrantnumber = "";
    private Vector<String> adminEmails = new Vector<String>();
    private String dspaceLoc = "";
    private String smtpHost = "";

    private static final String DSPACE_CONFIG_FILE = "dspace.cfg";

    // Hashtable for translating between department name sent by ProQuest and the
    //  collection ID in DSpace
    HashMap<String, String> collectionIDLookup = new HashMap<String, String>();

    // Hashtable for translating between department name sent by ProQuest and the
    //  department name used by the Library OPAC
    HashMap<String, String> collectionOPACNameLookup = new HashMap<String, String>();

    public Configuration(String filename, String dspaceHome) {
        logger.info("Initializing from " + dspaceHome + "/config/" + DSPACE_CONFIG_FILE);
        ConfigurationManager.loadConfig(dspaceHome + "/config/" + DSPACE_CONFIG_FILE);
        loadConfigFile(filename);
    }

    /**
     * Load data from the configuration file
     *
     * @param filename
     */
    private void loadConfigFile(String filename) {
        String CWD = System.getProperty("user.dir");

        logger.info("Reading the config file " + CWD + "/" + filename + " cwd=" + CWD);

        File collectionXML = new File(filename);
        if ((!collectionXML.exists()) || (!collectionXML.isFile()) || (!collectionXML.canRead())) {
            logger.fatal("Trouble reading collection.xml file");
            System.exit(1);
        }
        //read in the xml and populate the appropriate variables
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(collectionXML);
            Element rootElement = doc.getDocumentElement();
            if (!rootElement.getNodeName().equals("configuration")) {
                logger.fatal("Incorrect root node in config.xml file");
                System.exit(1);
            }

            //parentCommunity
            NodeList parentCommunityList = doc.getElementsByTagName("parentCommunity");
            if (parentCommunityList.getLength() > 0) {
                parentCommunity = parentCommunityList.item(0).getTextContent();
            } else {
                parentCommunity = "";
            }
            logger.info("parentCommunity: " + parentCommunity);

            //metadata XSL
            NodeList metadataXSLList = doc.getElementsByTagName("metadataTransformer");
            if (metadataXSLList.getLength() > 0) {
                metadataTransformXSL = metadataXSLList.item(0).getTextContent();
            } else {
                //if XML is already in DC
                metadataTransformXSL = "";
            }
            logger.info("metadataTransformXSL: " + metadataTransformXSL);


            //schema location (optional)
            NodeList schemaLocationList = doc.getElementsByTagName("schemaLocation");
            if (schemaLocationList.getLength() > 0) {
                schemaLocation = schemaLocationList.item(0).getTextContent();
            }

            logger.info("schemaLocation: " + schemaLocation);

            //filelist XSL
            NodeList filelistXSLList = doc.getElementsByTagName("filelistTransformer");
            if (filelistXSLList.getLength() > 0) {
                filelistTransformXSL = filelistXSLList.item(0).getTextContent();
            } else {
                //if 'contents' file should just list all files in dir except the XML one
                filelistTransformXSL = "";
            }

            logger.info("filelistTransformXSL: " + filelistTransformXSL);

            //bitstream XSL
            NodeList bitstreamXSLList = doc.getElementsByTagName("bitstreamTransformer");
            if (bitstreamXSLList.getLength() > 0) {
                bitstreamTransformXSL = bitstreamXSLList.item(0).getTextContent();
            } else {
                //not generating bitstream; it's already there
                bitstreamTransformXSL = "";
            }

            logger.info("bitstreamTransformXSL: " + bitstreamTransformXSL);

            //bitstream extension
            NodeList bitstreamExtensionList = doc.getElementsByTagName("bitstreamExtension");
            if (bitstreamExtensionList.getLength() > 0) {
                bitstreamExtension = bitstreamExtensionList.item(0).getTextContent();
            }

            logger.info("bitstreamExtension: " + bitstreamExtension);

            if ((!bitstreamTransformXSL.equals("")) && (bitstreamExtension.equals(""))) {
                //error
                logger.fatal("ERROR in collection.xml file. Bitstream transformer is specified but no extension is specified.");
                System.exit(1);
            }
            //authorlist XSL
            NodeList authorlistXSLList = doc.getElementsByTagName("authorlistTransformer");
            if (authorlistXSLList.getLength() > 0) {
                authorlistTransformXSL = authorlistXSLList.item(0).getTextContent();
            } else {
                authorlistTransformXSL = "";
            }

            logger.info("authorlistTransformXSL: " + authorlistTransformXSL);

            //eperson
            NodeList epersonList = doc.getElementsByTagName("eperson");
            if (epersonList.getLength() > 0) {
                eperson = epersonList.item(0).getTextContent();
            }

            logger.info("eperson: " + eperson);

            if (eperson.equals("")) {
                //error
                logger.fatal("ERROR no eperson specified in collection.xml.");
                System.exit(1);
            }
            //handle
            /*
	        NodeList handleList = doc.getElementsByTagName("handle");
	        if (handleList.getLength() > 0) {
	        	handle = handleList.item(0).getTextContent();
	        }
	        if (verbose) {
	        	System.out.println("handle: " + handle);
	        }
	        if ( handle.equals("") ) {
	        	//error
	        	System.out.println("ERROR no handle specified in collection.xml.");
	        	System.exit(1);
	        }
	        */
            // Project grant number to charge for storage
            NodeList projectgrantnumberList = doc.getElementsByTagName("projectgrantnumber");
            if (projectgrantnumberList.getLength() > 0) {
                projectgrantnumber = projectgrantnumberList.item(0).getTextContent();
            }

            logger.info("projectgrantnumber: " + projectgrantnumber);

            if (projectgrantnumber.equals("")) {
                //error
                logger.fatal("ERROR no projectgrantnumber specified in collection.xml.");
                System.exit(1);
            }
            //admin emails
            NodeList adminEmailList = doc.getElementsByTagName("adminEmail");
            if (adminEmailList.getLength() == 0) {
                logger.fatal("ERROR no admin email specified");
                System.exit(1);
            }
            int counter = 0;
            while (counter < adminEmailList.getLength()) {
                String email = adminEmailList.item(counter).getTextContent();

                logger.info("adminEmail: " + email);

                adminEmails.add(email);
                counter = counter + 1;
            }

            //dspaceLoc
            dspaceLoc = ConfigurationManager.getProperty("dspace.url");
            logger.info("dspaceLoc: " + dspaceLoc);
            if (dspaceLoc.equals("")) {
                logger.fatal("dspace.url undefined");
                System.exit(1);
            }

            //smtpHost
            NodeList smtpList = doc.getElementsByTagName("smtpHost");
            if (smtpList.getLength() > 0) {
                smtpHost = smtpList.item(0).getTextContent();
            }
            logger.info("smtpHost: " + smtpHost);
            if (smtpHost.equals("")) {
                //error
                logger.warn("no smtpHost specified");
            }

            //load collection names, department names, and identifiers defined in config.xml

            // Get the list of <collection> elements
            NodeList collList = doc.getElementsByTagName("collection");
            if (collList.getLength() == 0) {
                logger.fatal("there are no collections in config.xml file");
                System.exit(1);
            }

            // For each <collection>
            for (int i = 0; i < collList.getLength(); i++) {
                NodeList children = collList.item(i).getChildNodes();
                String collName = null;
                String collId = null;
                String opacDeptName = null;
                String nodename = null;

                // Read the data in the child <name>, <identifier>, and <opacname> elements
                for (int j = 0; j < children.getLength(); j++) {
                    nodename = children.item(j).getNodeName();

                    if (nodename.equals("name")) {
                        collName = children.item(j).getTextContent();
                    } else if (nodename.equals("identifier")) {
                        collId = children.item(j).getTextContent();
                    } else if (nodename.equals("opacname")) {
                        opacDeptName = children.item(j).getTextContent();
                    }

                }

                if (collName != null && collId != null) {
                    collectionIDLookup.put(new String(collName), new String(collId));
                }

                if (collName != null && opacDeptName != null) {
                    collectionOPACNameLookup.put(new String(collName), new String(opacDeptName));
                }

                collName = null;
                collId = null;
                opacDeptName = null;
            }

            //System.out.println("Collections in config.xml: "+collections);

        } catch (Exception err) {
            err.printStackTrace();
            System.out.println("Trouble reading collection.xml file");
            System.exit(1);
        }

    }

    // Accessor methods ...

    /**
     * @return parentCommunity  ID
     */
    public String getParentCommunity() {
        return parentCommunity;
    }

    /**
     * @return the metadataTransformXSL
     */
    public String getMetadataTransformXSL() {
        return metadataTransformXSL;
    }

    /**
     * @return the schemaLocation
     */
    public String getSchemaLocation() {
        return schemaLocation;
    }

    /**
     * @return the filelistTransformXSL
     */
    public String getFilelistTransformXSL() {
        return filelistTransformXSL;
    }

    /**
     * @return the bitstreamTransformXSL
     */
    public String getBitstreamTransformXSL() {
        return bitstreamTransformXSL;
    }

    /**
     * @return the bitstreamExtension
     */
    public String getBitstreamExtension() {
        return bitstreamExtension;
    }

    /**
     * @return the authorlistTransformXSL
     */
    public String getAuthorlistTransformXSL() {
        return authorlistTransformXSL;
    }

    /**
     * @return the eperson
     */
    public String getEperson() {
        return eperson;
    }

    /**
     * @return the handle
     */
    public String getHandle() {
        return handle;
    }

    /**
     * @return the projectgrantnumber
     */
    public String getProjectgrantnumber() {
        return projectgrantnumber;
    }

    /**
     * @return the adminEmails
     */
    public Vector<String> getAdminEmails() {
        return adminEmails;
    }


    /**
     * @return the dspaceLoc
     */
    public String getDspaceLoc() {
        return dspaceLoc;
    }

    /**
     * @return the smtpHost
     */
    public String getSmtpHost() {
        return smtpHost;
    }

    public Connection getDBConnection() throws SQLException {
        Connection conn = DatabaseManager.getConnection();

        logger.info("Working with DB=" + ConfigurationManager.getProperty("db.name") +
                "and user=" + ConfigurationManager.getProperty("db.username"));

        return conn;
    }

    /**
     * @return the collectionIDLookup
     */
    public HashMap<String, String> getCollectionIDLookup() {
        return collectionIDLookup;
    }

    /**
     * @return the collectionOPACNameLookup
     */
    public HashMap<String, String> getCollectionOPACNameLookup() {
        return collectionOPACNameLookup;
    }


}