package edu.princeton.dspace.etds;

import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.MarcWriter;
import org.marc4j.MarcStreamWriter;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.DataField;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.Record;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.sql.*;
import java.util.*;

import org.apache.commons.cli.*;

import org.apache.log4j.Logger;

/**
 * This application takes MARC data supplied by ProQuest and for each record:  1)  Looks up the corresponding ARK and adds
 * that information to the MARC data and 2) Cleans the MARC data in preparation for uploading into the University Library's
 * OPAC.
 * 
 * @author Mark Ratliff, Princeton University
 *
 */

public class ETDMARCProcessor {

	static Logger logger = Logger.getLogger(ETDMARCProcessor.class);

	static boolean verbose = false;

	// Because instantiation of MarcFactory is relatively expensive, we create a single static instance that
	//  can be reused throughout this class
	static MarcFactory factory = MarcFactory.newInstance();

	/* only looking at titles and alternate titles of items */
	static final String titles_in_items = "(metadata_field_id=64 OR metadata_field_id=65) AND resource_type_id = 2";
	static final String textval = "TEXT_VALUE LIKE ?";
	static final String charReplacePattern = "[,:.;\" ]+";
	static final String textval_upper = "UPPER(REGEXP_REPLACE(text_value, '[,:.;\" ]+', '')) LIKE  ?";
	static final String in_community = " RESOURCE_ID in (SELECT ITEM_ID FROM COMMUNITY2ITEM WHERE COMMUNITY_ID = ?)";

	static final String select = "SELECT resource_id,metadata_value_id,text_value FROM METADATAVALUE " +
			"WHERE " + titles_in_items + " " +
			"AND "  + in_community + " " +
			"AND "  + textval + " ";

	/* all table values are upper-regexp-ed if we do this
		 static final String select_upper = "SELECT resource_id,metadata_value_id,text_value,resource_id FROM METADATAVALUE " +
			"WHERE " + titles_in_items + " " +
			"AND "  + in_community + " " +
			"AND "  + textval_upper + " ";
	*/

	/*  just upper-regexp titles in given community
		WITH M AS
			( SELECT resource_id,metadata_value_id,text_value
				FROM METADATAVALUE
				WHERE (metadata_field_id=64 OR metadata_field_id=65) AND resource_type_id = 2
				AND  RESOURCE_ID in (SELECT ITEM_ID FROM COMMUNITY2ITEM WHERE COMMUNITY_ID = 67)
			) SELECT M.resource_id, M.metadata_value_id, M.text_value FROM M
				WHERE UPPER(REGEXP_REPLACE(M.text_value, '[,:.;" ]+', '')) LIKE  'Characterization of polyester-rope suspended Footbridges';
	 */
	static final String select_upper = "WITH M AS ( " +
				"SELECT resource_id,metadata_value_id,text_value FROM METADATAVALUE " +
				"WHERE " + titles_in_items + " " +
				"AND "  + in_community + " " +
			") SELECT M.resource_id, M.metadata_value_id, M.text_value FROM M " +
			" WHERE UPPER(REGEXP_REPLACE(M.text_value, '[,:.;\" ]+', '')) LIKE  ?" ;

	static final String select_ark = "SELECT TEXT_VALUE FROM METADATAVALUE " +
			"WHERE RESOURCE_TYPE_ID = 2 AND metadata_field_id  = 25 " +
			"AND RESOURCE_ID = ? ";


	private static String getArk(Connection db, String title, int  parentCommId) throws SQLException
	{
		String ark = null;

		// try title
	    int id = getIdForTitle(db, select, title, parentCommId);
		// try upper case simplified title
		if (id == -1) {
			String upper = title.replaceAll(charReplacePattern, "").toUpperCase();
			id = getIdForTitle(db, select_upper, upper, parentCommId);
		}
		// get ark if we found something
		if (id != -1) {
			PreparedStatement stmt = db.prepareStatement(select_ark);
			stmt.setInt(1, id);
			ResultSet matchIter = stmt.executeQuery();
			if (matchIter.next()) {
				ark = matchIter.getString("text_value");
			}
		}
		return ark;
	}

	private static int getIdForTitle(Connection db,  String select, String title, int parentCommId) throws SQLException {
		PreparedStatement stmt = db.prepareStatement(select);
		stmt.setInt(1, parentCommId);
		stmt.setString(2, title);

		int id = -1;
		ResultSet matchIter = stmt.executeQuery();
		int n = 0;
		while (matchIter.next()) {
			n = n + 1;
			id = matchIter.getInt("resource_id");

		}
		switch (n) {
			case 0: log("no match for '" + title + "'", verbose);
				break;
			case 1: log("matched '" + title + "' to id " + id, verbose);
				break;
			default:
				log("multiple matches for '" + title + "'", verbose);
		}
		return id;
	}

	public static void main(String args[]) throws Exception 
	{

		/*
		 * Before reading the MARC file, be sure to remove Line Feeds from between records with this command:
		 *     cat original_file | tr -d '\012' > new_file
		 *     
		 */
		String dspaceHome = "/dspace";
		String configFile = "./config/etds/config.xml";

		// The incoming MARC file
		String inputfile=null;
		
		// The resulting MARC file after processing
		String outputfile=null;		

		// Define the options which can be passed via the commandline
		
		CommandLineParser cliParser = new PosixParser();

		Options options = new Options();

        options.addOption("c", "config", true, "config file - default " + configFile);
		options.addOption("d", "dspace_home", true, "full path of dspace_home directory, default " + dspaceHome);
		options.addOption("h", "help", false, "help");
		options.addOption("v", "verbose", false, "verbose");

		options.addOption(OptionBuilder.isRequired(true).hasArg(true)
				.withDescription("MARC file to be processed")
				.create("i"));

		options.addOption(OptionBuilder.isRequired(true).hasArg(true)
				.withDescription("Processed MARC file")
				.create("o"));
		
		HelpFormatter f = new HelpFormatter();
		String usagestr = "java ETDMARCProcessor [-c config_file] [-d dspace_home] -i inputfile -o outputfile -h";

		// Extract the values of the options passed from the commandline
		try 
		{	
			CommandLine line = cliParser.parse(options, args);
			if (line.hasOption('h')) {
				f.printHelp(usagestr, options);
				System.exit(0);
			}
			verbose = line.hasOption('v');
			if (line.hasOption('d')) {
				dspaceHome = line.getOptionValue('d');
			}
			if (line.hasOption('c')) {
                configFile = line.getOptionValue('c');
            }
			inputfile = line.getOptionValue("i");
			outputfile = line.getOptionValue("o");
		}
		catch (MissingOptionException moe) 
		{
			System.err.println("Missing options: " + moe.getMessage());

			f.printHelp(usagestr, options);
			System.exit(1);
		}
		catch (MissingArgumentException mae) 
		{
			System.err.println("Missing arguments for: " + mae.getMessage());

			f.printHelp(usagestr, options);
			System.exit(1);
		}

		InputStream in = null;
		MarcReader reader = null;

		OutputStream out = null;
		MarcWriter writer = null;

		Connection dspace_conn = null;

		Configuration config = new Configuration(configFile, dspaceHome);

        int parentCommunityID =  -1;

		String commId = config.getParentCommunity();
		try {
			parentCommunityID = Integer.parseInt(config.getParentCommunity());
		} catch (Exception e) {
			throw new RuntimeException("must define a community id");
		}

/*
SELECT resource_id,metadata_value_id,text_value,resource_id FROM METADATAVALUE
WHERE (metadata_field_id=64 OR metadata_field_id=65) AND resource_type_id = 2
AND RESOURCE_ID in (SELECT ITEM_ID FROM COMMUNITY2ITEM WHERE COMMUNITY_ID = 67);
 */

		try {
			dspace_conn = config.getDBConnection();

			if (verbose) {
				System.out.println("select " + select);
				System.out.println("select_upper " + select_upper);
				System.out.println("select_ark " + select_ark);
				System.out.println("");

			}

			//stmt.setEscapeProcessing(true);

			log("Reading MARC records from file: "+inputfile, verbose);
			in = new FileInputStream(inputfile);
			reader = new MarcStreamReader(in);

			log("Writing updated MARC records to file: "+outputfile, verbose);
			out = new FileOutputStream(outputfile);
			writer = new MarcStreamWriter(out);

			int recordnum = 0;
			int numarksfound = 0;

			log("=== Processing started ===", verbose);

			while (reader.hasNext()) 
			{
				String ark = null;

				logger.info("Processing record: " + ++recordnum);

				Record record = reader.next();
				//             logger.debug(record.toString());

				// Get the title
				String title = ETDMARCProcessor.getTitle(record);

				// Replace double dash with single dash
				title = title.replace("--", "-");
				// Remove the period from the end of the title
				title = title.substring(0, title.length()-1);

				log("" + recordnum + ": Looking up ARK for title: " + title, verbose);

				ark = getArk(dspace_conn, title, parentCommunityID);
				if (ark  != null) {
					log("ARK: " + ark + " " + title, verbose);
					++numarksfound;
				}
				else
				{
					ark = "NO_ARK_FOUND";
					logerror("ARK: "+  "NO ARK FOR: " + title, verbose);
				}

				// Fix the 008 record
				ETDMARCProcessor.fix008(record);

				// Record the fact that we are modifying this record
				ETDMARCProcessor.addProvenance(record);

				// Remove undwanted 500 Adviser field
				removeUnwanted500AdviserField(record);

				// Remove unwanted fields
				ETDMARCProcessor.removeUnwantedFields(record);

				// Fix 790 field
				ETDMARCProcessor.fix790s(record);

				// Fix existing 856 field
				ETDMARCProcessor.fix856(record);

				// Fix department name
				ETDMARCProcessor.fixDeptName(record, config.getCollectionOPACNameLookup());

				// Add another 856 tag with the ARK URL
				if (ark != null) ETDMARCProcessor.addElectronicLocation(record, ark);

				writer.write(record);

				// Process only the first record while in development
				//				break;
			}    

			logger.info("=== Processing finished ===");
			logger.info("   Records Processed: "+recordnum);
			logger.info("   ARKs Found: "+numarksfound);

		}
		catch (SQLException sqle)
		{
			sqle.printStackTrace();
		}
		catch (IOException ie)
		{
			ie.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			dspace_conn.close();


			//reader.close();
			in.close();

			writer.close();
			out.flush();
			out.close();
		}

	}

	/**
	 * Get the title from the MARC record
	 * 
	 * @return  The title
	 */
	public static String getTitle(Record record)
	{
		// get data field 245
		DataField field = (DataField) record.getVariableField("245");

		// get indicator as int value
		//    char c = field.getIndicator2();

		// get the title proper
		Subfield subfield = field.getSubfield('a');
		String title = subfield.getData();

		// remove the non sorting characters
		//    int nonSort = Character.digit(c, 10);
		//    title = title.substring(nonSort);

		logger.debug("Title: "+title);

		return title;
	}

	/**
	 * Add an Electronic Location tag "856" indicating the ARK URL 
	 */
	public static void addElectronicLocation(Record record, String ark)
	{
		//MarcFactory factory = MarcFactory.newInstance();

		DataField df = factory.newDataField("856", '4', '0');
		df.addSubfield(factory.newSubfield('u', ark));

		record.addVariableField(df);
	}

	/**
	 * Adjust provenance to show that Princeton has modified the record
	 */
	public static void addProvenance(Record record)
	{
		//MarcFactory factory = MarcFactory.newInstance();

		DataField df = (DataField) record.getVariableField("040");
		df.addSubfield(factory.newSubfield('d', "NjP"));
	}

	/**
	 * Remove unwanted 500 field where a subfield is "Adviser"
	 */
	public static void removeUnwanted500AdviserField(Record record)
	{
		DataField field;
		Subfield subfield;
		String subfielddata;

		List<?> fields = (List<?>) record.getVariableFields("500");

		Iterator<?> iter = (Iterator<?>) fields.iterator();

		while (iter.hasNext()) 
		{
			field = (DataField) iter.next();

			subfield = field.getSubfield('a');

			subfielddata = subfield.getData();

			if (subfielddata.startsWith("Adviser")) record.removeVariableField(field);
		}
	}

	/**
	 * Remove unwanted fields "590", "650", "690", "791", "792"
	 */
	public static void removeUnwantedFields(Record record)
	{
		String[] field_nums = {"590", "650", "690", "791", "792"};

		DataField field;

		for (int i=0; i<field_nums.length; i++)
		{
			List<?> fields = (List<?>) record.getVariableFields(field_nums[i]);

			Iterator<?> iter = (Iterator<?>) fields.iterator();

			while (iter.hasNext()) 
			{
				field = (DataField) iter.next();

				if (field != null) record.removeVariableField(field);
			}
		}
	}

	/**
	 * Fix 008 tag by setting positions 15=x, 16=x, and 17=[SPACE]
	 * @param record
	 */
	public static void fix008(Record record)
	{
		boolean found008 = false;

		List<?> fields = (List<?>) record.getControlFields();

		Iterator<?> i = (Iterator<?>) fields.iterator();

		while (i.hasNext()) 
		{
			ControlField f = (ControlField) i.next();

			if (f.getTag().equals("008"))
			{
				logger.debug("Found 008 field!");
				found008 = true;

				String value = f.getData();
				StringBuffer sb = new StringBuffer(value);
				sb.setCharAt(15, 'x');
				sb.setCharAt(16, 'x');
				sb.setCharAt(17, ' ');

				f.setData(sb.toString());

				break;
			}
		}

		if (!found008) logger.error("Did not fine 008 field!");
	}

	/**
	 * Move 790 tags to 500
	 */

	public static void fix790s(Record record)
	{
		// The blank character that the second indicator will be set to
		char blank = ' ';
		DataField field;
		Subfield subfielda, subfielde;
		String adata, edata;

		List<?> fields = (List<?>) record.getVariableFields("790");

		Iterator<?> i = (Iterator<?>) fields.iterator();

		while (i.hasNext()) 
		{
			field = (DataField) i.next();

			subfielda = field.getSubfield('a');

			if (subfielda != null)
			{
				adata = subfielda.getData();

				// If subfield 'a' = 0181, then delete this field altogether
				if (adata.equals("0181"))
				{
					record.removeVariableField(field);
					continue;
				}

				// Otherwise, append subfield e to a and then delete e
				else
				{
					subfielde = field.getSubfield('e');

					if (subfielde != null)
					{
						edata = subfielde.getData();

						// Combine e data with a data
						if (!adata.endsWith(","))
						{
							subfielda.setData(adata + ", " + edata);
						}
						else
						{
							subfielda.setData(adata + " " + edata);
						}

						field.removeSubfield(subfielde);
					}
				}
			}

			// Change the field to a 500 field and set the second indicator to blank
			field.setTag("500");
			field.setIndicator2(blank);

			/* DELETE this old code after testing

			// If there is only 1 subfield and it's value is "0181" then delete this field
			List<?> subfields = (List<?>) field.getSubfields();

			if (subfields.size() == 1)
			{
				Iterator<?> j = (Iterator<?>) subfields.iterator();
				Subfield sf = (Subfield) j.next();

				String value = sf.getData();

				// If the field contains only the school code = 0181 then delete it
				if (value.equals("0181")) 
				{
					record.removeVariableField(field);
				}
				// Otherwise, change the field to a "500" field and set the second indicator to blank
				else
				{
					field.setTag("500");
					field.setIndicator2(blank);
				}
			}
			// Otherwise, change the field to a "500" field and set the second indicator to blank
			else
			{
				field.setTag("500");
				field.setIndicator2(blank);
			}
		}
			 */
		}
	}

	/**
	 * Set first and second indicators in the 856 field to "4" and "0" respectively
	 */

	public static void fix856(Record record)
	{
		DataField df = (DataField) record.getVariableField("856");
        if (df != null) {
			df.setIndicator1('4');
			df.setIndicator2('0');
		}
	}

	/**
	 * Translate Department name from ProQuest form to form appropriate for OPAC
	 */

	public static void fixDeptName(Record record, HashMap<String,String> proquest2opac_detpnames)
	{
		// The blank character that the second indicator will be set to
		char blank = ' ';

		DataField df = (DataField) record.getVariableField("710");
		df.setIndicator2(blank);

		Subfield b_subfield = df.getSubfield('b');

		String proquestname = b_subfield.getData();

		// Strip '.' off end
		if (proquestname.endsWith(".")) proquestname = proquestname.substring(0, proquestname.length()-1);

		String opacname = proquest2opac_detpnames.get(proquestname);

		if (opacname != null)
		{
			logger.debug("Proquest name = "+proquestname+", OPAC name = "+opacname);

			if (opacname.equals("Woodrow Wilson School of Public and International Affairs"))
			{
				Subfield a_subfield = df.getSubfield('a');
				a_subfield.setData(opacname);
				df.removeSubfield(b_subfield);
			}
			else
			{
				b_subfield.setData(opacname);
			}

		}
		else
		{
			String department = String.format("UNKNOWN_DEPARTMENT(%s)", proquestname);
			String error = String.format("Unable to resolve OPAC department name '%s' - setting department to '%s'",
					proquestname, department);
			logerror(error, true);
			b_subfield.setData(department);
		}
	}


	static void log(String s, Boolean verbose) {
		logger.info(s);
		if (verbose)
			System.out.println(s);
	}

	static void logerror(String s, Boolean verbose) {
		logger.error(s);
		if (verbose)
			System.err.println(s);
	}
}