package edu.princeton.dspace;

/*
 * intended to turn this into a proper curation task
 * for now just run from main program
 */

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;

import org.dspace.content.DSpaceObject;
import org.dspace.content.MetadataField;
import org.dspace.content.MetadataSchema;
import org.dspace.content.Metadatum;
import org.dspace.core.Constants;
import org.dspace.core.Context;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

public class Utils {
    private static Logger log = Logger.getLogger(Utils.class);

    public static Metadatum toMetadatum(String fully_qualified_metadata_field) throws SQLException, AuthorizeException {
        if (fully_qualified_metadata_field == null)
            return null;

        try {
            String[] splits = fully_qualified_metadata_field.split("\\.");
            Metadatum val = new Metadatum();
            val.schema = splits[0];
            val.element = splits[1];
            if (splits.length == 3) {
                val.qualifier = splits[2];
            }
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new RuntimeException("Malformed metadata field name " + fully_qualified_metadata_field);
        }
    }

    public static MetadataField getMetadataField(Context context, Metadatum val) throws SQLException, AuthorizeException {
        if (val == null)
            return null;

        int schema_id = -1;
        MetadataSchema schema = MetadataSchema.find(context, val.schema);
        if (schema == null) {
            throw new RuntimeException("unknown metadata schema " + schema);
        }
        schema_id = schema.getSchemaID();
        MetadataField field =  MetadataField.findByElement(context, schema_id, val.element, val.qualifier);
        log.debug("getMetadataField: " +  val  + " -> " + ((field == null) ? -1 : field.getFieldID()));
        return  field;
    }


    public static String describe(DSpaceObject dSpaceObject) throws SQLException {
        String b = dSpaceObject + " name=" + dSpaceObject.getName().replaceAll("\\s+", " ");
        for  (DSpaceObject o = dSpaceObject.getParentObject(); o != null; o = o.getParentObject()) {
            b = o.getHandle() + " < " + b;
            if (o.getType() == Constants.COMMUNITY)
                return o + " "  + b;
        }
        return b;
    }

    public static String readFile( String file ) throws IOException {
        BufferedReader reader = new BufferedReader( new FileReader(file));
        String         line = null;
        StringBuilder  stringBuilder = new StringBuilder();
        String         ls = System.getProperty("line.separator");

        while( ( line = reader.readLine() ) != null ) {
            stringBuilder.append( line );
            stringBuilder.append( ls );
        }

        return stringBuilder.toString();
    }

}
