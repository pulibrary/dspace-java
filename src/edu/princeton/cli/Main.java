package edu.princeton.cli;

import edu.princeton.oar.Mailer;
import org.apache.log4j.Logger;
import org.dspace.content.*;
import org.dspace.core.Context;

import java.sql.SQLException;

public class Main {
    protected Context context;
    public static Logger log = null;
    protected boolean testRun, verbose;
    protected static final String[] provenanceField = {"dc", "description", "provenance"};

    protected Main(Arguments args, Context ctxt, Class klass) {
        context = ctxt;
        verbose = args.getVerbose();
        testRun = args.getTestRun();
        if (null == log) log = Logger.getLogger(klass);
    }

    protected void debug(String message) {
        if (verbose)
            System.out.println("DEBUG: " + message);
        log.debug(message);
    }

    protected void info(String message) {
        if (verbose)
            System.out.println(message);
        log.info(message);
    }

    protected void warn(String message) {
        if (verbose)
            System.err.println("WARNING: " + message);
        log.warn(message);
    }

    protected void error(String message) {
        if (verbose)
            System.err.println("ERROR: " + message);
        log.error(message);
    }

    protected void setProvenance(DSpaceObject dso, DCDate date, String message) {
        String provMessage = "[" + date + "] " + context.getCurrentUser() + " " + message;
        dso.addMetadata(provenanceField[0], provenanceField[1], provenanceField[2], null, provMessage);
        info(String.format("update %s: %s.%s.%s=%s", dso.toString(), provenanceField[0], provenanceField[1], provenanceField[2], provMessage ));

    }

    protected String[] getFieldParts(String fieldName) {
        if (null == fieldName || fieldName.isEmpty()) {
            throw new RuntimeException("should have caught missing argument earlier");
        }
        MetadataField field = null;
        try {
            field = new MetadataField(context, fieldName).find(context);
        } catch (SQLException e) {
            // NOOP
        }
        if (null == field) {
            throw new RuntimeException(fieldName + " not a valid metadata field");
        }

        String[] fieldParts = new String[3];
        try {
            fieldParts[0] = MetadataSchema.find(context, field.getSchemaID()).getName();
        } catch (SQLException e) {
            throw new AssertionError("should never get here");
        }
        fieldParts[1] = field.getElement();
        fieldParts[2] = field.getQualifier();
        return fieldParts;
    }
}
