package edu.princeton.cli;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.dspace.content.DSpaceObject;
import org.dspace.content.MetadataField;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.eperson.EPerson;
import org.dspace.eperson.Group;
import org.dspace.utils.DSpace;

import java.sql.SQLException;

public class Arguments {
    protected String klass;
    protected Options options;
    private CommandLine line;
    private boolean verbose = false;

    public enum OptionMode {OPTIONAL, REQUIRED};

    protected static final Logger log = Logger.getLogger(Arguments.class);

    public static String VERBOSE = "v";
    public static String VERBOSE_LONG = "verbose";

    public static String HELP = "h";
    public static String HELP_LONG = "help";

    public static String TESTRUN = "t";
    public static String TESTRUN_LONG = "test";

    public static String EPERSON = "e";
    public static String EPERSON_LONG = "eperson";

    public static String NETID = "n";
    public static String NETID_LONG = "netid";

    public static String GROUP = "g";
    public static String GROUP_LONG = "group";

    public static String ACTION = "a";
    public static String ACTION_LONG = "action";

    public static String URL = "u";
    public static String URL_LONG = "url";

    public Arguments(String mainClass) {
        klass = mainClass;
        options = new Options();

        options.addOption(VERBOSE, VERBOSE_LONG, false, "verbose");
        options.addOption(HELP, HELP_LONG, false, "help");
    }

    public Options getOptions() {
        return options;
    }

    public Boolean parseArgs(String[] argv) {
        CommandLineParser parser = new PosixParser();
        try {
            line = parser.parse(options, argv);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            usage();
            return false;
        }

        if (line.hasOption(HELP)) {
            usage();
            return false;
        }
        getVerbose();
        return true;
    }

    public void checkReady() {
        if (null == line) {
            throw new RuntimeException("must call parseArgs first");
        }
    }

    public String getValue(String option, OptionMode mode) throws ParseException {
        String val = getOptionalValue(option);
        if (verbose) {
            System.out.println("Arguments.getValue: " + option + "='" + ((val == null) ? "null" : val + "'"));
        }
        if (val == null && mode == OptionMode.REQUIRED )
            throw new ParseException("must give a [" + klass + "] " + option + " argument");
        return val;
    }

    public String getOptionalValue( String option) {
        checkReady();
        String val = null;
        if (line.hasOption(option)) {
            val =  line.getOptionValue(option);
        } else {
            val= ConfigurationManager.getProperty(klass + "." + option);
        }
        if (null != val) {
            val = val.trim();
            if (val.isEmpty())
                val = null;
        }
        return val;
    }

    public boolean getBoolValue(String option, boolean def) {
        if (line.hasOption(option)) {
            return !def;
        }
        return def;
    }

    public boolean getTestRun() {
        return line.hasOption(TESTRUN);
    }
    public boolean getVerbose() {
        verbose = line.hasOption(VERBOSE);
        return verbose;
    }

    public MetadataField getField(Context context, String option, OptionMode mode) throws ParseException {
        checkReady();
        String fieldName = getValue(option, mode);
        MetadataField mdfield = null;
        if (null != fieldName) {
            try {
                mdfield = new MetadataField(context, fieldName).find(context);
            } catch (SQLException e) {
                throw new ParseException(fieldName + " not valid metadata field\n");
            }
        }
        if (null == mdfield && mode == OptionMode.REQUIRED) {
            throw new ParseException("must provide valid metadata field in " + option + " parameter" );
        }
        return mdfield;
    }

    public EPerson getEPerson(Context context, String option, OptionMode mode) throws ParseException {
        String emailOrNetid = getValue(option, mode);
        EPerson p = null;
        if (null != emailOrNetid) {
            try {
                p = EPerson.findByEmail(context, emailOrNetid);
                if (null == p)
                    p = EPerson.findByNetid(context, emailOrNetid);
            } catch (SQLException e) {
                /* pass */ ;
            }
            if (null == p) {
                throw new ParseException("can't find EPerson with netid/email " + emailOrNetid);
            }
        }
        if (null == p && mode == OptionMode.REQUIRED) {
            throw new ParseException("must give a [" + klass + "] " + EPERSON_LONG + " argument");
        }
        return p;
    }

    public Group getGroup(Context context, String option, OptionMode mode) throws ParseException, SQLException {
        Group group = null;
        String group_name = getValue(option, mode);
        if (group_name != null) {
            group = Group.findByName(context, group_name);
        }
        if ((group == null) && (mode == OptionMode.REQUIRED)) {
            throw new ParseException("must give a valid group name ");
        }
        return group;
    }

    public DSpaceObject getFromUrl(Context context, String option, OptionMode mode) throws ParseException, SQLException {
        String full_url = getValue(option, mode);
        String[] parts = full_url.split("/");
        try {
            String handle = parts[parts.length - 2] + "/" + parts[parts.length - 1];
            DSpaceObject obj = DSpaceObject.fromString(context, handle);
            if (null == obj || (obj.getType() != Constants.ITEM && obj.getType() != Constants.COLLECTION && obj.getType() != Constants.COMMUNITY)) {
                throw new ParseException(full_url + " not a valid item/collection/community url ");
            }
            return obj;
        } catch (IndexOutOfBoundsException i) {
            throw new ParseException(full_url + " is not a valued url");
        }
    }

    public void usage() {
      HelpFormatter myhelp = new HelpFormatter();
      myhelp.printHelp(klass + "\n", options);
      System.out.println("");
    }

    public boolean isVerbose() {
      checkReady();
      return line.hasOption(VERBOSE);
    }
}
