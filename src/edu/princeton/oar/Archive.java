package edu.princeton.oar;

import edu.princeton.cli.Arguments;
import edu.princeton.dspace.content.ItemLister;
import org.apache.commons.cli.ParseException;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.DCDate;
import org.dspace.content.Item;
import org.dspace.content.MetadataField;
import org.dspace.core.Context;
import org.dspace.eperson.EPerson;
import org.dspace.workflow.WorkflowItem;
import org.dspace.workflow.WorkflowManager;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

public class Archive extends edu.princeton.cli.Main {
    private String selectFieldName;
    private MetadataField selectField;
    private String stateField[];
    private String newState;
    private String fromState;
    private String selectValue;
    private String provenanceMessage;

    Archive(ArchiveArguments args, Context ctxt) throws ParseException {
        super(args, ctxt, Archive.class);
        selectField = args.getSelectField(context);
        selectFieldName = args.getSelectFieldName();
        selectValue = args.getSelectValue();
        stateField = getFieldParts(ArchiveArguments.WORKFLOW_STATE);
        fromState = args.getStateValue();
        newState = args.getNewStateValue();
        provenanceMessage = args.getProvenanceMessage();
        info(String.format("advance %s from '%s' to '%s' for items with %s='%s'",
                ArchiveArguments.WORKFLOW_STATE, fromState, newState,
                selectFieldName, selectValue));
    }

   void doit(EPerson advancer) throws SQLException, IOException, AuthorizeException {
        ItemLister lister = new ItemLister(selectField, selectValue, ItemLister.ListerMode.INWORKFLOW);
        Iterator<Item> iter = lister.getAll(context).iterator();
        info("lister: " + lister);
        DCDate now = DCDate.getCurrent();
        int n = 0;
        while (iter.hasNext()) {
            Item item = iter.next();
            String curState = item.getMetadataFirstValue(stateField[0],stateField[1], stateField[2], Item.ANY);
            if (curState == null) {
                info(String.format("skip %s: %s.%s.%s=%s", item.toString(), stateField[0], stateField[1], stateField[2], "null"));
            } else {
                if (curState.equals(fromState)) {
                    if (!testRun) {
                        WorkflowItem wfi = null;
                        wfi = WorkflowItem.findByItem(context, item);
                        // goto to STEP3 and then advance --> archive
                        wfi.setState(WorkflowManager.WFSTATE_STEP3);
                        WorkflowManager.advance(context, wfi, advancer);
                        setProvenance(item, now, provenanceMessage);
                        item.setMetadataSingleValue(stateField[0], stateField[1], stateField[2], null, newState);
                        item.update();
                    }
                    n += 1;
                    info(String.format("update%s %s: %s.%s.%s=%s",
                            testRun ? "-dryRun" : "".toString(), item, stateField[0], stateField[1], stateField[2], newState));
                } else {
                    info(String.format("skip %s: %s.%s.%s=%s", item.toString(), stateField[0], stateField[1], stateField[2], curState));
                }
            }
        }

        if (!testRun) {
            context.commit();
            info("committing changes");
        }
        info(String.format("archived %d items", n));
    }

    public static void main(String argv[]) throws SQLException, ParseException, IOException, AuthorizeException {
        ArchiveArguments args = new ArchiveArguments("OarArchive");
        Context context = new Context();
        if (args.parseArgs(argv, context)) {
            new Archive(args, context).doit(args.getEPerson(context));
        }   else {
            args.usage();
        }
    }
}


class ArchiveArguments extends edu.princeton.cli.Arguments {

    private static String SELECT = "s";
    private static String SELECT_LONG = "select";

    private static String VALUE = "V";
    private static String VALUE_LONG = "value";

    static final String WORKFLOW_STATE = "pu.workflow.state";

    private static String FROM_STATE = "w";
    private static String FROM_STATE_LONG = "workflow_state";

    private static String NEW_STATE = "W";
    private static String NEW_STATE_LONG = "new_workflow_state";

    private static String PROVENANCE_MESSAGE = "p";
    private static String PROVENANCE_MESSAGE_LONG = "provenance_msg";
    private static String PROVENANCE_MESSAGE_DEFAULT = "archived at end of waiting period";

    ArchiveArguments(String klass) {
        super(klass);
        options.addOption(EPERSON, EPERSON_LONG, true, "email/netid of person perfoming changes");
        options.addOption(TESTRUN, TESTRUN_LONG, false, "test - do not commit database changes");

        options.addOption(SELECT, SELECT_LONG, true, "select fully qualified metadata value");
        options.addOption(VALUE, VALUE_LONG, true, "value used with select metadata value to select items");

        options.addOption(FROM_STATE, FROM_STATE_LONG, true,
                "value for '" + WORKFLOW_STATE + "' metadata field - used to further narrow item selection to work on");

        options.addOption(NEW_STATE, NEW_STATE_LONG, true,
                "new value for '" + WORKFLOW_STATE + "' metadata field - indicates that Archive processed an item");

        options.addOption(PROVENANCE_MESSAGE, PROVENANCE_MESSAGE_LONG, true,
                "text to be used in provenance metadata field - default '" + PROVENANCE_MESSAGE_DEFAULT + "'");
    }

    MetadataField getSelectField(Context context) throws ParseException {
        return getField(context, SELECT_LONG, OptionMode.REQUIRED);
    }

    String getSelectFieldName() throws ParseException {
        return getValue(SELECT_LONG, OptionMode.REQUIRED);
    }

    String getSelectValue() throws ParseException {
        return getValue(VALUE_LONG, OptionMode.REQUIRED);
    }

    String getStateValue() throws ParseException {
        return getValue(FROM_STATE_LONG, OptionMode.REQUIRED);
    }

    String getNewStateValue() throws ParseException {
        return getValue(NEW_STATE_LONG, OptionMode.REQUIRED);
    }

    String getProvenanceMessage() throws ParseException {
        String msg =  getValue(PROVENANCE_MESSAGE_LONG, OptionMode.OPTIONAL);
        if (null == msg) {
            msg = PROVENANCE_MESSAGE_DEFAULT;
        }
        return msg;
    }

    EPerson getEPerson(Context context) throws ParseException {
        return getEPerson(context, EPERSON_LONG, Arguments.OptionMode.REQUIRED);
    }

    public Boolean parseArgs(String[] argv, Context ctxt) {
        if (!super.parseArgs(argv))
            return false;
        try {
            ctxt.setCurrentUser(getEPerson(ctxt));
            getSelectValue();
            getNewStateValue();
            return true;
        } catch (ParseException e)  {
            System.err.println(e.getMessage());
            return false;
        }
    }
}
