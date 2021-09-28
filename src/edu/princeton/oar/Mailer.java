package edu.princeton.oar;

import edu.princeton.cli.Arguments.OptionMode;
import edu.princeton.dspace.content.ItemLister;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.tools.ant.filters.StringInputStream;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.*;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Context;
import org.dspace.services.EmailService;
import org.dspace.utils.DSpace;
import org.xml.sax.SAXException;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;

public class Mailer extends edu.princeton.cli.Main {
    /* used to select items which are ready for author notification emails */
    private MetadataField workflowStateField;
    private String workflowStateFieldName;
    private String[] workflowState;
    private String readyForEmail;    /* ready for notification value */
    private String emailedValue;     /* value to inidicate that email was sent */

    /* list of items with   workflowStateField equal to  readyForEmail */
    private ItemLister list;

    /* metadata field that contains the unique employee ids */
    private String uidFieldName;
    private MetadataField uidField;

    /* the date by which we plan to make archive items, which we included in emails  */
    private String byDate;

    /* when testing al emails are sent to this address */
    private String redirectEnail;

    /* where we can retrieve info about faculty as identified by their unqiue employee id */
    private FacultyDataSource facDataSource;

    private boolean doMail;

    private String htmlTag;
    private String cssStyle;

    // constants used in author list generation
    public static final int MAX_AUTHORS = 5;
    private static final String etAl = "et al";

    public Mailer(MailerArguments args, Context ctxt) throws ParseException {
        super(args, ctxt, Mailer.class);
        setSelectorFields(
                args.getValue(MailerArguments.SELECT_LONG, OptionMode.REQUIRED),
                args.getValue(MailerArguments.SELECTVALUE_LONG, OptionMode.REQUIRED),
                args.getValue(MailerArguments.UNIQUEID_LONG, OptionMode.REQUIRED));
        workflowState = getFieldParts(args.getValue(MailerArguments.SELECT_LONG, OptionMode.REQUIRED));
        emailedValue = args.getValue(MailerArguments.EMAILEDVALUE_LONG, OptionMode.REQUIRED);
        byDate = args.getValue(MailerArguments.BYDATE_LONG, OptionMode.REQUIRED);
        redirectEnail = args.getValue(MailerArguments.REDIRECT_EMAIL_LONG, OptionMode.OPTIONAL);
        htmlTag = args.getHtmlTag();
        cssStyle = args.getCssStyle();
        doMail = args.getDoMail();
        facDataSource = args.getDataSource();
    }

    private void setSelectorFields(String selectFieldName, String selectValue, String uiField) {
        workflowStateFieldName = selectFieldName;
        workflowStateField = null;
        readyForEmail = selectValue;
        uidFieldName = uiField;

        if (null == selectFieldName || selectFieldName.isEmpty()) {
            throw new RuntimeException("selectFieldName must not be empty");
        }
        if (null == selectValue || selectValue.isEmpty()) {
            throw new RuntimeException("selectValue must not be empty");
        }
        if (null == uidFieldName || uidFieldName.isEmpty()) {
            throw new RuntimeException("uiField must not be empty");
        }

        try {
            workflowStateField = new MetadataField(context, workflowStateFieldName).find(context);
            uidField = new MetadataField(context, uidFieldName).find(context);
        } catch (SQLException e) {
            // NOOP
        }
        if (null == workflowStateField) {
            throw new RuntimeException(workflowStateFieldName + " not a valid metadata field");
        }
        if (null == uidField) {
            throw new RuntimeException(uidFieldName + " not a valid metadata field");
        }
    }

    /*
    * send emails to employees with  items that are marked ready-for-email (according to workflowStateField, readyForEmail properties)
    *
    * if oneEmployee == null - do this for all relevant employees
    * otherwise send mail only for this one employee
    *
    */
    public void doit(String templateFilename, String oneEmployee) throws IOException, SQLException, AuthorizeException {
        String templateEmail = getEmailTemplate(templateFilename);

        if (null == oneEmployee) {
            ItemLister list = new ItemLister(workflowStateField, readyForEmail, ItemLister.ListerMode.INWORKFLOW);
            Set<String> uids = new HashSet<>();
            List<Item> items = list.getAll(context);
            for (Item i : items) {
                String uid = i.getMetadata(uidFieldName);
                if (uids.contains(uid))
                    continue;
                uids.add(uid);
                do_employee(templateEmail, uid);
            }
            info("emailed to #: " + uids.size() + " faculty");
        } else {
            do_employee(templateEmail, oneEmployee);
        }
    }

    private void do_employee(String templateEmail, String uid) throws IOException, SQLException, AuthorizeException {
        DCDate now = DCDate.getCurrent();
        FacultyUserInfo fac = null;
        try {
            fac = new FacultyUserInfo(facDataSource, uid);
            ItemLister facMatches = new ItemLister(uidField, uid, ItemLister.ListerMode.INWORKFLOW);
            List<Item> facItems = facMatches.getAll(context);
            if (facItems.isEmpty()) {
                warn("# " + fac + " has no items");
            } else {
                ArrayList<Item>  included = new ArrayList<Item>();
                for (Item i : facItems) {
                    if (includeInMail(i)) {
                        included.add(i);
                    }
                }
                if (included.isEmpty()) {
                    info("# " + fac + " has no items ready for email");
                } else {
                    if  (compileAndSendMail(templateEmail, fac, included)) {
                        markEmailSent(fac, included, now);
                    }
                }
            }
        } catch (SAXException e) {
            error("could not retrieve info about " + uid);
            log.error(e.getStackTrace());
        } catch (ParserConfigurationException e) {
            error("could not retrieve info about " + uid);
            log.error(e.getStackTrace());
        }
    }

    private boolean compileAndSendMail(String template, FacultyUserInfo fac, List<Item> facItems) throws IOException, SQLException {
        try {
            sendMimeMessage(template, fac, citationList(facItems));
        } catch (MessagingException e) {
            error("could not send email to " + fac);
            error(e.toString());
            return false;
        }
        return true;
    }

    private boolean includeInMail(Item item) throws  SQLException {
        String state = item.getMetadataFirstValue(workflowState[0], workflowState[1], workflowState[2], Item.ANY) ;
        if (state == null || ! state.equals(readyForEmail)) {
            debug("not including " + item + ": not ready for email");
            return false;
        }
        Bundle[] bundles = item.getBundles("ORIGINAL");
        if (1 != bundles.length) {
            warn("not including " + item + ": it has no/multiple ORIGINAL bundles");
            return false;
        }
        Bitstream[] bits = bundles[0].getBitstreams();
        if (1 != bits.length) {
            warn("not including " + item + ": it has no/multiple ORIGINAL bitstreams");
            return false;
        }
        return true;
    }

    private void markEmailSent(FacultyUserInfo fac, List<Item> facItems, DCDate sendDate) throws SQLException, AuthorizeException {
        if (facItems != null) {
            String message = "notification about archival date=" + byDate + " send-to=" + fac.getEmail() + " id=" + fac.getUniqueId();
            for (Item item : facItems) {
                item.setMetadataSingleValue(workflowState[0], workflowState[1], workflowState[2], null, emailedValue);
                setProvenance(item, sendDate, message);
                item.update();
                info(String.format("update %s: %s.%s.%s=%s", item.toString(), workflowState[0], workflowState[1], workflowState[2], emailedValue));
            }
            if (!testRun) {
                context.commit();
                info("committing changes to articles by " + fac);
            }
        }
    }

    private String getEmailTemplate(String templateFilename) throws IOException {
        String templateEmail = "";
        for (String line : FileUtils.readLines(new File(templateFilename))) {
            if ((!line.isEmpty()) && line.charAt(0) == '#') {
                continue;
            }
            templateEmail += line + "\n";
        }
        if (templateEmail.isEmpty()) {
            throw new RuntimeException(templateFilename + " is empty; cowardly refusing to proceed");
        }
        return templateEmail;
    }

    private void sendMimeMessage(String templaeEmail, FacultyUserInfo fac, String citations) throws MessagingException {
        Session session = new DSpace().getServiceManager().
                getServicesByType(EmailService.class).get(0).getSession();

        Object[] formatArgs = {
                fac.getLastName(), fac.getFirstName(), fac.getDepartment(), fac.getUniqueId(), fac.getEmail(),
                citations, byDate, cssStyle};
        String mailBody = MessageFormat.format(templaeEmail, formatArgs);

        // TODO - use real email
        String toAuthor = (null != redirectEnail) ? redirectEnail : "momeven@gmail.com";

        // Create message
        BufferedInputStream body = new BufferedInputStream(new StringInputStream(mailBody));
        MimeMessage message = new MimeMessage(session, body);
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(toAuthor));

        message.setSentDate(new Date());

        if (verbose) {
            info("");
            info("# " + fac + " buildMimeMessage");
            for (Address adr : message.getAllRecipients()) {
                info("# " + fac + " To: " + adr.toString());
            }
            String[] lines = mailBody.split("\n");
            for (String line : lines) {
                info("# " + fac + " " + line);
            }
        }

        if (doMail) {
            info("sending message for " + fac + " to " + toAuthor);
            Transport.send(message);
        }
    }

    private String citationList(List<Item> items) throws SQLException {
        String citations = "";
        for (Item i : items) {
            String citation = shortCitation(i);
            citations += "\t" + "<" + htmlTag + ">\n\t" + citation + "\n\t</" + htmlTag + ">" + "\n";
        }
        return citations;
    }

    /**
     * return shortened citation, formatted as follows:
     * Title, at most 5 authors - if there are  more than five add etal,  if easy also add Publication - Year, Journal Name, doi
     * <p>
     * return null if dso has no title, no authors and/or no publication year
     *
     * @param dso
     * @return null of shortened citation
     */
    public static String shortCitation(DSpaceObject dso) {
        // Title, at most 5 authors - if there are  more than five add etal,  Publication - Year, Journal Name, doi
        String title = dso.getMetadataFirstValue("dc", "title", null, Item.ANY);
        if (StringUtils.isEmpty(title)) return null;
        String authors = authorString(dso);
        if (StringUtils.isEmpty(authors)) return null;
        String date = dso.getMetadataFirstValue("dc", "date", "issued", Item.ANY);
        if (StringUtils.isEmpty(date)) return null;
        String cite = String.format("%s, %s, %s", title, authors, date);
        String relation = dso.getMetadataFirstValue("dc", "relation", "ispartof", Item.ANY);
        if (StringUtils.isNotEmpty(relation)) {
            cite += ", " + relation;
        }
        String doi = dso.getMetadataFirstValue("dc", "identifier", "doi", Item.ANY);
        if (StringUtils.isNotEmpty(relation)) {
            cite += ", " + doi;
        }
        return cite;
    }

    public static String authorString(DSpaceObject dso) {
        String authors = "";
        Metadatum[] dcv = dso.getMetadata("dc", "contributor", "author", Item.ANY);
        int nauthors = 0;
        for (Metadatum dc : dcv) {
            if (nauthors == MAX_AUTHORS) {
                authors += "; " + etAl;
                break;
            }
            authors = authors + (nauthors == 0 ? "" : "; ") + dc.value;
            nauthors++;
        }
        return authors;
    }

    public static void main(String argv[]) {
        MailerArguments args = new MailerArguments("OarMailer");

        try {
            Context context = new Context();
            if (args.parseArgs(argv, context)) {
                Mailer mailer = new Mailer(args, context);
                mailer.doit(args.getTemplateFilename(), args.getValue(FUIArguments.EMPLOYEEID_LONG, OptionMode.OPTIONAL));
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            args.usage();
            System.exit(1);
        }
    }
}


class MailerArguments extends FUIArguments {

    public static String SELECT = "s";
    public static String SELECT_LONG = "select";

    public static String DOMAIL = "m";
    public static String DOMAIL_LONG = "mail";

    public static String UNIQUEID = "i";
    public static String UNIQUEID_LONG = "uniqueid";

    public static String SELECTVALUE = "V";
    public static String SELECTVALUE_LONG = "value";

    public static String EMAILEDVALUE = "n";
    public static String EMAILEDVALUE_LONG = "newvalue";

    public static String CITATION = "c";
    public static String CITATION_LONG = "citation";

    public static String SUBJECT = "S";
    public static String SUBJECT_LONG = "subject";

    public static String REDIRECT_EMAIL = "R";
    public static String REDIRECT_EMAIL_LONG = "redirect_email";

    public static String TEMPLATE = "T";
    public static String TEMPLATE_LONG = "template";
    public static String TEMPLATE_DEFAULT = "/config/emails/oar_author_inform";

    public static String BYDATE = "b";
    public static String BYDATE_LONG = "bydate";

    public static String HTMLTAG = "H";
    public static String HTMLTAG_LONG = "html_tag";
    public static String HTMLTAG_DEFAULT = "li";

    public static String CSSSTYLE = "C";
    public static String CSSSTYLE_LONG = "css_style";
    public static String CSSSTYLE_DEFAULT = "li { margin-bottom: 1ex; margin-top: 1ex;}" ;

    MailerArguments(String klass) {
        super(klass);
        options.addOption(EPERSON, EPERSON_LONG, true, "email/netid of person perfoming changes");
        options.addOption(TESTRUN, TESTRUN_LONG, false, "test - do not commit database changes - must specify " + REDIRECT_EMAIL_LONG);
        options.addOption(DOMAIL, DOMAIL_LONG, false, "send emails - default false; must give option if not running in test mode ");
        options.addOption(SELECT, SELECT_LONG, true, "fully qualified metadata value by which to select items for email notifications");
        options.addOption(SELECTVALUE, SELECTVALUE_LONG, true, "value used with select metadata field to select items");
        options.addOption(EMAILEDVALUE, EMAILEDVALUE_LONG, true, "new value for select metadata field to indicate that email was sent");
        options.addOption(UNIQUEID, UNIQUEID_LONG, true, "fully qualified metadata field used for employee ids");
        options.addOption(CITATION, CITATION_LONG, true, "metadata value for citation field");
        options.addOption(BYDATE, BYDATE_LONG, true, "date by which to refuse archival");
        options.addOption(SUBJECT, SUBJECT_LONG, true, "email subject");
        options.addOption(REDIRECT_EMAIL, REDIRECT_EMAIL_LONG, true, "use this email instead of faculty email - must sepecify " + TESTRUN_LONG);
        options.addOption(TEMPLATE, TEMPLATE_LONG, true, "email template file - default $DSPACE_HOME/" + TEMPLATE_DEFAULT);
        options.addOption(HTMLTAG, HTMLTAG_LONG, true, "html tag used to separate citations - default " + HTMLTAG_DEFAULT);
        // can't pu CSS in raw email template because it contains '{' '}' which are intepreted by theformatter as arguments
        options.addOption(CSSSTYLE, CSSSTYLE_LONG, true, "css style, passed to email template, so it can be included, default " + CSSSTYLE_DEFAULT);
    }

    String getTemplateFilename() {
        String fname = getOptionalValue(TEMPLATE_LONG);
        if (null == fname) {
            fname = ConfigurationManager.getProperty("dspace.dir") + TEMPLATE_DEFAULT;
        }
        return fname;
    }

    String getHtmlTag() {
        String tag = getOptionalValue(HTMLTAG_LONG);
        if (null == tag) {
            tag = HTMLTAG_DEFAULT;
        }
        return tag;
    }

    String getCssStyle() {
        String tag = getOptionalValue(CSSSTYLE_LONG);
        if (null == tag) {
            tag = CSSSTYLE_DEFAULT;
        }
        return tag;
    }

    boolean getDoMail() {
        return getBoolValue(DOMAIL_LONG, false);
    }

    public Boolean parseArgs(String[] argv, Context ctxt)  {
        boolean error = false;
        if (!super.parseArgs(argv))
            return false;
        try {
            ctxt.setCurrentUser(getEPerson(ctxt, EPERSON_LONG, OptionMode.REQUIRED));
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            error = true;
        }
        if (getTestRun()) {
            /* if  test mode - must have redirect email */
            if (null == getOptionalValue(REDIRECT_EMAIL_LONG)) {
                System.err.println("must give " + REDIRECT_EMAIL_LONG + " parameter");
                error = true;
           }
        }  else {
            // do it for real only if sending emails
            error =  ! getDoMail();
            if (error) {
                System.err.println("must specify " + DOMAIL_LONG + " parameter");
            }
        }
        if (error) {
            usage();
        }
        return !error;
    }
}
