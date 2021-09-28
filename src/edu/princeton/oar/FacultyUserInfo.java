package edu.princeton.oar;

import org.apache.commons.cli.*;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dspace.core.ConfigurationManager;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class FacultyUserInfo {
    private String first_name, last_name;
    private String email;
    private String department;
    private String uniqueId;

    public String getLastName() {
        return last_name;
    }

    public String getFirstName() {
        return first_name;
    }

    public String getEmail() {
        return email;
    }

    public String getDepartment() {
        return department;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    static final Logger log = Logger.getLogger(FacultyUserInfo.class);

    public FacultyUserInfo(FacultyDataSource source, String uniqueId) throws IOException, SAXException, ParserConfigurationException {
        this.uniqueId = uniqueId;

        InputSource is = new InputSource(new StringReader(source.getXml(uniqueId)));

        try {
            parseSymplecticXml(is);
        } catch (RuntimeException e) {
            throw new RuntimeException("DataSource " + source + ": " + e.getMessage());
        }
        if (log.isDebugEnabled())
            log.debug("Created " + this);
    }

    private void parseSymplecticXml(InputSource is) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(is);

        Element user = (Element) doc.getElementsByTagName("api:object").item(0);
        NamedNodeMap attrs = user.getAttributes();

        String cat = attrs.getNamedItem("category").getTextContent();
        if (!"user".equals(cat)) {
            throw new RuntimeException("returned" + cat + " object; expecting a user object");
        }
        String propid = attrs.getNamedItem("proprietary-id").getTextContent();
        if (!uniqueId.equals(propid)) {
            throw new RuntimeException("returned user with id" + propid + "; expecting a user with " + uniqueId);
        }

        email = user.getElementsByTagName("api:email-address").item(0).getTextContent();
        department = user.getElementsByTagName("api:primary-group-descriptor").item(0).getTextContent();
        last_name = user.getElementsByTagName("api:last-name").item(0).getTextContent();
        first_name = user.getElementsByTagName("api:first-name").item(0).getTextContent();
    }

    public String toString() {
        return String.format("%s[%s-%s]", getClass().getSimpleName(), uniqueId, email);
    }

    public static void main(String argv[]) throws IOException, ParseException, ParserConfigurationException, SAXException {
        FUIArguments args = new FUIArguments("FacultyUserInfo");
        if (args.parseArgs(argv)) {
            FacultyDataSource src = args.getDataSource();
            String xml = src.getXml(args.getUniqueId());
            if (args.isVerbose()) {
                System.out.println(xml);
            }
            FacultyUserInfo fac = new FacultyUserInfo(src, args.getUniqueId());
            System.out.println("" + fac + " : " + fac.getLastName() + ", " + fac.getFirstName() +  " department  " + fac.getDepartment());
        }
    }
}

abstract class FacultyDataSource {
    static final String UTF_8 = "UTF-8";

    public abstract String getXml(String uniqueid) throws IOException ;

    static FacultyDataSource create(String dir) {
        return new FacultyFileDataSource(dir);
    }

    static FacultyDataSource create(String url_or_dir, String user,String pwd) {
        if (url_or_dir.startsWith("http"))
            return new FacultyUrlDataSource(url_or_dir, user, pwd);
        return new FacultyFileDataSource(url_or_dir);
    }
}

class FacultyUrlDataSource extends FacultyDataSource {
    private String SymplecticURL = null;
    private String SymplecticUser = null;
    private String SymplecticPassword = null;
    private DefaultHttpClient client = null;

    public FacultyUrlDataSource() {
        create(null, null, null);
    }

    public FacultyUrlDataSource(String url, String user, String pwd) {
        if (null == url) {
            url = ConfigurationManager.getProperty("symplectic.api.faculty");
            if (null == url) {
                throw new RuntimeException("no 'symplectic.api.faculty' configuration setting");
            }
        }
        SymplecticURL = url;
        if (null == user) {
            user = ConfigurationManager.getProperty("symplectic.api.user");
            if (null == url) {
                throw new RuntimeException("no 'symplectic.api.user' configuration setting");
            }
        }
        SymplecticUser = user;
        if (null == pwd) {
            pwd = ConfigurationManager.getProperty("symplectic.api.password");
            if (null == url) {
                throw new RuntimeException("no 'symplectic.api.password' configuration setting");
            }
        }
        SymplecticPassword = pwd;

        client = new DefaultHttpClient();
        try {
            URI uri = new URI(SymplecticURL);
            client.getCredentialsProvider().setCredentials(
                    new AuthScope(uri.getHost(), uri.getPort()),
                    new UsernamePasswordCredentials(SymplecticUser, SymplecticPassword));
        } catch (URISyntaxException e) {
            throw new RuntimeException("invalid URI " + SymplecticURL);
        }
    }

    private String makeUrfFromUniqueId(String uniqueId) {
        String url = SymplecticURL.replace("UNIQUEID", uniqueId);
        return url;
    }

    public String getXml(String uniqueId) throws IOException {
        HttpGet request = new HttpGet(makeUrfFromUniqueId(uniqueId));
        HttpResponse response = client.execute(request);
        String body = EntityUtils.toString(response.getEntity(), UTF_8);
        FacultyUserInfo.log.debug(body);
        return body;
    }
}

class FacultyFileDataSource extends FacultyDataSource {
    private String SymplecticFileDir = null;

    public FacultyFileDataSource(String dir) {
        SymplecticFileDir = dir;
    }

    public String getXml(String uniqueId) throws IOException {
        return new String(Files.readAllBytes(Paths.get(SymplecticFileDir, uniqueId)));
    }
}

class FUIArguments extends edu.princeton.cli.Arguments {
    protected FacultyDataSource dataSource;

    protected static final Logger log = Logger.getLogger(FUIArguments.class);

    public static String URL = "U";
    public static String URL_LONG = "url";

    public static String USER = "u";
    public static String USER_LONG = "user";

    public static String PASSWORD = "p";
    public static String PASSWORD_LONG = "password";

    public static String EMPLOYEEID = "E";
    public static String EMPLOYEEID_LONG = "employeeid";

    public static String DIR = "d";
    public static String DIR_LONG = "directory";

    public FUIArguments(String mainClass) {
        super(mainClass);

        options.addOption(URL, URL_LONG, true, "symplectic api request url - contains UNIQUEID, to be replaced by given uniqueid value ");
        options.addOption(DIR, DIR_LONG, true, "directory: path of dir that contains faculty user xml file");
        options.addOption(USER, USER_LONG, true, "symplectic api user ");
        options.addOption(PASSWORD, PASSWORD_LONG, true, "symplectic password");
        options.addOption(EMPLOYEEID, EMPLOYEEID_LONG, true, "faculty unique employee id");
    }

    public FacultyDataSource getDataSource() throws ParseException {
        checkReady();
        String dir = getOptionalValue(DIR_LONG);
        if (null != dir) {
            return FacultyDataSource.create(dir);
        } else {
            String url = getOptionalValue(URL_LONG);
            if (null != url) {
             return FacultyDataSource.create(url,
                     getValue(USER_LONG, OptionMode.REQUIRED),
                     getValue(PASSWORD_LONG, OptionMode.REQUIRED));
            }
        }
        throw new ParseException(
                "must give [" + klass  + "] " +  URL_LONG + " or " + DIR_LONG + " parameter.");
    }

    public String getUniqueId() throws ParseException {
        return getValue(EMPLOYEEID_LONG, OptionMode.REQUIRED);
    }
}
