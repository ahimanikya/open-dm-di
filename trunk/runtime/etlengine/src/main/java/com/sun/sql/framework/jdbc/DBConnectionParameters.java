/*
 * BEGIN_HEADER - DO NOT EDIT
 * 
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the "License").  You may not use this file except
 * in compliance with the License.
 *
 * You can obtain a copy of the license at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * HEADER in each file and include the License file at
 * https://open-jbi-components.dev.java.net/public/CDDLv1.0.html.
 * If applicable add the following below this CDDL HEADER,
 * with the fields enclosed by brackets "[]" replaced with
 * your own identifying information: Portions Copyright
 * [year] [name of copyright owner]
 */

/*
 * @(#)DBConnectionParameters.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.jdbc;

import java.util.Properties;

import org.w3c.dom.Element;

import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.utils.ScEncrypt;
import com.sun.sql.framework.utils.StringUtil;
import com.sun.sql.framework.utils.XmlUtil;

/**
 * Holds configuration parameters for a database connection.
 * 
 * @author Jonathan Giron
 * @version 
 */
public class DBConnectionParameters implements Cloneable, Comparable {

    // Start of xml-specific declarations.
    /** CONNECTION_DEFINITION is the XML tag for an SQLConnectionDefinition */
    public static final String CONNECTION_DEFINITION_TAG = "connectiondef"; // NOI18N

    /** CONNECTION_DESC_TAG is the XML tag for description */
    public static final String CONNECTION_DESC_TAG = "description"; // NOI18N

    /** CONNECTION_NAME_TAG is the XML tag for name */
    public static final String CONNECTION_NAME_TAG = "name"; // NOI18N

    /** Attribute: database name */
    public static final String DB_VENDOR_ATTR = "dbName";

    /** Attribute: JDBC driver name */
    public static final String DRIVER_NAME_ATTR = "driverName";

    /** Attribute: DataSource JNDI path */
    public static final String DS_JNDI_PATH_ATTR = "dsJndiPath";

    /** Attribute: JDBC ConnectionProvider path/ OTD path */
    public static final String OTD_PATH_NAME_ATTR = "otdPathName";

    /** Attribute: Password */
    public static final String PASSWORD_ATTR = "password";

    /** Attribute: URL */
    public static final String URL_ATTR = "dbUrl";

    /** Attribute: user name */
    public static final String USER_NAME_ATTR = "userName";

    // end xml-specific

    /** LOG_CATEGORY is the name of this class. */
    private static final String LOG_CATEGORY = DBConnectionParameters.class.getName();
    protected String dbType = "";

    /** User-supplied description */
    protected volatile String description;
    protected String driverClass = "";
    protected String dsJndiPath = "";

    protected String jdbcUrl = "";

    /** User-supplied connection name */
    protected volatile String name;
    protected String otdPathName = "";
    protected String password = "";
    protected String userName = "";

    /**
     * DBConnectionParameters Constructor is used for the potential collection of
     * DBConnectionDefinitions that might be parsed from a given file.
     */
    public DBConnectionParameters() {

    }

    /**
     * Constructs an instance of DBConnectionParameters using the information contained in
     * the given XML element.
     * 
     * @param theElement DOM element containing XML representation of this new
     *        DBConnectionParameters instance
     * @throws BaseException if error occurs while parsing
     */
    public DBConnectionParameters(Element theElement) throws BaseException {
        parseXML(theElement);
    }

    /**
     * Creates a clone of this DBConnectionParameters object.
     * 
     * @return clone of this object
     */
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e.toString());
        }
    }

    /**
     * Compares name of this DBConnectionParameters with that of the given object for
     * sorting purposes only.
     * 
     * @param refObj SQLColumn to be compared.
     * @return -1 if the name is less than obj to be compared. 0 if the name is the same.
     *         1 if the name is greater than obj to be compared.
     * @throws ClassCastException if refObj is not of type DBConnectionParameters
     * @see Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(Object refObj) {
        DBConnectionParameters defn = (DBConnectionParameters) refObj;
        return name.compareTo(defn.name);
    }

    /**
     * Doesn't take table name into consideration.
     * 
     * @param refObj SQLColumn to be compared.
     * @return true if the object is identical. false if it is not.
     */
    public boolean equals(Object refObj) {
        if (!(refObj instanceof DBConnectionParameters)) {
            return false;
        }

        if (this == refObj) {
            return true;
        }

        DBConnectionParameters defn = (DBConnectionParameters) refObj;

        boolean result = (name != null) ? name.equals(defn.name) : (defn.name == null);
        result &= (dbType != null) ? dbType.equals(defn.dbType) : (defn.dbType == null);
        result &= (driverClass != null) ? this.driverClass.equals(defn.driverClass) : (defn.driverClass == null);
        result &= (jdbcUrl != null) ? jdbcUrl.equals(defn.jdbcUrl) : (defn.jdbcUrl == null);
        result &= (userName != null) ? userName.equals(defn.userName) : (defn.userName == null);
        result &= (password != null) ? this.password.equals(defn.password) : (defn.password == null);
        result &= (otdPathName != null) ? this.otdPathName.equals(defn.otdPathName) : (defn.otdPathName == null);
        result &= (dsJndiPath != null) ? this.dsJndiPath.equals(defn.dsJndiPath) : (defn.dsJndiPath == null);

        return result;
    }

    public Properties getConnectionProperties() {
        Properties props = new Properties();
        props.put(DBConnectionFactory.PROP_DBTYPE, getDBType());
        props.put(DBConnectionFactory.PROP_DRIVERCLASS, getDriverClass());
        props.put(DBConnectionFactory.PROP_URL, getConnectionURL());
        props.put(DBConnectionFactory.PROP_USERNAME, getUserName());
        props.put(DBConnectionFactory.PROP_PASSWORD, getPassword());
        props.put(DBConnectionFactory.PROP_OTD_PATH, getOTDPathName());
        props.put(DBConnectionFactory.PROP_DS_JNDI_PATH, getJNDIPath());
        return props;
    }

    /**
     * Gets URL used to reference and establish a connection to the data source referenced
     * in this object.
     * 
     * @return URL pointing to the data source
     */
    public String getConnectionURL() {
        return jdbcUrl;
    }

    /**
     * Gets descriptive name, if any, of type of DB data source from which this metadata
     * content was derived, e.g., "Oracle9" for an Oracle 9i database, etc. Returns null
     * if content was derived from a non-DB source, such such as a flatfile.
     * 
     * @return vendor name of source database; null if derived from non-DB source
     */
    public String getDBType() {
        return dbType;
    }

    /**
     * Getter for description
     * 
     * @return description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Gets fully-qualified class name of driver used to establish a connection to the
     * data source referenced in this object
     * 
     * @return fully-qualified driver class name
     */
    public String getDriverClass() {
        return driverClass;
    }

    /**
     * @return Returns the dsJndiPath.
     */
    public String getJNDIPath() {
        return dsJndiPath;
    }

    /**
     * Get name DBConnectionDefinition
     * 
     * @return Return name for the SQLConnectionDefnition
     */
    public synchronized String getName() {
        return name;
    }

    /**
     * Getter for OTD Path which includes OTD name.
     * 
     * @return
     */
    public String getOTDPathName() {
        return this.otdPathName;
    }

    /**
     * Gets password, if any, used in authenticating a connection to the data source
     * referenced in this object.
     * 
     * @return password, if any, used for authentication purposes
     */
    public String getPassword() {
        return password;
    }

    /**
     * Gets username, if any, used in authenticating a connection to the data source
     * referenced in this object.
     * 
     * @return username, if any, used for authentication purposes
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Returns the hashCode for this object.
     * 
     * @return the hashCode of this object.
     */
    public int hashCode() {
        int myHash = (name != null) ? name.hashCode() : 0;
        myHash += (dbType != null) ? dbType.hashCode() : 0;
        myHash += (driverClass != null) ? driverClass.hashCode() : 0;
        myHash += (jdbcUrl != null) ? jdbcUrl.hashCode() : 0;
        myHash += (userName != null) ? userName.hashCode() : 0;
        myHash += (password != null) ? password.hashCode() : 0;
        myHash += (otdPathName != null) ? otdPathName.hashCode() : 0;
        myHash += (dsJndiPath != null) ? dsJndiPath.hashCode() : 0;
        return myHash;
    }

    /**
     * Parses contents of internal Element containing XML representation of this instance.
     * 
     * @param xmlElement Element containing connection information
     * @throws BaseException if parsing fails
     */
    public void parseXML(Element xmlElement) throws BaseException {
        try {
            setName(xmlElement.getAttribute(CONNECTION_NAME_TAG));
            setDescription(xmlElement.getAttribute(CONNECTION_DESC_TAG));

            setConnectionURL(XmlUtil.getAttributeFrom(xmlElement, URL_ATTR, false));
            setDBType(XmlUtil.getAttributeFrom(xmlElement, DB_VENDOR_ATTR, false));
            setUserName(XmlUtil.getAttributeFrom(xmlElement, USER_NAME_ATTR, false));
            String passwd = XmlUtil.getAttributeFrom(xmlElement, PASSWORD_ATTR, false);

            // If password is empty, we should not call encrypt.
            // This was creating a issue StringIndexOutOfBounds during runtime QAI 66935
            if (!StringUtil.isNullString(passwd)) {
                String newPass = ScEncrypt.decrypt(this.getUserName(), passwd);
                setPassword(newPass);
            }
            setDriverClass(XmlUtil.getAttributeFrom(xmlElement, DRIVER_NAME_ATTR, false));
            setOTDPathName(XmlUtil.getAttributeFrom(xmlElement, OTD_PATH_NAME_ATTR, false));
            setJNDIPath(XmlUtil.getAttributeFrom(xmlElement, DS_JNDI_PATH_ATTR, false));

        } catch (Exception ex) {
            throw new BaseException(LOG_CATEGORY + ": Could not parse Connection Definition ", ex);
        }
    }

    /**
     * Set connection URL
     * 
     * @param newUrl - url
     */
    public void setConnectionURL(String newUrl) {
        jdbcUrl = newUrl;
    }

    /**
     * Set DB type
     * 
     * @param newDBType - type
     */
    public void setDBType(String newDBType) {
        dbType = (newDBType != null) ? newDBType : "";
    }

    /**
     * Setter for description
     * 
     * @param newDesc for this connection defn
     */
    public void setDescription(String newDesc) {
        description = (newDesc != null && newDesc.trim().length() != 0) ? newDesc.trim() : "";
    }

    /**
     * Set JDBC driver class
     * 
     * @param newDriverClass - driver class
     */
    public void setDriverClass(String newDriverClass) {
        driverClass = newDriverClass;
    }

    /**
     * @param dsJndiPath The dsJndiPath to set.
     */
    public void setJNDIPath(String dsJndiPath) {
        this.dsJndiPath = dsJndiPath;
    }

    /**
     * Set the name of this ConnectionInfo.
     * 
     * @param newName ConnectionInfo name
     */
    public void setName(String newName) {
        name = (newName != null && newName.trim().length() != 0) ? newName.trim() : "";
    }

    /**
     * Setter for OTD Path which includes OTD name.
     * 
     * @param newPathName
     */
    public void setOTDPathName(String newPathName) {
        this.otdPathName = newPathName;
    }

    /**
     * Set password
     * 
     * @param newPassword - password
     */
    public void setPassword(String newPassword) {
        password = newPassword;
    }

    /**
     * Set user name
     * 
     * @param newUserName - username
     */
    public void setUserName(String newUserName) {
        userName = newUserName;
    }

    /**
     * Return toString
     * 
     * @return String
     */
    public String toString() {
        StringBuffer buf = new StringBuffer(1000);
        final String nl = ", ";

        buf.append("{ name: \"").append(name).append("\"").append(nl);
        buf.append("DBType: \"").append(dbType).append("\"").append(nl);
        buf.append("driverClass: \"").append(driverClass).append("\"").append(nl);
        buf.append("jdbcUrl: \"").append(jdbcUrl).append("\"").append(nl);
        buf.append("userName: \"").append(userName).append("\"").append(nl);
        buf.append("otdPathName: \"").append(otdPathName).append("\"").append(nl);
        buf.append("dsJndiPath: \"").append(dsJndiPath).append("\"");
        buf.append(" }");
        return buf.toString();
    }

    /**
     * Writes contents of this DBConnectionParameters instance out as an XML element,
     * using the default prefix.
     * 
     * @return String containing XML representation of this DBConnectionParameters
     *         instance
     */
    public synchronized String toXMLString() {
        return toXMLString("\t");
    }

    /**
     * Writes contents of this DBConnectionParameters instance out as an XML element,
     * using the given prefix.
     * 
     * @param prefix String used to prefix each new line of the XML output
     * @return String containing XML representation of this DBConnectionParameters
     *         instance
     */
    public synchronized String toXMLString(String prefix) {
        StringBuffer xml = new StringBuffer(1000);
        if (prefix == null) {
            prefix = "";
        }

        xml.append(prefix);
        xml.append("<").append(CONNECTION_DEFINITION_TAG);

        if (!StringUtil.isNullString(name)) {
            xml.append(" ").append(CONNECTION_NAME_TAG).append("=\"").append(this.name.trim()).append("\"");
        }

        if (!StringUtil.isNullString(description)) {
            xml.append(" ").append(CONNECTION_DESC_TAG).append("=\"").append(this.description.trim()).append("\"");
        }

        if (!StringUtil.isNullString(driverClass)) {
            xml.append(" ").append(DRIVER_NAME_ATTR).append("=\"").append(driverClass.trim()).append("\"");
        }

        if (!StringUtil.isNullString(dbType)) {
            xml.append(" ").append(DB_VENDOR_ATTR).append("=\"").append(dbType.trim()).append("\"");
        }

        if (!StringUtil.isNullString(jdbcUrl)) {
            xml.append(" ").append(URL_ATTR).append("=\"").append(jdbcUrl.trim()).append("\"");
        }

        if (!StringUtil.isNullString(userName)) {
            xml.append(" ").append(USER_NAME_ATTR).append("=\"").append(userName.trim()).append("\"");
        }

        if (!StringUtil.isNullString(password) && !StringUtil.isNullString(userName)) {
            String newPass = ScEncrypt.encrypt(userName.trim(), password.trim());
            xml.append(" ").append(PASSWORD_ATTR).append("=\"").append(newPass).append("\"");
        }

        if (!StringUtil.isNullString(otdPathName)) {
            xml.append(" ").append(OTD_PATH_NAME_ATTR).append("=\"").append(otdPathName).append("\"");
        }

        if (!StringUtil.isNullString(dsJndiPath)) {
            xml.append(" ").append(DS_JNDI_PATH_ATTR).append("=\"").append(dsJndiPath).append("\"");
        }

        xml.append(">\n");

        xml.append(prefix).append("</").append(CONNECTION_DEFINITION_TAG).append(">").append("\n");

        return xml.toString();
    }
}
