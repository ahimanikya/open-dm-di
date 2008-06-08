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
 * @(#)SQLPart.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.jdbc;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.w3c.dom.DOMException;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.utils.Attribute;
import com.sun.sql.framework.utils.StringUtil;
import com.sun.sql.framework.utils.XmlUtil;

/**
 * Binds a SQL statement string with an associated statement type (String descriptor) and
 * connection pool name.
 * 
 * @author Sudhendra Seshachala
 * @author Jonathan Giron
 * @version 
 */
public class SQLPart {

    /**
     * Special String sequence to separate distinct SQL statements in a multiple-statement
     * String.
     */
    public static final char STATEMENT_SEPARATOR = '\uFFFF';

    public static final String STMT_CHECKTABLEEXISTS = "checkTableExists"; //NOI18N

    public static final String STMT_CREATE = "createStatement"; //NOI18N

    public static final String STMT_CREATEBEFOREPROCESS = "createBeforeProcess"; //NOI18N

    public static final String STMT_CREATEDBLINK = "createDbLinkStatement"; //NOI18N

    public static final String STMT_CREATEEXTERNAL = "createExternalStatement"; //NOI18N

    public static final String STMT_CREATEFLATFILE = "createFlatfileStatement"; //NOI18N

    public static final String STMT_CREATELOGDETAILSTABLE = "createDetailsTableStatement"; //NOI18N

    public static final String STMT_CREATELOGSUMMARYTABLE = "createSummaryTableStatement"; //NOI18N

    public static final String STMT_CREATEREMOTELOGDETAILSTABLE = "createRemoteDetailsTableStatement"; //NOI18N

    public static final String STMT_DEFRAG = "defragStatement"; //NOI18N

    public static final String STMT_DELETE = "deleteStatement"; //NOI18N

    public static final String STMT_DELETEBEFOREPROCESS = "deleteBeforeProcessStatement"; //NOI18N

    public static final String STMT_DELETEINVALIDROWFROMSUMMARY = "deleteInvalidRowFromSummaryTable"; //NOI18N

    public static final String STMT_DROP = "dropStatement"; //NOI18N

    public static final String STMT_DROPDBLINK = "dropDbLinkStatement"; //NOI18N

    public static final String STMT_INITIALIZESTATEMENTS = "initializeStatements"; //NOI18N

    public static final String STMT_INSERT = "insertStatement"; //NOI18N

    public static final String STMT_INSERTEXECUTIONRECORD = "insertExecutionRecordStatement"; //NOI18N

    public static final String STMT_INSERTSELECT = "insertSelectStatement"; //NOI18N

    public static final String STMT_MERGE = "mergeStatement"; //NOI18N

    public static final String STMT_REMOUNTREMOTETABLE = "remountRemoteTableStatement"; // NOI18N

    public static final String STMT_ROWCOUNT = "rowCountStatement"; //NOI18N

    public static final String STMT_SELECT = "selectStatement"; //NOI18N

    public static final String STMT_SELECTEXECUTIONIDFROMSUMMARY = "selectExecutionIdFromSummaryTable"; // NOI18N

    public static final String STMT_SELECTREJECTEDROWCTFROMDETAILS = "selectRejectedRowCountFromDetailsTable"; //NOI18N

    public static final String STMT_STATICINSERT = "staticInsertStatement"; //NOI18N

    public static final String STMT_TRUNCATE = "truncateStatement"; //NOI18N

    public static final String STMT_TRUNCATEBEFOREPROCESS = "truncateBeforeProcessStatement"; //NOI18N

    public static final String STMT_UPDATE = "updateStatement"; //NOI18N

    public static final String STMT_UPDATEEXECUTIONRECORD = "updateExecutionRecordStatement"; //NOI18N

    public static final String STMT_CORRELATED_SELECT = "correlatedSelect" ; //NOI18N

    public static final String STMT_CORRELATED_UPDATE = "correlatedUpdate" ; //NOI18N

    /** Tag name for associated XML element */
    public static final String TAG_SQLPART = "sqlPart"; //NOI18N

    /* XML attribute name: default file name (for SQL statements involving flat files) */
    private static final String ATTR_DEFAULT_NAME = "defaultFileName";

    /* XML attribute name: connection pool name */
    private static final String ATTR_POOLNAME = "connPoolName";

    /* XML attribute name: table name */
    private static final String ATTR_TABLE_NAME = "tableName";

    /* XML attribute name: statement type */
    private static final String ATTR_TYPE = "stmtType";

    /**
     * Special character string to substitute for STATEMENT_SEPARATOR when SQLPart is
     * marshalled to an XML string for persistence. Introduced to solve failure to
     * truncate issues discovered in WTs #65822, 66604, 67133.
     * 
     * @see #STATEMENT_SEPARATOR
     */
    private static final String XML_STATEMENT_SEPARATOR = "{@#END#@}";

    public static final String ATTR_JDBC_TYPE_LIST = "jdbcTypeList";   //NOI18N

    public static final String ATTR_DESTS_SRC = "DestinationsSource";   //NOI18N

    /**
     * Map of attributes; used by concrete implementations to store class-specific fields
     * without hardcoding them as member variables
     */
    private Map attributes = new HashMap();

    /* Name of associated connection pool */
    private String connPoolName;

    private String defaultFileName;

    /* SQL statement */
    private String sql;
    private Map sqlStmtMap = new HashMap();

    private String tableName;

    /* Statement type: create, delete, insert, select, etc. */
    private String type;

    /**
     * Creates a new instance of SQLPart, parsing the given DOM element to obtain its
     * content.
     * 
     * @param sqlElement DOM element containing SQL statement, type, etc.
     * @exception BaseException - exception
     */
    public SQLPart(Element sqlElement) throws BaseException {
        parseXML(sqlElement);
    }

    public SQLPart(String theTableName) {
        this.setTableName(theTableName);
    }

    /**
     * Creates a new instance of SQLPart with the given SQL statement, SQL type and
     * connection pool name.
     * 
     * @param statement SQL statement
     * @param sqlType statement type
     * @param connectionPool name of associated connection pool
     */
    public SQLPart(String statement, String sqlType, String connectionPool) {
        setSQL(statement);
        setType(sqlType);
        setConnectionPoolName(connectionPool);
    }

    public void addSQLStatement(String stmtType, String theSQL) {
        sqlStmtMap.put(stmtType, theSQL);
    }

    public Collection getAllSQLStatements() {
        return sqlStmtMap.values();
    }

    /**
     * Gets an attribute based on its name
     * 
     * @param attrName attribute Name
     * @return Attribute instance associated with attrName, or null if none exists
     */
    public Attribute getAttribute(String attrName) {
        return (Attribute) attributes.get(attrName);
    }

    /**
     * @see com.sun.jbi.ui.devtool.sql.framework.model.SQLObject#getAttributeNames
     */
    public Collection getAttributeNames() {
        return attributes.keySet();
    }

    /**
     * @see com.sun.jbi.ui.devtool.sql.framework.model.SQLObject#getAttributeObject
     */
    public Object getAttributeObject(String attrName) {
        Attribute attr = getAttribute(attrName);
        return (attr != null) ? attr.getAttributeValue() : null;
    }

    /**
     * Gets name of connection pool to use in executing this SQL statement.
     * 
     * @return name of connection pool to use
     */
    public String getConnectionPoolName() {
        return this.connPoolName;
    }

    /**
     * @return
     */
    public String getDefaultValue() {
        return this.defaultFileName;
    }

    /**
     * Returns Iterator of SQL string parts.
     * 
     * @return
     */
    public Iterator getIterator() {
        List list = new ArrayList();
        if ((this.sql != null) && (!"".equals(this.sql))) {
            StringTokenizer st = new StringTokenizer(this.sql, Character.toString(SQLPart.STATEMENT_SEPARATOR));
            while (st.hasMoreTokens()) {
                list.add(st.nextToken());
            }
        }
        return list.iterator();
    }

    public Map getTypeToStatementMap() {
        return sqlStmtMap;
    }

    /**
     * Gets SQL statement as a String.
     * 
     * @return associated SQL statement
     */
    public String getSQL() {
        return this.sql;
    }

    public String getSQL(String stmtType) {
        return (String) sqlStmtMap.get(stmtType);
    }

    /**
     * @return Table Name
     */
    public String getTableName() {
        return this.tableName;
    }

    /**
     * Gets SQL statement type.
     * 
     * @return statement type, e.g., "create", "insert", "select", "delete"
     */
    public String getType() {
        return this.type;
    }

    /**
     * Parses given XML element for the contents of this task node
     * 
     * @param element DOM Element to be parsed
     * @exception BaseException if error occurs while parsing
     */
    public void parseXML(Element element) throws BaseException {
        if (element == null) {
            throw new BaseException("Must supply non-null Element ref for parameter 'element'.");
        }

        connPoolName = element.getAttribute(ATTR_POOLNAME);
        if (StringUtil.isNullString(connPoolName)) {
            throw new BaseException("XML element has an empty or missing value for attribute '" + ATTR_POOLNAME + "'.");
        }

        type = element.getAttribute(ATTR_TYPE);
        if (StringUtil.isNullString(type)) {
            throw new BaseException("XML element has an empty or missing value for attribute '" + ATTR_TYPE + "'.");
        }

        this.tableName = element.getAttribute(ATTR_TABLE_NAME);
        this.defaultFileName = element.getAttribute(ATTR_DEFAULT_NAME);

        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node aNode = children.item(i);

            if (aNode.getNodeType() == Node.ELEMENT_NODE) {
                Element elem = (Element) aNode;
                if (elem.getNodeName().equals(Attribute.TAG_ATTR)) {
                    Attribute attr = new Attribute();
                    attr.parseXMLString(elem);
                    this.attributes.put(attr.getAttributeName(), attr);
                } else if (elem.getNodeName().equals("sql")) {
                    try {
                        // one sql part will contain only one sql string
                        NodeList sqlChildren = elem.getChildNodes();
                        Node sqlNode = sqlChildren.item(0);
                        sql = ((Text) sqlNode).getData();
                        if (sql == null || sql.trim().length() == 0) {
                            throw new BaseException("XML element has no SQL statement!");
                        }
                        sql = StringUtil.replaceInString(sql, XML_STATEMENT_SEPARATOR, Character.toString(STATEMENT_SEPARATOR)).trim();
                    } catch (DOMException e) {
                        throw new BaseException("Caught DOMException while parsing SQLPart.", e);
                    }
                }
            }
        }
    }

    /**
     * @see com.sun.jbi.ui.devtool.sql.framework.model.SQLObject#setAttribute
     */
    public void setAttribute(String attrName, Object val) {
        Attribute attr = getAttribute(attrName);
        if (attr != null) {
            attr.setAttributeValue(val);
        } else {
            attr = new Attribute(attrName, val);
            attributes.put(attrName, attr);
        }
    }

    /**
     * Sets name of connection pool to use in executing this SQL statement.
     * 
     * @param newPoolName name of connection pool to use
     */
    public void setConnectionPoolName(String newPoolName) {
        this.connPoolName = newPoolName;
    }

    /**
     * @param defValue
     */
    public void setDefaultValue(String defValue) {
        defaultFileName = defValue;
    }

    /**
     * Sets SQL statement body to the given string.
     * 
     * @param newSQL new SQL statement
     */
    public void setSQL(String newSQL) {
        this.sql = newSQL;
    }

    /**
     * @param theTableName Sets the table name
     */
    public void setTableName(String theTableName) {
        this.tableName = theTableName;
    }

    /**
     * Sets SQL statement type to the given String.
     * 
     * @param newType new statement type, e.g., "create", "insert", "select", "delete"
     */
    public void setType(String newType) {
        this.type = newType;
    }

    public String toString() {
        StringBuffer buf = new StringBuffer(60);
        if ((this.sql != null) && (!"".equals(this.sql))) {
            StringTokenizer st = new StringTokenizer(this.sql, Character.toString(SQLPart.STATEMENT_SEPARATOR));
            while (st.hasMoreTokens()) {
                buf.append(st.nextToken() + "\n\n");
            }
        }
        return buf.toString();
    }

    /**
     * Writes out an XML representation of this task node.
     * 
     * @return XML representation of the task node.
     */
    public String toXMLString() {
        return toXMLString("");
    }

    /**
     * Writes out an XML representation of this task node, prepending the given String to
     * each new line.
     * 
     * @param prefix String to prepend to each new line
     * @return XML representation of the task node.
     */
    public String toXMLString(String prefix) {
        StringBuffer buf = new StringBuffer(200);

        if (prefix == null) {
            prefix = "";
        }

        buf.append(prefix).append("<" + TAG_SQLPART + " ");
        buf.append(ATTR_POOLNAME + "=\"").append(connPoolName).append("\" ");
        if (!StringUtil.isNullString(this.tableName)) {
            if (this.tableName.startsWith("\"")) {
                this.tableName = XmlUtil.escapeXML(this.tableName);
            }
        }
        buf.append(ATTR_TABLE_NAME + "=\"").append(this.tableName).append("\" ");
        buf.append(ATTR_DEFAULT_NAME + "=\"").append(this.defaultFileName).append("\" ");
        buf.append(ATTR_TYPE + "=\"").append(type).append("\">\n");
        if (sql != null && sql.trim().length() != 0) {
            buf.append(prefix + "\t<sql>").append(
                XmlUtil.escapeXML(StringUtil.replaceInString(sql.trim(), Character.toString(STATEMENT_SEPARATOR), XML_STATEMENT_SEPARATOR)).trim()).append(
                "</sql>\n");
        }
        buf.append(toXMLAttributeTags(prefix));
        buf.append(prefix).append("</" + TAG_SQLPART + ">\n");

        return buf.toString();
    }

    /**
     * Generates XML elements representing this object's associated attributes.
     * 
     * @param prefix Prefix string to be prepended to each element
     * @return String containing XML representation of attributes
     */
    protected String toXMLAttributeTags(String prefix) {
        StringBuffer buf = new StringBuffer(100);

        Iterator iter = attributes.values().iterator();
        while (iter.hasNext()) {
            Attribute attr = (Attribute) iter.next();
            if (attr.getAttributeValue() != null) {
                buf.append(attr.toXMLString(prefix + "\t"));
            }
        }
        return buf.toString();
    }
}
