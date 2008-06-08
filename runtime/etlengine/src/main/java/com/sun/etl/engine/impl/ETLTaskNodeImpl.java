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
 * @(#)ETLTaskNodeImpl.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLEngineExecEvent;
import com.sun.etl.engine.ETLEngineListener;
import com.sun.etl.engine.ETLEngineLogEvent;
import com.sun.etl.engine.ETLTask;
import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.SQLPart;
import com.sun.sql.framework.utils.AttributeMap;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;

/**
 * @see com.sun.etl.engine.ETLTaskNode
 * @author Ahimanikya Satapathy
 * @version :
 */
public class ETLTaskNodeImpl implements ETLTaskNode {

    private static final String ATTR_DEPENDSON = "dependsOn"; //NOI18N
    private static final String ATTR_DN = "displayName"; //NOI18N

    private static final String ATTR_ID = "id"; //NOI18N

    private static final String ATTR_NAME = "name"; //NOI18N
    private static final String ATTR_TASKREFIDS = "taskRefIds"; //NOI18N
    private static final String ATTR_TASKTYPE = "taskType"; //NOI18N
    private static final String ATTR_TN = "tableName"; //NOI18N

    //Used only in init tasks
    private static final String TAG_INIT_PROCESS = "initMetadata";

    private static final String TAG_NEXT = "next"; //NOI18N

    // Used only for table-specific tasks
    private static final String TAG_PER_TABLE = "perTableStatements";
    private static final String TAG_SITUATION = "situation"; //NOI18N

    /**
     * Map of attributes; used by concrete implementations to store class-specific fields
     * without hardcoding them as member variables
     */
    protected AttributeMap attributes = new AttributeMap();

    /* Holds comma-delimited list of IDs of task node dependencies */
    private String dependsOn;

    /* Associated display name, if any. */
    private String displayName;

    /* Unique ID of this task */
    private String id;

    /* Parent of this task node. */
    private ETLEngine parent;

    /* Map of situation names to comma-separated String list of next task IDs. */
    private Map situationToNextTaskIdMap;

    private List sqlParts;

    /* Map of statement type keys to SQLPart instances. */
    private Map statementMap;

    /*
     * Runtime status, set by engine, useful while browsing/scheduling/refreshing the task
     * nodes at run time
     */
    private String status;

    private String tableName;

    private Map tableToStatementsMap;

    /* Task type descriptor */
    private String taskType;

    /**
     * Creates a new default instance of ETLTaskNodeImpl
     */
    public ETLTaskNodeImpl() {
        statementMap = new HashMap();
        tableToStatementsMap = new HashMap();
        situationToNextTaskIdMap = new HashMap();
        this.sqlParts = new ArrayList();
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#addNextETLTaskNode(String, String)
     */
    public int addNextETLTaskNode(String situation, String nextTaskID) {
        if (situation == null || ETLEngine.START.equals(nextTaskID)) {
            return -1;
        }

        String idList = (String) situationToNextTaskIdMap.get(situation);

        if (idList == null || idList.length() == 0) {
            idList = nextTaskID;
        } else {
            // Use Set to eliminate duplicates.
            Set nodeIds = new HashSet(StringUtil.createStringListFrom(idList));
            if (!nodeIds.contains(nextTaskID)) {
                nodeIds.add(nextTaskID);
                idList = StringUtil.createDelimitedStringFrom(new ArrayList(nodeIds));
            }
        }

        situationToNextTaskIdMap.put(situation, idList);
        return 0;
    }

    public void addOptionalTask(SQLPart sqlPart) {
        this.sqlParts.add(sqlPart);
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#addStatement(com.sun.sql.framework.jdbc.SQLPart)
     */
    public void addStatement(SQLPart newSql) {
        if (newSql == null) {
            throw new IllegalArgumentException("Must supply non-null ETLSQLStatement ref for parameter 'newSql'.");
        }

        statementMap.put(newSql.getType(), newSql);
    }

    public void addTableSpecificStatement(String aTableName, SQLPart newSql) {
        Map tableStmtMap = getOrCreateTableStatementMap(aTableName);
        tableStmtMap.put(newSql.getType(), newSql);
    }

    
    /**
     * Fire ETLEngineExecutionEvent 
     * @param evnt
     */
    public void fireETLEngineExecutionEvent(ETLEngineExecEvent evnt){
    	ETLEngineListener listener = parent.getETLEngineListener();
    	if (listener != null){
    		listener.executionPerformed(evnt);
    	}
    }
    
    /**
     * @see com.sun.etl.engine.ETLTaskNode#fireETLEngineLogEvent
     */
    public synchronized void fireETLEngineLogEvent(String logMessage) {
        fireETLEngineLogEvent(logMessage, Logger.INFO);
    }

    public synchronized void fireETLEngineLogEvent(String logMessage, int level) {
        ETLEngineListener listener;
        MessageManager msgMgr = MessageManager.getManager("com.sun.etl.engine.impl");

        try {
            listener = parent.getETLEngineListener();
            if (listener != null) {
                String msgMarker = "";
                if (!StringUtil.isNullString(displayName)) {
                    String collabName = parent.getDisplayName();
                    if (!StringUtil.isNullString(collabName)) {
                        msgMarker = msgMgr.getString("LBL_tasknode_marker_collab_dn", collabName.trim(), displayName.trim());
                    } else {
                        msgMarker = msgMgr.getString("LBL_tasknode_marker_dn_only", displayName.trim());
                    }
                }

                final String nl = System.getProperty("line.separator");
                listener.updateOutputMessage(new ETLEngineLogEvent(msgMarker, logMessage + nl, level));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getAllNextETLTaskNodes
     */
    public List getAllNextETLTaskNodes() {
        // Use a set to ensure no duplications, then copy to a new List.
        Set nodeSet = new HashSet();

        Iterator iter = situationToNextTaskIdMap.values().iterator();
        while (iter.hasNext()) {
            nodeSet.addAll(StringUtil.createStringListFrom((String) iter.next()));
        }

        return new ArrayList(nodeSet);
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getAllSituations
     */
    public List getAllSituations() {
        return new ArrayList(situationToNextTaskIdMap.keySet());
    }

    /**
     * Added for Flatfile DB init purpose
     * 
     * @return Map
     */
    public Map getAllStatements() {
        return statementMap;
    }

    public AttributeMap getAttributeMap() {
        return this.attributes;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getContext
     */
    public ETLEngineContext getContext() {
        return (parent == null) ? null : parent.getContext();
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getDependsOn
     */
    public String getDependsOn() {
        return this.dependsOn;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getDisplayName()
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getId
     */
    public String getId() {
        return this.id;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getNextTaskList(String)
     */
    public String getNextTaskList(String situation) {
        return (situationToNextTaskIdMap != null && situation != null) ? (String) situationToNextTaskIdMap.get(situation) : null;
    }

    /**
     * com.sun.etl.engine.ETLTaskNode.getOptionalTasks
     */
    public List getOptionalTasks() {
        return sqlParts;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getParent
     */
    public ETLEngine getParent() {
        return parent;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getStatement(String)
     */
    public SQLPart getStatement(String stmtType) {
        if (StringUtil.isNullString(stmtType)) {
            throw new IllegalArgumentException("Must supply non-empty String value for parameter 'stmtType'.");
        }

        return (SQLPart) statementMap.get(stmtType);
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getStatementTypes
     */
    public List getStatementTypes() {
        return new ArrayList(statementMap.keySet());
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getStatus
     */
    public String getStatus() {
        return status;
    }

    /**
     * com.sun.etl.engine.ETLTaskNode.getTableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getTableNamesWithSpecificStatements()
     */
    public Collection getTableNamesWithSpecificStatements() {
        return tableToStatementsMap.keySet();
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getTableSpecificStatement(java.lang.String,
     *      java.lang.String)
     */
    public SQLPart getTableSpecificStatement(String aTableName, String stmtType) {
        Map tableStmtMap = getOrCreateTableStatementMap(aTableName);
        return (SQLPart) tableStmtMap.get(stmtType);
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getTableSpecificStatements(java.lang.String)
     */
    public Map getTableSpecificStatements(String aTableName) {
        Map tableStmtMap = getOrCreateTableStatementMap(aTableName);
        return Collections.unmodifiableMap(tableStmtMap);
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#getTaskType
     */
    public String getTaskType() {
        return this.taskType;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#isStartNode
     */
    public boolean isStartNode() {
        ETLEngine.START.equals(this.getTaskType());
        return false;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#parseXML(Element)
     */
    public void parseXML(Element element) throws BaseException {
        if (element == null) {
            throw new BaseException("Must supply non-null Element ref for parameter 'element'.");
        }

        id = element.getAttribute(ATTR_ID);
        if (StringUtil.isNullString(id)) {
            throw new BaseException("XML element has an empty or missing value for attribute '" + ATTR_ID + "'.");
        }

        taskType = element.getAttribute(ATTR_TASKTYPE);
        if (StringUtil.isNullString(taskType)) {
            throw new BaseException("XML element has an empty or missing value for attribute '" + ATTR_TASKTYPE + "'.");
        }

        displayName = element.getAttribute(ATTR_DN);
        if (StringUtil.isNullString(taskType)) {
            displayName = null;
        }
        this.tableName = element.getAttribute(ATTR_TN);
        if (StringUtil.isNullString(taskType)) {
            this.tableName = null;
        }

        dependsOn = element.getAttribute(ATTR_DEPENDSON);

        NodeList list = element.getElementsByTagName(TAG_NEXT);
        parseNextTaskElements(list);

        // We only want child SQLPart.TAG_SQLPART not child of TAG_INIT_PROCESS.
        NodeList childNodes = element.getChildNodes();
        List childSQLParts = new ArrayList(); 
        Element tmpElement = null;	
        int length = childNodes.getLength();   
        for (int i=0; i < length; i++) {
            if (childNodes.item(i).getNodeType() == Node.ELEMENT_NODE) {
                tmpElement = (Element) (childNodes.item(i));

                if (SQLPart.TAG_SQLPART.equals(tmpElement.getNodeName())) {
                	childSQLParts.add(tmpElement);
                }  
            }
        }        
        parseSQLStatements(childSQLParts, statementMap);

        list = element.getElementsByTagName(TAG_INIT_PROCESS);
        parseInitProcessTasks(list);

        list = element.getElementsByTagName(TAG_PER_TABLE);
        parsePerTableStatements(list);

        list = element.getChildNodes();
        try {
            attributes.parseAttributeList(list);
        } catch (Exception e) {
            Logger.print(Logger.DEBUG, getClass().getName(), "Failed to parseAttributeList in TaskNodeImpl", e);
            throw new BaseException("Failed to parseAttributeList in TaskNodeImpl", e);

        }
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#removeNextETLTaskNode(String, String)
     */
    public int removeNextETLTaskNode(String situation, String nextTaskID) {
        if (situation == null || ETLEngine.START.equals(nextTaskID)) {
            return -1;
        }

        String idList = (String) situationToNextTaskIdMap.get(situation);
        if (idList == null || idList.length() == 0) {
            return 0;
        }

        // Use Set to eliminate duplicates.
        Set nodeIds = new HashSet(StringUtil.createStringListFrom(idList));
        if (nodeIds.contains(nextTaskID)) {
            nodeIds.remove(nextTaskID);
            idList = StringUtil.createDelimitedStringFrom(new ArrayList(nodeIds));
            situationToNextTaskIdMap.put(situation, idList);
        }

        return 0;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#removeStatement(String)
     */
    public boolean removeStatement(String stmtType) {
        if (StringUtil.isNullString(stmtType)) {
            throw new IllegalArgumentException("Must supply non-empty String value for parameter 'typeName.'");
        }

        return (statementMap.remove(stmtType) != null);
    }

    public void setAllStatement(Map stmtMap) {
        this.statementMap = stmtMap;
    }

    public void setAttributeMap(AttributeMap attrMap) {
        this.attributes = attrMap;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#setDependsOn(String)
     */
    public void setDependsOn(String adepends) {
        this.dependsOn = adepends;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#setDisplayName(String)
     */
    public void setDisplayName(String name) {
        displayName = name;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#setId(String)
     */
    public void setId(String aId) {
        this.id = aId;
    }

    /**
     * com.sun.etl.engine.ETLTaskNode.setOptionalTasks
     */
    public void setOptionalTasks(List theSqlParts) {
        sqlParts = theSqlParts;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#setParent(ETLEngine)
     */
    public void setParent(ETLEngine parent) {
        this.parent = parent;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#setStatus(String)
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * com.sun.etl.engine.ETLTaskNode.setTableName
     */
    public void setTableName(String name) {
        this.tableName = name;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#setTaskType(String)
     */
    public void setTaskType(String ataskType) {
        this.taskType = ataskType;
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#toXMLString()
     */
    public String toXMLString() {
        return toXMLString("");
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#toXMLString(String)
     */
    public String toXMLString(String prefix) {
        StringBuffer buf = new StringBuffer(250);

        if (prefix == null) {
            prefix = "";
        }

        buf.append(prefix).append("<" + TAG_TASK + " ");

        buf.append(ATTR_ID + "=\"").append(id).append("\" ");
        buf.append(ATTR_TASKTYPE + "=\"").append(taskType).append("\" ");

        buf.append(ATTR_DEPENDSON + "=\"");
        if (dependsOn != null) {
            buf.append(dependsOn.trim());
        }
        buf.append("\"");

        if (!StringUtil.isNullString(displayName)) {
            buf.append(" ").append(ATTR_DN).append("=\"").append(displayName.trim()).append("\"");
        }

        if (!StringUtil.isNullString(this.tableName)) {
            buf.append(" ").append(ATTR_TN).append("=\"").append(tableName.trim()).append("\"");
        }
        buf.append(">\n");

        //write out attributes
        buf.append(attributes.toXMLString(prefix));

        buf.append(writeNextTaskElements(prefix + "\t"));
        buf.append(writeSQLStatementsForMap(statementMap, prefix + "\t"));
        buf.append(writeSQLStatementsForList(prefix + "\t"));
        buf.append(writePerTableSQLStatements(prefix + "\t"));

        buf.append(prefix).append("</" + TAG_TASK + ">\n");

        return buf.toString();
    }

    /**
     * @see com.sun.etl.engine.ETLTaskNode#validate
     */
    public boolean validate() throws ETLException {
        return true;
    }

    /**
     * @param tableName
     * @return
     */
    private Map getOrCreateTableStatementMap(String aTableName) {
        Map tableStmtMap = (Map) tableToStatementsMap.get(aTableName);

        if (tableStmtMap == null) {
            tableStmtMap = new HashMap();
            tableToStatementsMap.put(aTableName, tableStmtMap);
        }

        return tableStmtMap;
    }

    private void parseInitProcessTasks(NodeList list) throws BaseException {
        if (list != null && list.getLength() != 0) {
            Node aNode = list.item(0);
            if (aNode.getNodeType() == Node.ELEMENT_NODE) {
                NodeList sqlPartsList = aNode.getChildNodes();
                parseSQL(sqlPartsList);
            }
        }
    }

    private void parseNextTaskElements(NodeList list) throws BaseException {
        if (list.getLength() == 0) {
            throw new BaseException("No next task elements found!");
        }

        for (int i = 0; i < list.getLength(); i++) {
            Node aNode = list.item(i);
            if (aNode.getNodeType() == Node.ELEMENT_NODE) {
                Element elem = (Element) aNode;
                if (TAG_NEXT.equals(aNode.getNodeName())) {
                    NodeList situations = elem.getElementsByTagName(TAG_SITUATION);
                    parseSituationElements(situations);
                    break;
                }
            }
        }
    }

    /**
     * @param list
     */
    private void parsePerTableStatements(NodeList list) throws BaseException {
        if (list != null && list.getLength() != 0) {
            for (int i = 0; i < list.getLength(); i++) {
                Node aNode = list.item(i);
                if (aNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element perTableElement = (Element) aNode;
                    String aTableName = perTableElement.getAttribute(ATTR_TN);
                    NodeList sqlPartsList = aNode.getChildNodes();
                    parseSQLStatements(sqlPartsList, getOrCreateTableStatementMap(aTableName));
                }
            }
        }
    }

    private void parseSituationElements(NodeList list) {
        for (int i = 0; i < list.getLength(); i++) {
            Node aNode = list.item(i);
            if (aNode.getNodeType() == Node.ELEMENT_NODE) {
                Element elem = (Element) aNode;
                String situationType = elem.getAttribute(ATTR_NAME);
                if (ETLTask.SUCCESS.equals(situationType) || ETLTask.EXCEPTION.equals(situationType)) {
                    situationToNextTaskIdMap.put(situationType, elem.getAttribute(ATTR_TASKREFIDS));
                }
            }
        }
    }

    private void parseSQL(NodeList list) throws BaseException {
        for (int i = 0; i < list.getLength(); i++) {
            Node aNode = list.item(i);
            if (aNode.getNodeType() == Node.ELEMENT_NODE) {
                SQLPart stmt = new SQLPart((Element) aNode);
                this.sqlParts.add(stmt);
            }
        }
    }

    private void parseSQLStatements(NodeList list, Map stmtMap) throws BaseException {
        for (int i = 0; i < list.getLength(); i++) {
            Node aNode = list.item(i);
            if (aNode.getNodeType() == Node.ELEMENT_NODE) {
                SQLPart stmt = new SQLPart((Element) aNode);
                stmtMap.put(stmt.getType(), stmt);
            }
        }
    }

    private void parseSQLStatements(List list, Map stmtMap) throws BaseException {
        for (int i = 0; i < list.size(); i++) {
            Node aNode = (Node) list.get(i);
            if (aNode.getNodeType() == Node.ELEMENT_NODE) {
                SQLPart stmt = new SQLPart((Element) aNode);
                stmtMap.put(stmt.getType(), stmt);
            }
        }
    }
    
    /*
     * Writes out associations between situation IDs and lists of task node IDs as nested
     * XML elements, prepending the given String to the beginning of each line of the
     * output. @param prefix String to prepend to each new line @return XML representation
     * of situation-to-task ID associations
     */
    private String writeNextTaskElements(String prefix) {
        StringBuffer buf = new StringBuffer(100);
        if (prefix == null) {
            prefix = "";
        }

        buf.append(prefix).append("<" + TAG_NEXT);
        if (situationToNextTaskIdMap.size() == 0) {
            buf.append(" />\n");
        } else {
            buf.append(">\n");
            Iterator iter = situationToNextTaskIdMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String situation = (String) entry.getKey();
                String idList = (String) entry.getValue();

                buf.append(prefix + "\t").append("<" + TAG_SITUATION + " ");
                buf.append(ATTR_NAME + "=\"" + situation.trim() + "\" ");
                buf.append(ATTR_TASKREFIDS + "=\"" + idList.trim() + "\" />\n");
            }
            buf.append(prefix).append("</" + TAG_NEXT + ">\n");
        }

        return buf.toString();
    }

    /**
     * @param string
     * @return
     */
    private String writePerTableSQLStatements(String prefix) {
        StringBuffer buf = new StringBuffer(100);
        if (prefix == null) {
            prefix = "";
        }

        if (tableToStatementsMap.size() != 0) {
            Iterator iter = tableToStatementsMap.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String aTableName = (String) entry.getKey();
                Map stmtMap = (Map) entry.getValue();

                buf.append(prefix).append("<").append(TAG_PER_TABLE).append(" ").append(ATTR_TN).append("=\"").append(aTableName).append("\"").append(
                    ">").append("\n");
                buf.append(writeSQLStatementsForMap(stmtMap, prefix + "\t"));
                buf.append(prefix).append("</").append(TAG_PER_TABLE).append(">").append("\n");
            }
        }

        return buf.toString();
    }

    private String writeSQLStatements(String prefix, Iterator iter) {
        StringBuffer buf = new StringBuffer(100);
        if (prefix == null) {
            prefix = "";
        }
        while (iter.hasNext()) {
            SQLPart stmt = (SQLPart) iter.next();
            buf.append(stmt.toXMLString(prefix));
        }
        return buf.toString();
    }

    private String writeSQLStatementsForList(String prefix) {
        StringBuffer buf = new StringBuffer(100);
        if (prefix == null) {
            prefix = "";
        }
        if (this.sqlParts.size() != 0) {
            buf.append(prefix).append("<").append(TAG_INIT_PROCESS).append(">").append("\n");
            Iterator iter = this.sqlParts.iterator();
            buf.append(writeSQLStatements(prefix + "\t", iter));
            buf.append(prefix).append("</").append(TAG_INIT_PROCESS).append(">\n");
        }
        return buf.toString();
    }

    /*
     * Writes out associated SQL statements as XL elements, prepending the given String to
     * the beginning of each line of the output. @param prefix String to prepend to each
     * new line @return XML representation of associated SQL statements
     */
    private String writeSQLStatementsForMap(Map statements, String prefix) {
        Iterator iter = statements.values().iterator();
        return writeSQLStatements(prefix, iter);
    }
}
