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
 * @(#)ETLTaskNode.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import com.sun.etl.engine.utils.ETLException;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.SQLPart;
import com.sun.sql.framework.utils.AttributeMap;

/**
 * ETLTaskNode represent one execution node of ETLEngine, used to build a
 * execution/process flow various Tasks for ETL Process.
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public interface ETLTaskNode {

    /**
     * Tag name for XML elements that represent implementations of this interface.
     */
    public static final String TAG_TASK = "task"; //NOI18N

    /**
     * Adds given task node ID to the list of next task nodes associated with the given
     * situation.
     * 
     * @param situation situation ID with which to associate nextTaskName
     * @param nextTaskID ID of task node.
     * @return 0 if successful, -1 if failed.
     */
    public int addNextETLTaskNode(String situation, String nextTaskID);

    /**
     * Adds an optional task
     * 
     * @param sqlPart
     */
    public void addOptionalTask(SQLPart sqlPart);

    /**
     * Adds the given SQL statement (bound with its embedded statement type) to this task
     * node.
     * 
     * @param newSql new SQL statement to associate with this task node, using its
     *        embedded statement type as a key
     */
    public void addStatement(SQLPart newSql);

    /**
     * Adds the given SQL statement (bound with its embedded statement type) to the task
     * name, associated specifically with the given table name.
     * 
     * @param tableName table name with which <code>newSql</code> will be associated
     * @param newSql SQLStatement to associate with this task node and the given table
     *        name.
     */
    public void addTableSpecificStatement(String tableName, SQLPart newSql);

    /**
     * Fire ETLEngineExecutionEvent 
     * @param evnt
     */
    public void fireETLEngineExecutionEvent(ETLEngineExecEvent evnt);

    
    /**
     * Fire ETLEngineLogEvent to log the message
     * 
     * @param logMessage
     */
    public void fireETLEngineLogEvent(String logMessage);

    /**
     * Fire ETLEngineLogEvent to log the given message at the given level of severity.
     * 
     * @param logMessage message to be logged
     * @param level log level
     */
    public void fireETLEngineLogEvent(String logMessage, int level);

    /**
     * Gets IDs of all "nextOn" tasks for this task node.
     * 
     * @return List of Strings representing IDs of nextOn tasks.
     */
    public List getAllNextETLTaskNodes();

    /**
     * Gets List of all known situation IDs in this task node
     * 
     * @return List of Strings representing situation IDs in use
     */
    public List getAllSituations();

    /**
     * Added for Flatfile DB init purpose
     * 
     * @return Map
     */
    public Map getAllStatements();

    /**
     * Gets attribute Map
     * 
     * @return AttributeMap
     */
    public AttributeMap getAttributeMap();

    /**
     * Gets associated context object for this task node
     * 
     * @return associated ETLEngineContext object
     */
    public ETLEngineContext getContext();

    /**
     * Gets comma-delimited String list of task IDs to dependencies on task nodes.
     * 
     * @return String containing IDs of dependent task nodes
     */
    public String getDependsOn();

    /**
     * Gets display name of the task.
     * 
     * @return name of associated display name.
     */
    public String getDisplayName();

    /**
     * Gets ID of this task node.
     * 
     * @return task node ID
     */
    public String getId();

    /**
     * Gets comma-separated list of task IDs associated with the given situation ID.
     * 
     * @param situation situation ID to use in looking up task ID list
     * @return associated comma-separated task ID list, or null if situation is not
     *         associated with any task list
     */
    public String getNextTaskList(String situation);

    /**
     * com.sun.etl.engine.ETLTaskNode.getOptionalTasks
     */
    public List getOptionalTasks();

    /**
     * Gets parent of this task node.
     * 
     * @return parent ETLEngine
     */
    public ETLEngine getParent();

    /**
     * Gets SQLPart instance, if any, associated with the given statement type.
     * 
     * @param stmtType statement type to search against
     * @return SQLPart representing SQL statement associated with stmtType, or null if no
     *         such instance exists for stmtType
     */
    public SQLPart getStatement(String stmtType);

    /**
     * Gets List of all statement type Strings currently associated with statements in
     * this task node.
     * 
     * @return List of Strings representing statement types.
     */
    public List getStatementTypes();

    /**
     * Gets current status of this task node.
     * 
     * @return current status
     */
    public String getStatus();

    /**
     * Gets Table name of the task.
     * 
     * @return name of associated display name.
     */
    public String getTableName();

    /**
     * Gets collection of tables that have table-specific SQLPart statements associated
     * with them.
     * 
     * @return Collection of Strings representing table names which have table-specific
     *         SQLParts associated with them
     */
    public Collection getTableNamesWithSpecificStatements();

    /**
     * Gets SQLPart instance, if any, associated with the given statement type and table
     * name.
     * 
     * @param tableName name of table whose statements are to be searched against
     * @param stmtType statement type to search against
     * @return SQLPart representing SQL statement associated with stmtType, or null if no
     *         such instance exists for stmtType
     */
    public SQLPart getTableSpecificStatement(String tableName, String stmtType);

    /**
     * Gets snapshot of currently defined SQLPart statements which are associated with the
     * given table name.
     * 
     * @param tableName table against which to search for associated SQLPart statements
     * @return Map of statement types (String values) to SQLParts
     */
    public Map getTableSpecificStatements(String tableName);

    /**
     * Gets task node type.
     * 
     * @return task node type
     */
    public String getTaskType();

    /**
     * Indicates whether this node is a start node.
     * 
     * @return true if this is a start node; false otherwise
     */
    public boolean isStartNode();

    /**
     * Parses given XML element for the contents of this task node
     * 
     * @param elem Element to be parsed
     * @exception com.sun.sql.framework.exception.BaseException thrown while parsing
     */
    public void parseXML(Element elem) throws BaseException;

    /**
     * Removes given task node ID, if any, from the list of next task nodes associated
     * with the given situation.
     * 
     * @param situation situation ID with which to dissociate nextTaskName
     * @param nextTaskID ID of task node.
     * @return 0 if successful, -1 if failed.
     */
    public int removeNextETLTaskNode(String situation, String nextTaskID);

    /**
     * Removes the SQL statement, if any, associated with the given statement type.
     * 
     * @param stmtType statement type whose associated SQL statement is to be removed
     * @return true if an associated statement was removed, false if no such statement
     *         exists for stmtType
     */
    public boolean removeStatement(String stmtType);

    /**
     * Sets All Statement
     * 
     * @param stmtMap
     */
    public void setAllStatement(Map stmtMap);

    /**
     * Sets Attibute map
     * 
     * @param attrMap
     */
    public void setAttributeMap(AttributeMap attrMap);

    /**
     * Sets list of task IDs to dependencies on task nodes to the contents of the given
     * comma-delimited String.
     * 
     * @param newDependsList new list of task node IDs
     */
    public void setDependsOn(String newDependsList);

    /**
     * Sets display name of the task.
     * 
     * @param name of the task
     */
    public void setDisplayName(String name);

    /**
     * Sets ID of this task node.
     * 
     * @param newId new ID for this task
     */
    public void setId(String newId);

    /**
     * com.sun.etl.engine.ETLTaskNode.setOptionalTasks
     */
    public void setOptionalTasks(List sqlParts);

    /**
     * Sets parent ETLEngine of this task node.
     * 
     * @param newParent Reference to new parent ETLEngine
     */
    public void setParent(ETLEngine newParent);

    /**
     * Sets status of this task node.
     * 
     * @param status The new status value
     */
    public void setStatus(String status);

    /**
     * Sets TableName name of the task.
     * 
     * @param name of the task
     */
    public void setTableName(String name);

    /**
     * Sets task node type to the given String.
     * 
     * @param newTaskType to be set
     */
    public void setTaskType(String newTaskType);

    /**
     * Writes out an XML representation of this task node.
     * 
     * @return XML representation of the task node.
     */
    public String toXMLString();

    /**
     * Writes out an XML representation of this task node, prepending the given String to
     * each new line.
     * 
     * @param prefix String to prepend to each new line
     * @return XML representation of the task node.
     */
    public String toXMLString(String prefix);

    /**
     * Validates the contents of this task node.
     * 
     * @return true if successful. false if failed.
     * @exception com.sun.sql.framework.exception.BaseException General exception
     */
    public boolean validate() throws ETLException;

}
