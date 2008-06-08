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
 * @(#)CorrelatedQueryExecutor.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLEngineExecEvent;
import com.sun.etl.engine.ETLTask;
import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.DBConnectionFactory;
import com.sun.sql.framework.jdbc.SQLPart;
import com.sun.sql.framework.jdbc.SQLUtils;
import com.sun.sql.framework.utils.AttributeMap;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;

/**
 * Executes correlated Queries. Example: SELECT and UPDATE - Update target table
 * rows using values from previous query.
 *
 * @author Girish Patil
 * @version 
 */
public class CorrelatedQueryExecutor extends SimpleTask {
    private static final String LOG_CATEGORY = CorrelatedQueryExecutor.class.getName();
    private static final MessageManager MSG_MGR = MessageManager.getManager("com.sun.etl.engine.impl");
    private static final int DEFAULT_BATCH_SIZE = 5000;

    private ETLTaskNode taskNode;
    private int batchSize = DEFAULT_BATCH_SIZE;
    // For now, this task will executes on one single TGT database.
    // Seperate SRC and TGT connection gives future flexibilty of SRC and TGT
    // databases not being the same.
    private DBConnectionFactory factory = DBConnectionFactory.getInstance();
    private String srcConnName = null;
    private String tgtConnName = null;
    private Connection srcConn = null;
    private Connection tgtConn = null;

    public CorrelatedQueryExecutor() {
    }

    public void cleanUp() {
        try {
            factory.closeConnection(srcConn);

            if (tgtConn != null) {
                taskNode.getContext().closeAndReleaseLater(tgtConn);
            }
            taskNode = null;
        } catch (Exception ex) {
            Logger.printThrowable(Logger.WARN, LOG_CATEGORY, this.getClass().getName(),
                                     "Exception while task cleanup:", ex);
        } finally {
            super.cleanUp();
        }
    }

    private void handleInternalException(ETLTaskNode node, Throwable ex) throws ThreadDeath, ETLException {
        String msg;
        ex = unwrapThrowable(ex);

        msg = ex.getMessage();
        if (StringUtil.isNullString(msg)) {
            msg = ex.toString();
        }
        String failureMsg = MSG_MGR.getString("MSG_CQE_insert_or_update_failure", msg);

        try {
            if (tgtConn != null) {
                if (!tgtConn.getAutoCommit()) {
                    Logger.print(Logger.DEBUG, LOG_CATEGORY, "Rolling back transactions");
                    tgtConn.rollback();
                }
            }
        } catch (Exception ignore) {
            // Ignore if any
        }

        // Set insert count to zero, as failure has occurred.
        updateRowsProcessedCount(node, 0);

        throw new ETLException(failureMsg, ex);
    }

    public void handleException(ETLException ex) {
        this.cleanUp();
        Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, "Co-related Query Executor:",
                              "Handling exception for CorrelatedQueryExecutor.", ex);

        if (tgtConn != null && tgtConnName != null) {
            factory.closeConnection(tgtConn);
            tgtConn = null;
        }
    }

    public String process(ETLTaskNode node) throws ETLException {
        final Timestamp startTime;
        final int executionId;
        ETLEngineContext.CollabStatistics stats = null;
        ETLEngineExecEvent evnt;
        List connList;

        AttributeMap taskAttrMap;
        String batchSizeStr;
        String msg;

        SQLPart updateSQLPart;
        SQLPart insSelectSQLPart;
        String updateStmnt = null;
        String insSelect = null;;
        List bindingVarJdbcTypes;
        List bindVariableSource;
        ResultSet selectRS = null;
        int updateCount = 0;
        int insertCount = 0;
        try {
            if (node != null) {
                this.taskNode = node;
            } else {
                throw new ETLException("Task node is null.");
            }

            taskAttrMap = this.taskNode.getAttributeMap();
            batchSizeStr = (String) taskAttrMap.getAttributeValue("batchSize"); // NOI18N
            stats = node.getParent().getContext().getStatistics();
            startTime = new java.sql.Timestamp(System.currentTimeMillis());
            //commented out till Monitoring system quality check-in
            super.createExecutionEntryInSummaryTable(node);
            stats.setTableStartTime(node.getTableName(), startTime);
            executionId = -1;
            super.getExecutionEntryIdFromSummaryTable(node, startTime);
            //commented out till Monitoring system quality check-in
            stats.setTableExecutionId(node.getTableName(), executionId);
            evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_STARTED, node.getTableName(),
                                            "" + executionId);
            taskNode.fireETLEngineExecutionEvent(evnt);

            if (!StringUtil.isNullString(node.getDisplayName())) {
                DN += " <" + node.getDisplayName().trim() + ">";
            }

            msg = MSG_MGR.getString("MSG_CQE_started");
            node.fireETLEngineLogEvent(msg, Logger.DEBUG);

            if (!StringUtil.isNullString(batchSizeStr)) {
                int bsInt = StringUtil.getInt(batchSizeStr);
                if (bsInt > 0) {
                    this.batchSize = bsInt;
                }
            }

            connList = node.getParent().getConnectionDefList();
            // Get correlated update SQLPart...
            updateSQLPart = node.getStatement(SQLPart.STMT_CORRELATED_UPDATE);
            if (updateSQLPart == null) {
                throw new ETLException(DN + "Missing required insert SQLPart element");
            }

            updateStmnt = updateSQLPart.getSQL();
            if (updateStmnt == null) {
                throw new ETLException(DN + "Missing required insert statement");
            }

            tgtConnName = updateSQLPart.getConnectionPoolName();
            tgtConn = getConnection(tgtConnName, connList);
            Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + " Got staging connection: " + tgtConn);

            // drop and create raw table if specified
            setAutoCommitIfRequired(tgtConn, true);
            dropTable(node, tgtConn);
            createTable(node, tgtConn);
            setAutoCommitIfRequired(tgtConn, false);

            // create target table if non-existent, or truncate, if specified
            if (!createBeforeProcess(node, tgtConn)) {
                truncateBeforeProcess(node, tgtConn);
            }

            // extract data from source db
            selectRS = selectData(node);

            // insert the data based on the result set received above
            bindingVarJdbcTypes = (List) updateSQLPart.getAttribute(SQLPart.ATTR_JDBC_TYPE_LIST).getAttributeValue();
            bindVariableSource = (List) updateSQLPart.getAttribute(SQLPart.ATTR_DESTS_SRC).getAttributeValue();

            // Execute UPDATE
            updateCount = updateData(selectRS, updateStmnt, tgtConn, bindingVarJdbcTypes, bindVariableSource, node);

            insSelectSQLPart = node.getStatement(SQLPart.STMT_INSERTSELECT);
            if (insSelectSQLPart != null){
                insSelect = insSelectSQLPart.getSQL();
            }

            // Execute INSERT-SELECT
            if ((tgtConn != null) && (!tgtConn.isClosed()) && (insSelect != null)){
                insertCount = insertdata(node, insSelect, tgtConn, executionId);
            }

            // Commit later and then close the connection
            node.getParent().getContext().commitLater(tgtConn);

            msg = MSG_MGR.getString("MSG_CQE_finished");
            node.fireETLEngineLogEvent(msg, Logger.DEBUG);

            // Use task node table name as key to represent total number of rows
            // processed.
            stats.setRowsExtractedCount(node.getTableName(), updateCount + insertCount);
            updateRowsProcessedCount(node, updateCount + insertCount);
            evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_COMPLETED, node.getTableName(),
                                    "" + stats.getTableExecutionId(node.getTableName()));
            taskNode.fireETLEngineExecutionEvent(evnt);
        } catch (Exception ex) {
            evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_EXCEPTION, node.getTableName(), "" + stats.getTableExecutionId(node.getTableName()));
            evnt.setCause(ex);
            node.fireETLEngineExecutionEvent(evnt);
            handleInternalException(node, ex);
        } finally {
            Statement rsStmt = null;
            if (selectRS != null) {
                try {
                    rsStmt = selectRS.getStatement();
                } catch (SQLException e) {
                    // ignore
                }
                closeResultSet(selectRS);
                closeStatement(rsStmt);
            }
        }
        return ETLTask.SUCCESS;
    }

    /**
     * Create Table when appropriate, mostly used while extracting source data
     * to temp table, but not used if we are directly extracting to target
     * table. Temp table is required only when data comes from diversified data
     * sources.
     *
     * @param node ETLTaskNode containing create statement
     * @param con  Connection to use in executing statement
     * @throws Exception if error occurs during execution
     */
    private void createTable(ETLTaskNode node, Connection con) throws Exception {
        Statement stmt = null;
        String createTableRelatedSql = "";

        try {
            SQLPart createSQLPart = node.getStatement(SQLPart.STMT_CREATE); // NO18N

            if (createSQLPart == null) {
                return; // user did not choose to create if does not exist
            }

            Iterator itr = createSQLPart.getIterator();
            while (itr.hasNext()) {
                stmt = con.createStatement();
                createTableRelatedSql = (String) itr.next();

                String createStartMsg = MSG_MGR.getString("MSG_extractor_create_attempt");
                taskNode.fireETLEngineLogEvent(createStartMsg, Logger.DEBUG);

                String showSqlMsg = MSG_MGR.getString("MSG_common_using_sql", createTableRelatedSql);
                taskNode.fireETLEngineLogEvent(showSqlMsg, Logger.DEBUG);

                stmt.executeUpdate(createTableRelatedSql);
                String successMsg = MSG_MGR.getString("MSG_extractor_create_success");
                taskNode.fireETLEngineLogEvent(successMsg, Logger.DEBUG);
            }
        } catch (SQLException e) {
            if (isObjectAlreadyExistsException(e)) {
                String tableExistsMsg = MSG_MGR.getString("MSG_extractor_create_failure_table_exists");
                taskNode.fireETLEngineLogEvent(tableExistsMsg, Logger.WARN);
                return;
            }

            super.logSQLException(Logger.ERROR, LOG_CATEGORY, e);
            throw e;
        } catch (Exception e) {
            String failureMsg = MSG_MGR.getString("MSG_extractor_create_failure");
            taskNode.fireETLEngineLogEvent(failureMsg, Logger.DEBUG);
            Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, this, DN + failureMsg, e);
            throw e;
        } finally {
            closeStatement(stmt);
        }
    }

    /**
     * Executes a drop command associated with the given ETLTaskNode, using the
     * given Connection.
     *
     * @param node  ETLTaskNode containing the drop command to execute
     * @param con  Connection to use in executing the drop command
     * @throws SQLException if non-recoverable error occurs during execution
     * @throws Exception  if error occurs during execution
     */
    private void dropTable(ETLTaskNode node, Connection con) throws BaseException, SQLException {
        Statement stmt = null;
        try {
            SQLPart checkTablePart = node.getStatement(SQLPart.STMT_CHECKTABLEEXISTS);
            SQLPart dropSQLPart = node.getStatement(SQLPart.STMT_DROP);

            if (dropSQLPart == null) {
                return; // user choose not to drop raw/target table
            }

            String dropMsg = MSG_MGR.getString("MSG_extractor_drop_attempt");
            node.fireETLEngineLogEvent(dropMsg, Logger.DEBUG);

            String dropSQL = dropSQLPart.getSQL();
            String checkSQL = checkTablePart.getSQL();

            String checkStmtMsg = MSG_MGR.getString("MSG_common_using_sql", checkSQL);
            String dropStmtMsg = MSG_MGR.getString("MSG_common_using_sql", dropSQL);

            Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + checkStmtMsg);
            node.fireETLEngineLogEvent(dropStmtMsg, Logger.DEBUG);

            stmt = con.createStatement();

            if (isTableExists(con, checkSQL,node)) {
                stmt.executeUpdate(dropSQL);
                if (!con.getAutoCommit()) {
                    con.commit();
                }
            }

            String droppedMsg = MSG_MGR.getString("MSG_common_table_dropped");
            node.fireETLEngineLogEvent(droppedMsg, Logger.DEBUG);
        } catch (SQLException e) {
            if (isObjectDoesNotExistException(e)) {
                String tableExistsMsg = MSG_MGR.getString("MSG_extractor_drop_failure_table_nonexistent");
                taskNode.fireETLEngineLogEvent(tableExistsMsg, Logger.WARN);
                return;
            }

            super.logSQLException(Logger.ERROR, LOG_CATEGORY, e);
            throw e;
        } catch (Exception t) {
            Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, "Unexpected error while dropping table.", t);
            throw new BaseException(t);
        } finally {
            closeStatement(stmt);
        }
    }

    /**
     * Executes a query associated with the given ETLTaskNode, returning a
     * ResultSet representing rows returned in response.
     *
     * @param node ETLTaskNode containing the query to execute
     * @return ResultSet (possibly empty) containing results of the query
     * @throws BaseException if error occurs during execution
     */
    private ResultSet selectData(ETLTaskNode node) throws BaseException {
        SQLPart selectSQLPart = null;
        String selectStmt = null;
        PreparedStatement stmt = null;
        ResultSet rs;

        try {
            selectSQLPart = node.getStatement(SQLPart.STMT_CORRELATED_SELECT);
            if (selectSQLPart == null) {
                throw new ETLException(DN + "Missing required select SQLPart element");
            }

            selectStmt = selectSQLPart.getSQL();
            if (selectStmt == null) {
                throw new ETLException(DN + "Missing required select statement");
            }

            // get Source db connection
            srcConnName = selectSQLPart.getConnectionPoolName();
            srcConn = getConnection(srcConnName, node.getParent().getConnectionDefList());
            Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + " got source connection: " + srcConn);

            String startMsg = MSG_MGR.getString("MSG_CQE_select_for_update_attempt");
            node.fireETLEngineLogEvent(startMsg, Logger.DEBUG);

            String showSqlMsg = MSG_MGR.getString("MSG_common_using_sql", selectStmt);
            node.fireETLEngineLogEvent(showSqlMsg, Logger.DEBUG);

            // execute select and get result set
            List paramList = new ArrayList();
            Map attribMap = taskNode.getParent().getInputAttrMap();

            String ps = SQLUtils.createPreparedStatement(selectStmt, attribMap, paramList);
            stmt = srcConn.prepareStatement(ps);
            stmt.setFetchSize(this.batchSize);

            SQLUtils.populatePreparedStatement(stmt, attribMap, paramList);
            rs = stmt.executeQuery();
            if (rs == null) {
                throw new ETLException(DN + "Failed to get the ResultSet in " + LOG_CATEGORY + "; SQL execution failed");
            }

            String successMsg = MSG_MGR.getString("MSG_CQE_got_select_for_update_rs");
            node.fireETLEngineLogEvent(successMsg, Logger.DEBUG);
        } catch (Exception e) {
            closeStatement(stmt);
            throw new BaseException("Failed to get the ResultSet for " + selectStmt, e);
        }

        return rs;
    }

    private int updateData(ResultSet rs, String insertStmt, Connection dbCon, List types, List destsSource, ETLTaskNode node) throws Exception {
        PreparedStatement prepStmt = null;
        Map attribMap = node.getParent().getInputAttrMap();
        Map attribValues = SQLUtils.getRuntimeInputNameValueMap(attribMap);

        int localBatchSize = 0;
        int updateCount = 0;
        int numOfBindingVars = 0;
        int srcNum = 0;
        int colType = 0;
        String src = null;

        Object value;

        try {
            ResultSetMetaData rsMeta = rs.getMetaData();

            if (rsMeta == null) {
                taskNode.fireETLEngineLogEvent( "Unable to fetch source table metadata", Logger.DEBUG);
                throw new BaseException("Unable to fetch source table metadata");
            }

            String insertStartMsg = MSG_MGR.getString("MSG_CQE_update_attempt");
            taskNode.fireETLEngineLogEvent(insertStartMsg, Logger.DEBUG);

            String prepStmtMsg = MSG_MGR.getString("MSG_extractor_show_prep_stmt", insertStmt);
            taskNode.fireETLEngineLogEvent(prepStmtMsg, Logger.DEBUG);

            prepStmt = dbCon.prepareStatement(insertStmt);
            if (prepStmt == null) {
                String errMsg = MSG_MGR.getString("MSG_CQE_invalid_update");
                taskNode.fireETLEngineLogEvent(errMsg, Logger.DEBUG);
                throw new BaseException("Invalid UPDATE statement.");
            }

            numOfBindingVars = types.size();

            while (rs.next()) {
                for (int bv = 1; bv <= numOfBindingVars; bv++) {
                    src = (String) destsSource.get(bv -1);
                    if (src.startsWith("$")){
                        srcNum = -1 ;
                        colType = StringUtil.getInt((String)types.get(bv -1));
                        value = attribValues.get(src.substring(1));
                        SQLUtils.setAttributeValue(prepStmt, bv, colType, value);
                    } else {
                        srcNum = StringUtil.getInt(src);
                        colType = rsMeta.getColumnType(srcNum);
                        switch (colType) {
                        // Must treat timestamp specially, as Oracle's
                        // implementationof ResultSet.getObject() does not return an impl instance
                        // of java.sql.Timestamp, but a concrete Oracle implementation.
                        case Types.TIMESTAMP:
                            value = rs.getTimestamp(srcNum);
                            break;

                        default:
                            value = rs.getObject(srcNum);
                            break;
                        }

                        if (!rs.wasNull() && value != null) {
                            prepStmt.setObject(bv, value);
                        } else {
                            if (rsMeta.isNullable(srcNum) == ResultSetMetaData.columnNullable) {
                                prepStmt.setNull(bv, StringUtil.getInt((String) types.get(bv - 1)));
                            } else {
                                throw new BaseException("Column " + rsMeta.getColumnType(srcNum)
                                                            + " is not nullable.");
                            }
                        }

                    }
                }

                if (batchSize > 1) {
                    prepStmt.addBatch();
                    localBatchSize++;
                    if (localBatchSize == batchSize) {
                        String engineState = (String) taskNode.getContext().getValue("engineState"); // NO18N
                        if (engineState != null && engineState.trim().equalsIgnoreCase("not-active")) { // NOI18N
                            prepStmt.close();
                            return updateCount; // -- engine no longer active
                        }

                        int row[] = prepStmt.executeBatch();
                        prepStmt.clearBatch();

                        String rowInsertMsg = MSG_MGR.getString("MSG_CQE_batch_update_cnt", new Integer(row.length));
                        taskNode.fireETLEngineLogEvent(rowInsertMsg);
                        updateCount += localBatchSize;
                        localBatchSize = 0;
                    }
                } else {
                    prepStmt.executeUpdate();
                    updateCount++;
                }
            }

            if (batchSize > 1) {
                int row[] = prepStmt.executeBatch();
                prepStmt.clearBatch();

                String rowInsertMsg = MSG_MGR.getString("MSG_CQE_batch_update_cnt", new Integer(row.length));
                taskNode.fireETLEngineLogEvent(rowInsertMsg);
                updateCount += row.length;
            }

            String successMsg = MSG_MGR.getString("MSG_CQE_update_success", new Integer(updateCount));
            taskNode.fireETLEngineLogEvent(successMsg, Logger.INFO);
            return updateCount;
        } catch (Exception se) {
            String errMsg = MSG_MGR.getString("MSG_common_sql_failed_show", se.getMessage());
            Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, null, errMsg, se);
            taskNode.fireETLEngineLogEvent(errMsg, Logger.ERROR);
            throw se;
        } finally {
            closeResultSet(rs);
            closeStatement(prepStmt);
        }
    }


    private int insertdata(ETLTaskNode node, String strInsSel, Connection con, int execId) throws Exception  {
        PreparedStatement stmt = null;
        List paramList = new ArrayList();
        int insertCt = 0;
        try {
            Map attribMap = node.getParent().getInputAttrMap();
            String ps = SQLUtils.createPreparedStatement(strInsSel, attribMap, paramList);
            stmt = con.prepareStatement(ps);
            SQLUtils.populatePreparedStatement(stmt, attribMap, paramList);
            int rows = stmt.executeUpdate();
            insertCt += (rows > 0) ? rows : 0;
            if (rows == Statement.EXECUTE_FAILED) {
                String errMsg = MSG_MGR.getString("MSG_common_sql_failed_show", strInsSel);
                node.fireETLEngineLogEvent(errMsg, Logger.ERROR);
            }
            stmt.close();
            String cntMsg = MSG_MGR.getString("MSG_CQE_insert_success", new Integer(insertCt));
            taskNode.fireETLEngineLogEvent(cntMsg, Logger.INFO);
            return insertCt;
        } catch (Exception ex) {
            String errMsg = MSG_MGR.getString("MSG_common_sql_failed_show", ex.getMessage());
            Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, null, errMsg, ex);
            taskNode.fireETLEngineLogEvent(errMsg, Logger.ERROR);
            throw ex;
        } finally {
            closeStatement(stmt);
        }

    }


    /**
     * Indicates whether the given SQLException reports that an object
     * referenced by a DML statement already exists.
     *
     * @param e SQLException to be evaluated
     * @return true if e indicates that a referenced object already exists;
     *         false otherwise
     */
    private boolean isObjectAlreadyExistsException(SQLException e) {
        String sqlState = e.getSQLState();
        int vendorCode = e.getErrorCode();

        // TODO Add more SQLState/vendor code combinations for supported DBs.
        // DB2: SQLSTATE 42710
        // Oracle: SQLSTATE 42000 + vendorCode 00955
        return "42710".equals(sqlState)
                || ("42000".equals(sqlState) && 955 == vendorCode);
    }

    /**
     * Indicates whether the given SQLException reports that an object
     * referenced by a DML statement does not exist.
     *
     * @param e SQLException to be evaluated
     * @return true if e indicates that a referenced object already exists;
     *         false otherwise
     */
    private boolean isObjectDoesNotExistException(SQLException e) {
        String sqlState = e.getSQLState();
        int vendorCode = e.getErrorCode();

        // TODO Add more SQLState/vendor code combinations for supported DBs.
        // DB2: SQLSTATE 42704
        // Oracle: SQLSTATE 42000 + vendorCode 00942
        return "42704".equals(sqlState)
                || ("42000".equals(sqlState) && 942 == vendorCode);
    }

    /**
     * Update statistics associated with the given task node, setting the insert count to
     * the given value.
     *
     * @param insertCt count of rows inserted
     */
    private void updateRowsProcessedCount(ETLTaskNode node, int insertCount) {
        ETLEngineContext context = node.getParent().getContext();
        ETLEngineContext.CollabStatistics stats = context.getStatistics();

        // Record end time of table execution.
        final Timestamp endDate = new Timestamp(System.currentTimeMillis());
        stats.setTableFinishTime(node.getTableName(), endDate);

        stats.setRowsInsertedCount(node.getTableName(), insertCount);
    }

}
