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
 * @(#)SimpleTask.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.StringTokenizer;

import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLTask;
import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.DBConnectionFactory;
import com.sun.sql.framework.jdbc.DBConnectionParameters;
import com.sun.sql.framework.jdbc.SQLPart;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.jdbc.SQLUtils;
import com.sun.sql.framework.utils.StringUtil;

/**
 * This class is a Basic Task.
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public class SimpleTask implements ETLTask {
    private static final String LOG_CATEGORY = SimpleTask.class.getName();
    private static final MessageManager MSG_MGR = MessageManager.getManager("com.sun.etl.engine.impl");
    protected static final String NL = System.getProperty("line.separator", "\n");

    /** Display name for message context. */
    protected String DN = "";

    private Connection statsConn;
    private final String URL_PREFIX = "jdbc:axiondb:";

    public SimpleTask() {
    }

    /**
     * @see com.sun.etl.engine.ETLTask#cleanUp
     */
    public void cleanUp() {
        if (statsConn != null) {
            DBConnectionFactory.getInstance().closeConnection(statsConn);
        }
    }

    /**
     * @see com.sun.etl.engine.ETLTask#handleException
     */
    public void handleException(ETLException ex) {
    }

    /**
     * @see com.sun.etl.engine.ETLTask#process
     */
    public String process(ETLTaskNode node) throws ETLException {
        return ETLTask.SUCCESS;
    }

    protected void closeResultSet(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            // ignore
        }
    }

    protected void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ignore) {
                // ignore
            }
        }
    }

    /**
     * TODO Determine whether this method should be moved back to PipelineStatistics - it
     * may have to be invoked using the same connection that executed the multi-part
     * insert.
     * 
     * @param node
     */
    protected void computeRowCountStatistics(ETLTaskNode node, String tableName, Connection conn) throws ETLException {
        SQLPart selectRejectedRowCountPart = node.getTableSpecificStatement(tableName, SQLPart.STMT_SELECTREJECTEDROWCTFROMDETAILS);

        PreparedStatement stmt = null;
        ETLEngineContext.CollabStatistics stats = node.getContext().getStatistics();
        final long insertCt = stats.getRowsInsertedCount(tableName);
        long rejectedRows = 0;
        long extractedRows = insertCt;

        try {
            if (selectRejectedRowCountPart != null) {

                int executionId = stats.getTableExecutionId(tableName);

                String selectSql = selectRejectedRowCountPart.getSQL();
                stmt = conn.prepareStatement(selectSql);
                stmt.setInt(1, executionId);

                ResultSet rs = stmt.executeQuery();
                rs.next();

                rejectedRows = rs.getInt(1);
                extractedRows = insertCt + rejectedRows;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ETLException(e);
        } finally {
            closeStatement(stmt);
            stats.setRowsRejectedCount(tableName, rejectedRows);
            stats.setRowsExtractedCount(tableName, extractedRows);
        }
    }

    protected boolean createBeforeProcess(ETLTaskNode node, Connection con) throws ETLException {
        Statement stmt = null;
        boolean originalState = false;
        boolean tableCreated = false;

        try {
            SQLPart createSQLPart = node.getStatement(SQLPart.STMT_CREATEBEFOREPROCESS); //NOI18N
            if (createSQLPart == null) {
                return tableCreated; // user did not choose to create if does not exist
            }

            originalState = con.getAutoCommit();
            //DBConnectionFactory.getInstance().setAutoCommit(con, true);
            setAutoCommitIfRequired(con, true);
            stmt = con.createStatement();
            Iterator stmtIter = createSQLPart.getIterator();
            if (stmtIter.hasNext()) {
                String ifExists = (String) stmtIter.next();
                if (!isTableExists(con, ifExists, node) && stmtIter.hasNext()) {
                    do {
                        String doCreate = (String) stmtIter.next();
                        String msg = MSG_MGR.getString("MSG_simple_show_create_stmt", doCreate);
                        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
                        node.fireETLEngineLogEvent(NL + msg + NL);

                        stmt.executeUpdate(doCreate);
                    } while (stmtIter.hasNext());
                    tableCreated = true;
                }
            }
        } catch (Exception e) {
            throw new ETLException("Error occurred while attempting to create table", e);
        } finally {
            closeStatement(stmt);
            resetCommitState(con, originalState);
        }
        return tableCreated;
    }

    /**
     * Creates an execution entry in the summary table of the collab statistics database.
     * 
     * @param node ETLTaskNode containing SQL statement for inserting an execution entry
     * @return Timestamp representing time recorded with new execution entry; returns
     *         <code>startTime</code> if not null, otherwise returns current time when
     *         execution entry was created.
     * @throws SQLException if error occurs during creation of the execution entry
     */
    protected Timestamp createExecutionEntryInSummaryTable(ETLTaskNode node) throws BaseException {
        SQLPart insertStartDatePart = node.getStatement(SQLPart.STMT_INSERTEXECUTIONRECORD); //NOI18N
        PreparedStatement stmt = null;
        Timestamp timeUsed = null;
        
        if (insertStartDatePart != null) {
            // Critical section: only one thread should insert its new record into the
            // summary table at any one time.
            synchronized (SimpleTask.class) {
                try {                	
                    String connPoolName = insertStartDatePart.getConnectionPoolName();
                    if (statsConn == null) {
                        statsConn = this.getConnection(connPoolName, node.getParent().getConnectionDefList());
                    }
                    statsConn.setAutoCommit(true);
                    String insertStmt = insertStartDatePart.getSQL();
                    stmt = statsConn.prepareStatement(insertStmt);
                    timeUsed = new Timestamp(System.currentTimeMillis());                    
                    stmt.setTimestamp(1, timeUsed);
                    int noOfRecords = stmt.executeUpdate();
                   
                } catch (SQLException e) {
                    throw new BaseException(e);
                } finally {
                    closeStatement(stmt);
                }
            }
        }
        return timeUsed;
    }

    /**
     * @param node
     */
    protected void createSummaryTable(ETLTaskNode node) throws ETLException {
        // Create summary table if it doesn't already exist.
        SQLPart summarySql = node.getStatement(SQLPart.STMT_CREATELOGSUMMARYTABLE);
        if (null != summarySql) {
            Statement stmt = null;
            try {
                String poolName = summarySql.getConnectionPoolName();
                if (null == statsConn) {
                    statsConn = this.getConnection(poolName, node.getParent().getConnectionDefList());
                }
                statsConn.setAutoCommit(true);
                
                stmt = statsConn.createStatement();
                stmt.executeUpdate(summarySql.getSQL());
            } catch (Exception e) {
                Logger.print(Logger.ERROR, LOG_CATEGORY, DN + "Failed to execute InitTask", e);
                throw new ETLException(DN + "Failed to execute InitTask.", e);
            } finally {
                closeStatement(stmt);
            }
        }
    }

    protected Connection getConnection(String poolName, List connList) throws BaseException {
        DBConnectionFactory factory = DBConnectionFactory.getInstance();
        DBConnectionParameters dbConDefn = lookupConnDefinitionBy(poolName, connList);

        if (dbConDefn != null) {
            Connection con = factory.getConnection(dbConDefn);
            return con;
        }
        throw new BaseException("Could not locate connection definition for pool name " + poolName);
    }

    /**
     * @param node
     * @return
     */
    protected int getExecutionEntryIdFromSummaryTable(ETLTaskNode node, Timestamp startTime) throws BaseException {
        int executionId = -1;

        //select execution Id from summary table
        SQLPart selectExecutionIdPart = node.getStatement(SQLPart.STMT_SELECTEXECUTIONIDFROMSUMMARY); //NOI18N
        PreparedStatement stmt = null;

        if (selectExecutionIdPart != null) {
            synchronized (SimpleTask.class) {
                try {
                    String connPoolName = selectExecutionIdPart.getConnectionPoolName();
                    if (statsConn == null) {
                        statsConn = this.getConnection(connPoolName, node.getParent().getConnectionDefList());
                    }

                    statsConn.setAutoCommit(true);
                    
                    String selectSql = selectExecutionIdPart.getSQL();
                    stmt = statsConn.prepareStatement(selectSql);
                    stmt.setTimestamp(1, startTime);
                    stmt.setString(2, selectExecutionIdPart.getTableName());

                    ResultSet rs = stmt.executeQuery();

                    // QAI 83952: Don't assume ResultSet is in a valid state - table
                    // could be empty due to failure to insert the initial execution
                    // entry.
                    if (rs.next()) {
                        executionId = rs.getInt(1);
                    } else {
                        throw new BaseException(MSG_MGR.getString("MSG_common_execid_notfound"));
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new BaseException(e);
                } finally {
                    closeStatement(stmt);
                }
            }
        }

        return executionId;
    }

    protected String getMetadataDir(String url) {
        String metaData = null;
        String prefixStripped = url.substring(URL_PREFIX.length());
        int colon = prefixStripped.indexOf(":");
        if (colon == -1 || (prefixStripped.length() - 1 == colon)) {
        } else {
            String temp = prefixStripped.substring(colon + 1);
            StringTokenizer st = new StringTokenizer(temp, ";");
            if (st.hasMoreTokens()) {
                metaData = st.nextToken();
                return metaData;
            }
        }
        return null;
    }

    protected boolean isTableExists(Connection conn, String checkSQL, ETLTaskNode node) throws BaseException {
        boolean isTableExist = false;
        ResultSet rs = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            Map attribMap = node.getParent().getInputAttrMap();
            checkSQL = SQLUtils.replaceTableNameFromRuntimeArguments(checkSQL, attribMap);
            rs = stmt.executeQuery(checkSQL);
            // Query assumes DB knowledge
            if (checkSQL.indexOf("SELECT 1 FROM")>= 0){
                // No exception, so table is present.
                isTableExist = true;
            } else {
                isTableExist = rs.next();
            }
        } catch (SQLException e) {
            isTableExist = false;
        } finally {
            closeResultSet(rs);
            closeStatement(stmt);
        }
        return isTableExist;
    }

    /**
     * Logs given SQLException to app logger, using the given log level and category.
     * 
     * @param logLevel log level to use in classifying the message
     * @param log_category log category to use in classifying the message
     * @param e SQLException to be logged
     */
    protected void logSQLException(int logLevel, String logCategory, SQLException e) {
        if (logCategory == null) {
            logCategory = LOG_CATEGORY;
        }

        String sqlState = e.getSQLState();
        String vendorCode = Integer.toString(e.getErrorCode());
        String msg = MSG_MGR.getString("MSG_common_sqlexception", DN, sqlState, vendorCode, e.getLocalizedMessage());
        Logger.print(logLevel, logCategory, msg);
    }

    /**
     * Gets DBConnectionDefinition, if any, with the given name from the given List.
     * 
     * @param name name of DBConnectionParameters to retrieve from list
     * @param defList list of DBConnectionParameters instances to search from
     * @return matching DBConnectionParameters from <code>defList</code>, or null if no
     *         matches are found
     */
    protected DBConnectionParameters lookupConnDefinitionBy(String name, List defList) {
        if (defList == null || name == null) {
            return null;
        }

        DBConnectionParameters result = null;
        Iterator iter = defList.iterator();
        while (iter.hasNext()) {
            DBConnectionParameters item = (DBConnectionParameters) iter.next();
            if (item.getName().equalsIgnoreCase(name)) {
                result = item;
                break;
            }
        }
        return result;
    }

    protected void truncateBeforeProcess(ETLTaskNode node, Connection con) {
        Statement stmt = null;
        String doTruncate = null;
        boolean originalState = false;

        try {
            SQLPart truncateSQLPart = node.getStatement(SQLPart.STMT_TRUNCATEBEFOREPROCESS);

            if (truncateSQLPart == null) {
                return; // user did not choose to truncate if does not exist
            }

            originalState = con.getAutoCommit();
            //DBConnectionFactory.getInstance().setAutoCommit(con, true);
            setAutoCommitIfRequired(con, true);
            stmt = con.createStatement();
            truncateTable(node, con, stmt, truncateSQLPart);
        } catch (Exception e) {
            String msg = MSG_MGR.getString("MSG_simple_truncate_failed");
            Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, DN, msg, e);
            node.fireETLEngineLogEvent(msg);

            // Use delete statement as fallback.
            deleteOnFailedTruncate(node, con, doTruncate);
        } finally {
            closeStatement(stmt);
            resetCommitState(con, originalState);
        }
    }

    /**
     * Sets auto-commit flag on the given Connection to the value of the given boolean if
     * required by the Connection's underlying database. Sybase databases do not allow
     * drop and create statements within multi-statement transactions, so we need to allow
     * autocommital of the individual drop and create statements. Otherwise, we should
     * leave the autocommit flag at its default setting. TODO Verify this is still the
     * case when Sybase eWay becomes available.
     *
     * @param conn Connection whose auto-commit flag should be set to the value of
     *        <code>flag</code>, if required.
     * @param flag value to use in setting auto-commit flag if deemed necessary.
     */
    protected void setAutoCommitIfRequired(Connection conn, boolean flag) {
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            String url = dbmd.getURL();
            if (!StringUtil.isNullString(url) && url.toUpperCase().indexOf(":SYBASE:") != -1) {
                // DataDirect driver is Ok, but JConnector driver cribs for setting to true without
                // Rollback or Commit.
                conn.commit();
                conn.setAutoCommit(flag);
            }
        } catch (SQLException ignore) {
            Logger.print(Logger.DEBUG, LOG_CATEGORY, "Could not determine database vendor in order to set autocommit flag - continuing.");
        }
    }


    /**
     * @param t
     * @return
     */
    protected Throwable unwrapThrowable(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
            // Prevent infinite loop if cause of t references itself.
            if (t.getCause() == t) {
                break;
            }
        }
        return t;
    }

    /**
     * @param con
     */
    private void deleteOnFailedTruncate(ETLTaskNode node, Connection con, final String truncateSql) {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            SQLPart deleteSQLPart = node.getStatement(SQLPart.STMT_DELETEBEFOREPROCESS);

            if (deleteSQLPart == null || truncateSql == null) {
                return; // user did not choose to truncate if does not exist
            }

            // Execute delete statement only if it's not identical to the
            // previously attempted truncate statement.
            String doDelete = deleteSQLPart.getSQL();
            if (doDelete != null && truncateSql != null && !doDelete.trim().equalsIgnoreCase(truncateSql.trim())) {

                String msg = MSG_MGR.getString("MSG_simple_truncate_fallback");
                Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
                node.fireETLEngineLogEvent(msg);

                truncateTable(node, con, stmt, deleteSQLPart);

                msg = MSG_MGR.getString("MSG_simple_truncate_fallback_success");
                Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
                node.fireETLEngineLogEvent(msg);
            }
        } catch (Exception e) {
            String msg = MSG_MGR.getString("MSG_simple_truncate_fallback_failure", e.getMessage());
            Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, DN, msg, e);
            node.fireETLEngineLogEvent(msg);
        } finally {
            closeStatement(stmt);
        }
    }

    private void resetCommitState(Connection con, boolean originalState) {
        try {
            if (con != null) {
                con.setAutoCommit(originalState);
            }
        } catch (SQLException e) {
            // ignore
        }
    }

    private void truncateTable(ETLTaskNode node, Connection con, Statement stmt, SQLPart truncateSQLPart) throws BaseException, SQLException {
        String doDelete;
        String msg;
        Iterator stmtIter = truncateSQLPart.getIterator();
        if (stmtIter.hasNext()) {
            String ifExists = (String) stmtIter.next();

            if (isTableExists(con, ifExists, node) && stmtIter.hasNext()) {
                msg = MSG_MGR.getString("MSG_simple_truncate_attempt");
                Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
                node.fireETLEngineLogEvent(msg);

                do {
                    doDelete = (String) stmtIter.next();

                    msg = MSG_MGR.getString("MSG_common_using_sql", doDelete);
                    Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
                    node.fireETLEngineLogEvent(msg);

                    stmt.executeUpdate(doDelete);
                } while (stmtIter.hasNext());

                msg = MSG_MGR.getString("MSG_simple_truncate_success");
                Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
                node.fireETLEngineLogEvent(msg + NL);
            }
        }
    }
}
