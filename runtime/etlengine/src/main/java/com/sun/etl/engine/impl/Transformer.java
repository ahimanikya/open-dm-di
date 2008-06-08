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
 * @(#)Transformer.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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
import com.sun.sql.framework.jdbc.DBConnectionFactory;
import com.sun.sql.framework.jdbc.SQLPart;
import com.sun.sql.framework.jdbc.SQLUtils;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;

/**
 * This class generates the SQL query for transformation.
 * 
 * @version 
 * @author Amrish K. Lal
 * @author Ahimanikya Satapathy
 */
public class Transformer extends SimpleTask  {

    /** * Constant for this log category. */
    private static final String LOG_CATEGORY = Transformer.class.getName();

    private static final MessageManager MSG_MGR = MessageManager.getManager("com.sun.etl.engine.impl");

    private Connection con;

    /**
     * Cleans up the resources
     */
    public void cleanUp() {
        super.cleanUp();
    }

    /**
     * Handles BaseException
     * 
     * @param ex BaseException that needs to be handled
     */
    public void handleException(ETLException ex) {
        Logger.print(Logger.DEBUG, LOG_CATEGORY, "Handling exception for Transformer....");
        Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, DN, ex.getMessage(), ex);

        // Ensure target connection is released unconditionally if exception is thrown.
        if (con != null) {
            DBConnectionFactory.getInstance().closeConnection(con);
            con = null;
        }
    }

    /**
     * Process the given TaskNode
     * 
     * @param node Current TaskNode
     * @return Success or failure of execution of tasknode
     * @throws ETLException indicating processing problem.
     */
    public String process(ETLTaskNode node) throws ETLException {
        if (node == null) {
            throw new ETLException("Transformer task node is null....");
        }

        if (!StringUtil.isNullString(node.getDisplayName())) {
            DN += " <" + node.getDisplayName().trim() + ">";
        }

        String msg = MSG_MGR.getString("MSG_transformer_started");
        node.fireETLEngineLogEvent(msg, Logger.DEBUG);

        SQLPart transformSQLPart = getTransformSQLPart(node);

        List connList = node.getParent().getConnectionDefList();

        con = null;
        PreparedStatement stmt = null;
        String conName = transformSQLPart.getConnectionPoolName();
        ETLEngineContext context = node.getParent().getContext();
        ETLEngineContext.CollabStatistics stats = context.getStatistics();

        try {
            // Insert new execution entry with current start time in summary table
            // Commented out for now - Need to fix it (MS5)
            final Timestamp startTime = super.createExecutionEntryInSummaryTable(node);
            stats.setTableStartTime(node.getTableName(), startTime);

            final int executionId = super.getExecutionEntryIdFromSummaryTable(node, startTime);
            stats.setTableExecutionId(node.getTableName(), executionId);
            ETLEngineExecEvent evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_STARTED, node.getTableName(), "" + executionId);
            node.fireETLEngineExecutionEvent(evnt);            
            con = this.getConnection(conName, connList);
            Logger.print(Logger.INFO, LOG_CATEGORY, "Transformer<" + node.getId() + "> - got connection: " + con);

            // create target table if does not exist
            if (!createBeforeProcess(node, con)) {
                truncateBeforeProcess(node, con);
            }
            String startMsg = MSG_MGR.getString("MSG_transformer_insert_attempt");
            node.fireETLEngineLogEvent(startMsg);
            String showSqlMsg = MSG_MGR.getString("MSG_common_using_sql", transformSQLPart);
            node.fireETLEngineLogEvent(showSqlMsg, Logger.DEBUG);

            List paramList = new ArrayList();
            Map attribMap = node.getParent().getInputAttrMap();
            long insertCt = 0;

            Iterator stmtIter = transformSQLPart.getIterator();
            while (stmtIter.hasNext()) {
                String currStmt = (String) stmtIter.next();
                //Replace the Tokenized table name, if any
                currStmt = SQLUtils.replaceTableNameFromRuntimeArguments(currStmt, attribMap);
                
                showSqlMsg = MSG_MGR.getString("MSG_common_using_sql", currStmt);
                node.fireETLEngineLogEvent(showSqlMsg, Logger.DEBUG);
                String ps = SQLUtils.createPreparedStatement(currStmt, attribMap, paramList);
                stmt = con.prepareStatement(ps);
                SQLUtils.populatePreparedStatement(stmt, attribMap, paramList);                
                int rows = stmt.executeUpdate();                
                insertCt += (rows > 0) ? rows : 0;
                if (rows == Statement.EXECUTE_FAILED) {
                    String errMsg = MSG_MGR.getString("MSG_common_sql_failed_show", currStmt);
                    node.fireETLEngineLogEvent(errMsg, Logger.ERROR);
                }
                
                stmt.close();
            }
            
            updateInsertStats(node, insertCt);
            // commit later and then close the connection
            context.commitLater(con);

            //String successMsg = MSG_MGR.getString("MSG_transformer_insert_success", new Long(insertCt));
            String successMsg = MSG_MGR.getString("MSG_transformer_insert_success", String.valueOf(insertCt));
            node.fireETLEngineLogEvent(successMsg);
            // Commented out for now - Need to fix it (MS5)
            //evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_COMPLETED, node.getTableName(), "" + stats.getTableExecutionId(node.getTableName()));
            //  evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_COMPLETED, node.getTableName(), "" + "0");
            //  node.fireETLEngineExecutionEvent(evnt);            
        } catch (Exception ex) {
            ex.printStackTrace();
            ETLEngineExecEvent evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_EXCEPTION, node.getTableName(), "" + stats.getTableExecutionId(node.getTableName()));
            evnt.setCause(ex);
            node.fireETLEngineExecutionEvent(evnt);                            
            handleException(node, ex);            
        } finally {
            closeStatement(stmt);
        }

        String doneMsg = MSG_MGR.getString("MSG_transformer_finished");
        node.fireETLEngineLogEvent(doneMsg, Logger.DEBUG);

        return ETLTask.SUCCESS;
    }

    private SQLPart getTransformSQLPart(ETLTaskNode node) throws ETLException {
        SQLPart transformSQLPart = node.getStatement(SQLPart.STMT_STATICINSERT); //NOI18N
        if (transformSQLPart == null) {
            transformSQLPart = node.getStatement(SQLPart.STMT_INSERTSELECT); //NOI18N
        }

        if (transformSQLPart == null) {
            transformSQLPart = node.getStatement(SQLPart.STMT_MERGE); //NOI18N
        }

        if (transformSQLPart == null) {
            transformSQLPart = node.getStatement(SQLPart.STMT_UPDATE); //NOI18N
        }

        if (transformSQLPart == null) {
            transformSQLPart = node.getStatement(SQLPart.STMT_DELETE); //NOI18N
        }

        if (transformSQLPart == null) {
            throw new ETLException("No SQL statement to execute in this transform node.");
        }
        return transformSQLPart;
    }

    private void handleException(ETLTaskNode node, Throwable t) throws ThreadDeath, ETLException {
        String msg;
        t = unwrapThrowable(t);

        msg = t.getMessage();
        if (StringUtil.isNullString(msg)) {
            msg = t.toString();
        }
        String failureMsg = MSG_MGR.getString("MSG_transformer_insert_failure", msg);
        node.fireETLEngineLogEvent(failureMsg, Logger.INFO);
        if (t instanceof SQLException) {
            logSQLException(Logger.INFO, LOG_CATEGORY, (SQLException) t);
            t.printStackTrace();
        }

        try {
            if (con != null) {
                if (!con.getAutoCommit()) {
                    Logger.print(Logger.DEBUG, LOG_CATEGORY, "Rolling back transactions");
                    con.rollback();
                }
            }
        } catch (Exception ignore) {
            // ignore
        }

        // Set insert count to zero, as failure has occurred.
        updateInsertStats(node, 0);

        throw new ETLException(failureMsg, t);
    }

    /**
     * Update statistics associated with the given task node, setting the insert count to
     * the given value.
     * 
     * @param insertCt count of rows inserted
     */
    private void updateInsertStats(ETLTaskNode node, long insertCt) {
        ETLEngineContext context = node.getParent().getContext();
        ETLEngineContext.CollabStatistics stats = context.getStatistics();

        final Timestamp endDate = new Timestamp(System.currentTimeMillis());
        stats.setTableFinishTime(node.getTableName(), endDate);

        // Use task node table name as key to represent total number of rows processed.
        // TODO This code stinks! Should get count key from a centralized source.
        context.putValue(node.getTableName(), new Long(insertCt));
        stats.setRowsInsertedCount(node.getTableName(), insertCt);
    }
}
