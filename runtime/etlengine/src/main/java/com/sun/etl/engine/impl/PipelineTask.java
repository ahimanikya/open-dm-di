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
 * @(#)PipelineTask.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLEngineExecEvent;
import com.sun.etl.engine.ETLTask;
import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.jdbc.SQLPart;
import com.sun.sql.framework.jdbc.SQLUtils;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;

/**
 * This class generates the SQL query for transformation.
 * 
 * @version 
 * @author Jonathan Giron
 */
public class PipelineTask extends SimpleTask {

    protected static final MessageManager MSG_MGR = MessageManager.getManager("com.sun.etl.engine.impl");

    /** * Constant for this log category. */
    private static final String LOG_CATEGORY = PipelineTask.class.getName();

    private Connection conn;

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
        Logger.print(Logger.DEBUG, LOG_CATEGORY, "Handling Exception for Pipeline....");
        Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, DN, ex.getMessage(), ex);
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
            throw new ETLException(getTaskName() + " task node is null....");
        }

        if (!StringUtil.isNullString(node.getDisplayName())) {
            DN += " <" + node.getDisplayName().trim() + ">";
        }

        String msg = getMessageStarted();
        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
        node.fireETLEngineLogEvent(msg);

        SQLPart transformSQLPart = getTransformSQLPart(node);

        String insertSelect = transformSQLPart.getSQL();
        List connList = node.getParent().getConnectionDefList();

        conn = null;
        PreparedStatement stmt = null;
        String conName = transformSQLPart.getConnectionPoolName();
        ETLEngineContext context = node.getParent().getContext();
        ETLEngineContext.CollabStatistics stats = context.getStatistics();

        try {
            conn = this.getConnection(conName, connList);

            StringBuffer tmpMessage = new StringBuffer(100);
            tmpMessage.append(getTaskName());
            tmpMessage.append("<");
            tmpMessage.append(node.getId());
            tmpMessage.append("> - got connection: ");
            tmpMessage.append(conn);

            Logger.print(Logger.DEBUG, LOG_CATEGORY, tmpMessage.toString());

            // Insert new execution entry with current start time in summary table
            final Timestamp startTime = super.createExecutionEntryInSummaryTable(node);//new java.sql.Timestamp(System.currentTimeMillis());
            stats.setTableStartTime(node.getTableName(), startTime);

            final int executionId = super.getExecutionEntryIdFromSummaryTable(node, startTime);
            stats.setTableExecutionId(node.getTableName(), executionId);

            ETLEngineExecEvent evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_STARTED, node.getTableName(), "" + executionId);
            node.fireETLEngineExecutionEvent(evnt);            
            
            
            // create target table if does not exist
            if (!createBeforeProcess(node, conn)) {
                truncateBeforeProcess(node, conn);
            }

            String startMsg = MSG_MGR.getString("MSG_common_insert_attempt");
            Logger.print(Logger.DEBUG, LOG_CATEGORY, startMsg);
            node.fireETLEngineLogEvent(startMsg);

            String showSqlMsg = MSG_MGR.getString("MSG_common_using_sql", insertSelect);
            Logger.print(Logger.DEBUG, LOG_CATEGORY, showSqlMsg);
            node.fireETLEngineLogEvent(showSqlMsg);

            StringTokenizer st = new StringTokenizer(insertSelect, Character.toString(SQLPart.STATEMENT_SEPARATOR));

            List paramList = new ArrayList();
            Map attribMap = new HashMap(node.getParent().getInputAttrMap());
            populateExecutionId(attribMap, paramList, executionId);

            long insertCt = 0;
            while (st.hasMoreElements()) {
                String currStmt = st.nextToken();
                String ps = SQLUtils.createPreparedStatement(currStmt, attribMap, paramList);
                stmt = conn.prepareStatement(ps);
                SQLUtils.populatePreparedStatement(stmt, attribMap, paramList);

                int rows = stmt.executeUpdate();
                insertCt += (rows > 0) ? rows : 0;
                if (rows == Statement.EXECUTE_FAILED) {
                    String errMsg = MSG_MGR.getString("MSG_common_sql_failed_show", currStmt);
                    Logger.print(Logger.ERROR, LOG_CATEGORY, errMsg);
                    node.fireETLEngineLogEvent(errMsg);
                }
                stmt.close();
            }
            stats.setRowsInsertedCount(node.getTableName(), insertCt);

            final Timestamp endDate = new Timestamp(System.currentTimeMillis());
            stats.setTableFinishTime(node.getTableName(), endDate);

            String successMsg = MSG_MGR.getString("MSG_common_insert_success", new Long(insertCt));
            Logger.print(Logger.INFO, LOG_CATEGORY, successMsg);
            node.fireETLEngineLogEvent(successMsg);

            // Use task node table name as key to represent total number of rows
            // processed.
            // TODO This code stinks! Should get count key from a centralized source.
            context.putValue(node.getTableName(), new Long(insertCt));

            //commit later and then closes the connection
            context.commitLater(conn);

            evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_COMPLETED, node.getTableName(), "" + stats.getTableExecutionId(node.getTableName()));
            node.fireETLEngineExecutionEvent(evnt);            
        } catch (Exception ex) {
            ETLEngineExecEvent evnt = new ETLEngineExecEvent(ETLEngine.STATUS_ACTIVITY_EXCEPTION, node.getTableName(), "" + stats.getTableExecutionId(node.getTableName()));
            evnt.setCause(ex);
            node.fireETLEngineExecutionEvent(evnt);
            handleException(node, context, ex);            
        } finally {
            closeStatement(stmt);
        }

        String doneMsg = getMessageFinished();
        Logger.print(Logger.DEBUG, LOG_CATEGORY, doneMsg);
        node.fireETLEngineLogEvent(doneMsg);

        return ETLTask.SUCCESS;
    }

    protected String getMessageFinished() {
        return MSG_MGR.getString("MSG_pipeline_finished");
    }

    protected String getMessageStarted() {
        return MSG_MGR.getString("MSG_pipeline_started");
    }

    protected String getTaskName() {
        return "Pipeline";
    }

    protected void populateExecutionId(Map attribMap, List paramList, int value) {
        //do nothing, will be overriden Validating task to populate Execution ID.
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
            transformSQLPart = node.getStatement(SQLPart.STMT_DELETE);
        }

        if (transformSQLPart == null) {
            throw new ETLException("No SQL statement to execute in this transform node.");
        }
        return transformSQLPart;
    }

    private void handleException(ETLTaskNode node, ETLEngineContext context, Throwable t) throws ETLException {
        String msg;
        t = unwrapThrowable(t);

        msg = t.getMessage();
        if (StringUtil.isNullString(msg)) {
            msg = t.toString();
        }
        String failureMsg = MSG_MGR.getString("MSG_common_insert_failure", msg);
        Logger.print(Logger.DEBUG, LOG_CATEGORY, failureMsg, t);
        node.fireETLEngineLogEvent(failureMsg);

        try {
            if (conn != null) {
                if (!conn.getAutoCommit()) {
                    Logger.print(Logger.DEBUG, LOG_CATEGORY, "Rolling back transactions");
                    conn.rollback();
                }
                context.closeAndReleaseLater(conn);
            }
        } catch (Exception ignore) {
            // ignore
        }

        throw new ETLException(failureMsg, t);
    }
}
