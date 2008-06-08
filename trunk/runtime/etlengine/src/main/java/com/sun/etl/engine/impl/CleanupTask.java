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
 * @(#)CleanupTask.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLTask;
import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.jdbc.DBConnectionFactory;
import com.sun.sql.framework.jdbc.SQLPart;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;

/**
 * Implements cleanup activities after extraction and transformation.
 * 
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 * @version 
 */
public class CleanupTask extends SimpleTask {

    private static final String LOG_CATEGORY = CleanupTask.class.getName();

    private ETLTaskNode taskNode;

    /**
     * Shuts down and closes all connections referenced as ready to be closed and released
     * in the ETLEngineContext for the taskNode most recently executed by this task.
     */
    public void cleanUp() {
        if (taskNode == null) {
            Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "No associated task node!  Nothing executed in cleanUp().");
            return;
        }

        try {
            DBConnectionFactory factory = DBConnectionFactory.getInstance();
            int releasedCt = 0;

            if (factory != null) {
                ETLEngineContext context = taskNode.getContext();
                boolean isActivityCleanupTask = !(this.isThisCollaborationCleanupTask());

                try {
                    if (!isActivityCleanupTask){
                        Collection closeList = context.getConnectionsToClose();
                        Iterator iter = closeList.iterator();
                        while (iter.hasNext()) {
                            Connection aConn = (Connection) iter.next();
                            Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "Closing connection " + aConn);
                            factory.closeConnection(aConn);
                            releasedCt++;                               
                        }                        
                    }
                    
                    // flat file specific: defrag if any update/merge
                    // executed.
                    SQLPart defragPart = taskNode.getStatement(SQLPart.STMT_DEFRAG); //NOI18N
                    Connection idbConnection = null;
                    if ((defragPart != null) && (defragPart.getSQL() != null) && (!"".equals(defragPart.getSQL()))) {                    
                        try {
                            idbConnection = this.getConnection(defragPart.getConnectionPoolName(), taskNode.getParent().getConnectionDefList());
                            factory.shutdown(idbConnection, true, defragPart.getSQL());
                        } catch (Exception ex) {
                            Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, DN + this,
                                "Could not shut down DB associated with connection " + idbConnection, ex);
                        } finally {
                            factory.closeConnection(idbConnection);                               
                        }
                    }
                } finally {
                    Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "Executing node-level release commands.");
                    release(taskNode);
                }
            }

            // Write task finished message to user log.
            MessageManager messageManager = MessageManager.getManager("com.sun.etl.engine.impl");
            String msg = messageManager.getString("MSG_cleanup_finished");
            taskNode.fireETLEngineLogEvent(msg);

            // Write connection release specs to debug log.
            msg = messageManager.getString("MSG_cleanup_finished_specs", new Integer(releasedCt));
            Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
        } catch (Exception e) {
            Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, DN, e.getMessage(), e);
        } finally {
            taskNode = null;
        }
    }

    /**
     * Handles the BaseException
     * 
     * @param ex BaseException that needs to be handled
     */
    public void handleException(com.sun.sql.framework.exception.BaseException ex) {
        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN, "Handling Exception for cleanup task....");
        Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, DN, ex.getMessage(), ex);
        cleanUp();
    }

    /**
     * @param node Process the given ETLTaskNode
     * @exception Exception caused during Process
     * @return Return Success or Failure String
     */
    public String process(ETLTaskNode node) throws ETLException {
        boolean isTruncate = false;
        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "Processing cleanup task...");
        taskNode = node;

        if (!StringUtil.isNullString(taskNode.getDisplayName())) {
            DN += " <" + taskNode.getDisplayName().trim() + ">";
        }

        Connection con = null;
        Statement stmt = null;
        final MessageManager messageManager = MessageManager.getManager("com.sun.etl.engine.impl");

        if (node != null) {
            // First drop all extractor temp tables.
            SQLPart sqlPart = node.getStatement(SQLPart.STMT_DROP); //NOI18N
            if( sqlPart == null ) {
                sqlPart = node.getStatement(SQLPart.STMT_TRUNCATEBEFOREPROCESS);
                isTruncate = true;
            }
            if (sqlPart != null) {
                String cleanUpSQL = sqlPart.getSQL();
                String cleanUpSQLConName = sqlPart.getConnectionPoolName();
                try {
                    List connList = node.getParent().getConnectionDefList();

                    con = getConnection(cleanUpSQLConName, connList);
                    if(isTruncate) {
                        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "Got connection for truncate: " + con);
                    } else {
                        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "Got connection for drop: " + con);
                    }

                    stmt = con.createStatement();

                    String attemptDropMsg = messageManager.getString(isTruncate ? "MSG_cleanup_truncate_attempt":"MSG_cleanup_drop_attempt");
                    Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + attemptDropMsg);
                    node.fireETLEngineLogEvent(attemptDropMsg);

                    StringTokenizer st = new StringTokenizer(cleanUpSQL, Character.toString(SQLPart.STATEMENT_SEPARATOR));
                    int tableCt = 0;
                    while (st.hasMoreElements()) {
                        String sql = st.nextToken();
                        tableCt++;

                        String dropSqlMsg = messageManager.getString(isTruncate ? "MSG_common_truncate_stmt":"MSG_common_drop_stmt", sql);

                        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + dropSqlMsg);
                        node.fireETLEngineLogEvent(dropSqlMsg);

                        stmt.addBatch(sql);
                    }

                    int[] status = stmt.executeBatch();
                    con.commit();

                    int dropCt = 0;
                    for (int i = 0; i < status.length; i++) {
                        dropCt += (status[i] != Statement.EXECUTE_FAILED) ? 1 : 0;
                    }

                    String dropCountMsg = messageManager.getString(isTruncate ? "MSG_cleanup_truncate_count":"MSG_cleanup_drop_count", new Integer(dropCt), new Integer(tableCt));
                    Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + dropCountMsg);
                    node.fireETLEngineLogEvent(dropCountMsg);
                } catch (Exception ignore) {
                    //Ignore
                } finally {
                    closeStatement(stmt);
                    // Push connection to list of those to be closed in
                    // cleanUp().
                    node.getParent().getContext().closeAndReleaseLater(con);
                }
            }
        } else {
            Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "No associated task node!");
        }

        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "Finished cleanup task.");
        return ETLTask.SUCCESS;
    }

    private void execute(Connection con, String sql) throws SQLException {
        Statement stmt = con.createStatement();
        try {
            if (sql != null) {
                stmt.executeUpdate(sql);
            }
        } catch (SQLException ignore) {
            Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "Error occurred during execution of SQL statement.", ignore);
        } finally {
            closeStatement(stmt);
        }
    }

    private void execute(List sqlParts, List conDefList) throws SQLException, Exception {
        //Drop First
        Iterator iter = sqlParts.iterator();
        while (iter.hasNext()) {
            SQLPart part = (SQLPart) iter.next();
            String sql = part.getSQL();
            String poolName = part.getConnectionPoolName();
            Connection con = null;

            try {
                con = this.getConnection(poolName, conDefList);
                if (sql != null) {
                    execute(con, sql);
                }
            } finally {
                DBConnectionFactory.getInstance().closeConnection(con);
            }
        }
    }

    private void release(ETLTaskNode node) {
        ETLEngine engine = node.getParent();

        List conDefList = engine.getConnectionDefList();

        try {
            if (!engine.isRunningOnAppServer()) {
                return;
            }

            // TODO Move optional tasks associated with cleanup on app server to a
            // specialized map.
            List sqlParts = node.getOptionalTasks();
            if (sqlParts.size() != 0) {
                execute(sqlParts, conDefList);
            }
        } catch (Exception e) {
            Logger.print(Logger.ERROR, LOG_CATEGORY, DN + "Failed to execute on appConn.", e);
        }
    }
    
    private boolean isThisCollaborationCleanupTask(){
    	boolean ret = false;
    	if (this.taskNode != null){
    		String nextTaskList = this.taskNode.getNextTaskList("Success");
    		if (nextTaskList.indexOf("STOP") >= 0){
    			ret = true;
    		}
    	}
    	return ret;
    }
}
