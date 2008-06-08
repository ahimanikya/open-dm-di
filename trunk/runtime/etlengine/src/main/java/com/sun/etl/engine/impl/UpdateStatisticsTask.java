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
 * @(#)UpdateStatisticsTask.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Time;
import java.util.Iterator;
import java.util.List;

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
 * Updates summary statistics table in mbean monitor for a particular deployed eTL
 * application.
 * 
 * @author Jonathan Giron
 * @version 
 */
public class UpdateStatisticsTask extends SimpleTask {

    private static final String LOG_CATEGORY = UpdateStatisticsTask.class.getName();

    // Any change here or in template file need to be synchronized.
    // \com\sun\sql\framework\evaluators\database\axion\config\createLogSummaryTable.vm
    private static final int MAX_EXCEPTION_MESSAGE_LENGTH = 2000;

    private static final MessageManager MSG_MGR = MessageManager.getManager("com.sun.etl.engine.impl");

    public void cleanUp() {
        super.cleanUp();
    }

    public void handleException(ETLException ex) {
        Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, "UpdateStatisticsTask", "Handling exception for UpdateStatistics....", ex);
    }

    public String process(ETLTaskNode node) throws ETLException {
        // Commented out for now - Need to fix it (MS5)
        if (node == null) {
            throw new ETLException("UpdateStatistics task node is null....");
        }

        if (!StringUtil.isNullString(node.getDisplayName())) {
            DN += " <" + node.getDisplayName().trim() + ">";
        }

        String msg = MSG_MGR.getString("MSG_updatestats_started");
        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + msg);
        node.fireETLEngineLogEvent(msg);

        ETLEngineContext context = node.getContext();
        ETLEngineContext.CollabStatistics collabStats = context.getStatistics();
        List connList = node.getParent().getConnectionDefList();
        Connection conn = null;
        PreparedStatement stmt = null;

        synchronized (UpdateStatisticsTask.class) {
        // Allow only one thread to update and commit.
			try {
				Iterator tableNameIter = node.getTableNamesWithSpecificStatements().iterator();
				while (tableNameIter.hasNext()) {
					String tableName = (String) tableNameIter.next();
					SQLPart updateEndDatePart = node.getTableSpecificStatement(tableName, SQLPart.STMT_UPDATEEXECUTIONRECORD);

					if (updateEndDatePart != null) {
						String poolName = updateEndDatePart.getConnectionPoolName();
						String updateStmt = updateEndDatePart.getSQL();
						conn = getConnection(poolName, connList);
						conn.setAutoCommit(true);
						
						// Get count of rejected rows from details table and
						// update statistics
						// context
						super.computeRowCountStatistics(node, tableName, conn);

						stmt = conn.prepareStatement(updateStmt);
						Timestamp startTs = collabStats.getTableStartTime(tableName);
						Timestamp finishTs = collabStats.getTableFinishTime(tableName);
						long extractedCt = collabStats.getRowsExtractedCount(tableName);
						long insertedCt = collabStats.getRowsInsertedCount(tableName);
						long rejectedCt = collabStats.getRowsRejectedCount(tableName);
						String exceptionMessage = getExceptionMessage(context.getThrowableList());
                                                
						stmt.setTimestamp(1, finishTs);
                                                stmt.setLong(2, extractedCt);
						stmt.setLong(3, insertedCt);
						stmt.setLong(4, rejectedCt);
						stmt.setString(5, exceptionMessage);
						stmt.setTimestamp(6, startTs);

						stmt.executeUpdate();
						stmt.close();

						// conn.commit();
						DBConnectionFactory.getInstance().closeConnection(conn);
						conn = null;

						msg = MSG_MGR.getString("MSG_updatestats_wroteStats", tableName, new Long(extractedCt), new Long(insertedCt), new Long(rejectedCt));
						Logger.print(Logger.INFO, LOG_CATEGORY, DN + msg);
					}
				}

				stmt = null;
				conn = null;
			} catch (Exception t) {
                                t.printStackTrace();
				t = (Exception) unwrapThrowable(t);

				msg = t.getMessage();
				if (StringUtil.isNullString(msg)) {
					msg = t.toString();
					if (StringUtil.isNullString(msg)) {
						msg = MSG_MGR
								.getString("MSG_common_no_details_available");
					}
				}
				String failureMsg = MSG_MGR.getString(
						"MSG_updatestats_insert_failure", msg);
				Logger.print(Logger.DEBUG, LOG_CATEGORY, failureMsg, t);
				node.fireETLEngineLogEvent(failureMsg);
				throw new ETLException(failureMsg, t);
			} finally {
				closeStatement(stmt);
				DBConnectionFactory.getInstance().closeConnection(conn);
			}

		}

        String doneMsg = MSG_MGR.getString("MSG_updatestats_finished");
        Logger.print(Logger.DEBUG, LOG_CATEGORY, doneMsg);
        node.fireETLEngineLogEvent(doneMsg);
        return ETLTask.SUCCESS;
    }

    private String getExceptionMessage(List throwableList) {
        StringBuffer sb = null;
        String exceptionMessage = null;

        if ((throwableList != null) && (throwableList.size() > 0)) {
            Iterator itr = throwableList.iterator();
            Throwable ex = null;
            sb = new StringBuffer();

            int i = 0;
            while (itr.hasNext()) {
                try {
                    if (i++ > 0) {
                        sb.append("\n");
                    }
                    ex = (Throwable) itr.next();
                    sb.append(ex.getMessage());
                } catch (ClassCastException exp) {
                    Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, "UpdateStatisticsTask",
                        "Caught ClassCastException while constructing exception message.", exp);
                }
            }
            if (sb.length() > MAX_EXCEPTION_MESSAGE_LENGTH) {
                exceptionMessage = sb.substring(0, MAX_EXCEPTION_MESSAGE_LENGTH);
            } else {
                exceptionMessage = sb.toString();
            }
        }

        return exceptionMessage;
    }
}
