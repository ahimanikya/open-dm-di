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
 * @(#)CommitTask.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.ListIterator;

import com.sun.etl.engine.ETLTask;
import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;

/**
 * Implements commit and closure of DB connections associated with transformation process.
 * 
 * @author Jonathan Giron
 * @version :
 */
public class CommitTask extends SimpleTask {

    private static final String LOG_CATEGORY = CommitTask.class.getName();

    private static final MessageManager MSG_MGR = MessageManager.getManager("com.sun.etl.engine.impl");

    private ETLTaskNode taskNode;

    public void cleanUp() {
        taskNode = null;
        super.cleanUp();
    }

    public void handleException(ETLException ex) {
        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + "Handling Exception for commit task....");
        Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, this, "Caught exception while processing commit task", ex);
    }

    public String process(ETLTaskNode node) throws ETLException {
        if (node != null) {
            this.taskNode = node;
        } else {
            throw new ETLException("No associated task node - cannot obtain commit list!");
        }

        if (!StringUtil.isNullString(node.getDisplayName())) {
            DN += " <" + node.getDisplayName().trim() + ">";
        }

        String status = ETLTask.SUCCESS;
        int commitCt = 0;
        int closeCt = 0;

        List connCommitList = taskNode.getParent().getContext().getConnectionListToCommit();

        String startMsg = MSG_MGR.getString("MSG_commit_attempt", new Integer(connCommitList.size()));
        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + startMsg);
        node.fireETLEngineLogEvent(startMsg);

        ListIterator iter = connCommitList.listIterator();
        while (iter.hasNext()) {
            Connection victim = (Connection) iter.next();
            try {
                if (!victim.getAutoCommit()) {
                    victim.commit();
                    String commitMsg = MSG_MGR.getString("MSG_commit_success", new Integer(iter.nextIndex()));
                    node.fireETLEngineLogEvent(commitMsg);
                    commitCt++;
                } else {
                    String errMsg = MSG_MGR.getString("MSG_commit_autocommitted", new Integer(iter.nextIndex()));
                    node.fireETLEngineLogEvent(errMsg);
                }
            } catch (SQLException ex) {
                String errMsg = MSG_MGR.getString("MSG_commit_exception", ex.getMessage());
                node.fireETLEngineLogEvent(errMsg);
                status = ETLTask.EXCEPTION;
                throw new ETLException(errMsg, ex);
            } finally {
                taskNode.getContext().closeAndReleaseLater(victim);
                closeCt++;

                String markMsg = MSG_MGR.getString("MSG_commit_marked", new Integer(iter.nextIndex()));
                Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + markMsg);
            }
        }

        // Write task finished message to user log.
        String doneMsg = MSG_MGR.getString("MSG_commit_finished");
        taskNode.fireETLEngineLogEvent(doneMsg);

        // Write commit and marked-for-release specs to debug log.
        doneMsg = MSG_MGR.getString("MSG_commit_finished_specs", new Integer(commitCt), new Integer(closeCt));
        Logger.print(Logger.DEBUG, LOG_CATEGORY, DN + doneMsg);

        return status;
    }
}
