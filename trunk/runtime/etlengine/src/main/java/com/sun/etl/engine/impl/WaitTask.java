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
 * @(#)WaitTask.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;

/**
 * Keeps track of return states of dependent tasknodes and determines its successor path
 * based on those states.
 * 
 * @author Jonathan Giron
 * @version 
 */
public class WaitTask extends SimpleTask {

    /* Set containing task IDs of active tasks still outstanding */
    private Set mDependentTasks;

    /* Set containing task IDs of successful failed tasks */
    private Set mFailedTasks;

    /* Set containing task IDs of successful dependent tasks */
    private Set mSuccessfulTasks;

    /**
     * Constructs a default instance of WaitTask.
     */
    public WaitTask() {
        mSuccessfulTasks = new HashSet();
        mFailedTasks = new HashSet();
        mDependentTasks = new HashSet();
    }

    /**
     * Indicates whether this task still has outstanding active dependencies.
     * 
     * @return true if active dependencies are outstanding; false otherwise
     */
    public boolean hasActiveDependencies() {
        Logger.print(Logger.DEBUG, WaitTask.class.getName(), "Active dependency count: " + mDependentTasks.size());
        return (mDependentTasks.size() != 0);
    }

    /**
     * Marks the given dependent task ID as having failed.
     * 
     * @param nodeId name of dependent task ID to be marked as having failed.
     */
    public void markAsFailure(String nodeId) {
        if (mDependentTasks.remove(nodeId)) {
            Logger.print(Logger.DEBUG, WaitTask.class.getName(), nodeId + " marked as failed." + NL + "Remaining dependencies: "
                + mDependentTasks.size());
            mFailedTasks.add(nodeId);
        }
    }

    /**
     * Marks the given dependent task ID as successful.
     * 
     * @param nodeId name of dependent task ID to be marked as successful.
     */
    public void markAsSuccess(String nodeId) {
        if (mDependentTasks.remove(nodeId)) {
            Logger.print(Logger.DEBUG, WaitTask.class.getName(), nodeId + " marked as successful." + NL + "Remaining dependencies: "
                + mDependentTasks.size());
            mSuccessfulTasks.add(nodeId);
        }
    }

    /**
     * Overrides parent implementation to determine whether a sufficient number of
     * dependent tasks have succeeded to direct execution flow to the success flowpath or
     * exception flowpath.
     * 
     * @param node ETLTaskNode to use in processing this task
     * @return SUCCESS if thread should proceed down the success flowpath; EXCEPTION if
     *         thread should proceed down the exception flowpath
     * @see com.sun.etl.engine.ETLTask#process(com.sun.etl.engine.ETLTaskNode)
     * @see com.sun.etl.engine.ETLTask#SUCCESS
     * @see com.sun.etl.engine.ETLTask#EXCEPTION
     */
    public String process(ETLTaskNode node) throws ETLException {
        if (!StringUtil.isNullString(node.getDisplayName())) {
            DN += " <" + node.getDisplayName().trim() + ">";
        }

        final boolean dependentTasksSucceeded = (mDependentTasks.isEmpty() && mFailedTasks.isEmpty());
        if (!dependentTasksSucceeded) {
            List idList = new ArrayList(mFailedTasks);
            Collections.sort(idList);
            String failedTaskIds = StringUtil.createDelimitedStringFrom(idList);
            MessageManager msgMgr = MessageManager.getManager(WaitTask.class);
            if (failedTaskIds.trim().length() == 0) {
                failedTaskIds = msgMgr.getString("MSG_common_unknown");
            }

            String msg = msgMgr.getString("MSG_wait_failed_dependencies", failedTaskIds);
            Logger.print(Logger.DEBUG, WaitTask.class.getName(), DN + msg);
            node.fireETLEngineLogEvent(msg);
        }
        return dependentTasksSucceeded ? SUCCESS : EXCEPTION;
    }

    /**
     * Sets list of dependent task IDs using the given Collection.
     * 
     * @param taskIds Collection of Strings representing dependent task IDs
     */
    public void setDependentTaskIds(Collection taskIds) {
        mDependentTasks = new HashSet(taskIds);
    }

    /**
     * Sets list of dependent task IDs from the given String of comma-separated values.
     * 
     * @param csvIds String containing comma-separated names of dependent task IDs
     */
    public void setDependentTaskIds(String csvIds) {
        setDependentTaskIds(StringUtil.createStringListFrom(csvIds));
    }
}
