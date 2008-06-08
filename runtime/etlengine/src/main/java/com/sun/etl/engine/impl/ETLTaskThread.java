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
 * @(#)ETLTaskThread.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.TimerTask;

import com.sun.etl.engine.ETLEngine;
import com.sun.etl.engine.ETLEngineContext;
import com.sun.etl.engine.ETLTask;
import com.sun.etl.engine.ETLTaskNode;
import com.sun.etl.engine.utils.ETLException;
import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.utils.Logger;
import com.sun.sql.framework.utils.StringUtil;

/**
 * Extends Timer Task and does the actual work of going through tasks and processing task
 * nodes.
 * 
 * @author Ahimanikya Satapathy
 * @version 
 */
public class ETLTaskThread extends TimerTask {

    /* Constant value indicating end task */
    private static final int END_TASK = 12;

    private static MessageManager messageManager = ETLEngineImpl.getPackageMessageManager();

    /* Constant value indicating start task. */
    private static final int START_TASK = 11;

    /* Constant value indicating user task. */
    private static final int USER_TASK = 20;

    /* Constant value indicating join task. */
    private static final int WAIT_TASK = 13;

    /* Runtime context of this class. */
    private final String LOG_CATEGORY = ETLTaskThread.class.getName();

    /* ETLTask object being controlled by this thread. */
    private ETLTask task;

    /* Name of this task thread. */
    private String taskId;

    /* ETL Task manager object. */
    private ETLTaskThreadManager taskManager;

    /* Reference to a task node. */
    private ETLTaskNode taskNode;

    /* Type of this task. */
    private int taskType;

    /**
     * Constructor for the TaskThread object
     * 
     * @param theTaskManager The TaskThreadManager for current Task Thread
     * @param theETLTaskNode Current node to be processed
     * @exception com.sun.sql.framework.exception.BaseException Exception that is thrown
     */
    public ETLTaskThread(ETLTaskThreadManager theTaskManager, ETLTaskNode theETLTaskNode) throws ETLException {
        init(theTaskManager, theETLTaskNode);
        new Thread(this, taskId).start();
    }

    /**
     * Gets the name attribute of the TaskThread object
     * 
     * @return The name value
     */
    public String getName() {
        return taskId;
    }

    public ETLTask getTask() {
        return task;
    }

    /**
     * Gets the taskType attribute of the TaskThread object
     * 
     * @param categoryName The category to get the task Type
     * @return The taskType value
     */
    public int getTaskType(String categoryName) {
        int aTaskType;
        if (categoryName.equals(ETLEngine.START)) {
            aTaskType = START_TASK;
        } else if (categoryName.equals(ETLEngine.END)) {
            aTaskType = END_TASK;
        } else if (categoryName.equals(ETLEngine.WAIT)) {
            aTaskType = WAIT_TASK;
        } else {
            aTaskType = USER_TASK;
        }
        return aTaskType;
    }

    /**
     * Main processing method for the TaskThread object
     */
    public void run() {
        if (taskType == WAIT_TASK) {
            synchronized (taskManager) {
                while (taskManager.isDependentExists(getName())) {
                    try {
                        if (taskManager.isActive()) {
                            taskManager.wait();
                            //-- wait for the dependent task to finish
                        } else {
                            return;
                        }
                    } catch (InterruptedException int_exp) {
                        continue;
                    }
                }

                taskManager.removeWaitDependency(getName());
            }
            //-- at this point, all the dependent tasks are processed
        }

        String situation = null;
        try {
            situation = task.process(taskNode);
        } catch (Exception prcs_exp) {
            String err = messageManager.getString("ERR_PROCESING_TASK", getName());
            Logger.print(Logger.ERROR, LOG_CATEGORY, err);

            handleException(task, taskNode, prcs_exp);
            situation = ETLTask.EXCEPTION;
        } finally {
            task.cleanUp();
        }

        if (taskType == START_TASK) {
            String infoMsg = messageManager.getString("INFO_MSG_ENGINE_START");
            Logger.print(Logger.INFO, LOG_CATEGORY, infoMsg);
            ETLEngineContext context = taskNode.getContext();
            if (context != null) {
                // XXX Shouldn't we have a lightweight START task that has this code in
                // its process() method?
                ETLEngineContext.CollabStatistics stats = context.getStatistics();
                if (stats != null) {
                    stats.collabStarted();
                }
            } else {
                Logger.print(Logger.ERROR, LOG_CATEGORY, "" + "Could not locate statistics object - start timestamp is not set.");
            }
        } else if (taskType == WAIT_TASK) {
            taskManager.clearTaskThread(getName());
        }

        String nextTaskParam = taskNode.getNextTaskList(situation);
        if (nextTaskParam == null) {
            String errMsg = messageManager.getString("ERR_ILLEGAL_NEXT_TASK", getName());
            Logger.print(Logger.ERROR, LOG_CATEGORY, errMsg);
            handleException(task, taskNode, new Throwable(errMsg));
            taskManager.fireETLEngineExecEvent(ETLEngine.STATUS_COLLAB_EXCEPTION, errMsg);
            taskManager.setEngineListener(null);
            return;
        }

        ArrayList nextTaskList = (ArrayList) StringUtil.createStringListFrom(nextTaskParam);

        ETLTaskNode nextETLTaskNode;
        String nextTaskId;
        String categoryName;
        int nextTaskType;
        ETLTaskThread nextTask;
        for (int i = 0; i < nextTaskList.size(); i++) {
            nextTaskId = (String) nextTaskList.get(i);
            nextETLTaskNode = taskNode.getParent().getETLTaskNode(nextTaskId);
            categoryName = nextETLTaskNode.getTaskType();
            nextTaskType = getTaskType(categoryName);

            try {
                if (nextTaskType == END_TASK) {
                    // XXX Shouldn't we have a lightweight END task that has this code in
                    // its process() method?
                    ETLEngineContext context = taskNode.getContext();
                    if (context != null) {
                        ETLEngineContext.CollabStatistics stats = context.getStatistics();
                        if (stats != null) {
                            stats.collabStopped();
                        }
                    }

                    //-- Assuming that there can only be one task referring
                    //   to END task
                    taskManager.resumeStartThread();

                    if (taskType != START_TASK && taskType != WAIT_TASK) {
                        //-- since there can be no other tasks
                        //   other than END task in nextTaskList,
                        //   we can just return
                        return;
                    }
                } else if (nextTaskType == WAIT_TASK) {
                    //-- remove this task from the dependent list of JOIN task
                    synchronized (taskManager) {
                        if (!taskManager.isTaskThreadExists(nextTaskId)) {
                            if (taskManager.isActive()) {
                                nextTask = new ETLTaskThread(taskManager, nextETLTaskNode);
                                if (nextTask == null) {
                                    throw new RuntimeException("Problem in executing eTL engine...");
                                }
                            } else {
                                taskManager.notifyAll();
                            }
                        }

                        if (ETLTask.SUCCESS.equals(situation)) {
                            taskManager.markDependencyAsSucceeded(nextTaskId, getName());
                        } else {
                            taskManager.markDependencyAsFailed(nextTaskId, getName());
                        }
                        taskManager.notifyAll();
                    }
                } else {
                    if (taskManager.isActive()) {
                        nextTask = new ETLTaskThread(taskManager, nextETLTaskNode);
                    }
                }
            } catch (Exception exp) {
                String err = messageManager.getString("ERR_TASK_THREAD_PROCESSING", exp.toString());
                Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, null, err, exp);

                handleException(task, taskNode, exp);
                taskManager.fireETLEngineExecEvent(ETLEngine.STATUS_COLLAB_EXCEPTION, err);
                taskManager.setEngineListener(null);
            }
        }

        if (taskType == START_TASK) {
            //-- suspend START task thread to be woken up later by END task
            //   thread
            taskManager.suspendStartThread();

            //-- wake up and do things needed for END task
            String infoMsg = messageManager.getString("INFO_ETL_ENGINE_END");
            Logger.print(Logger.INFO, LOG_CATEGORY, infoMsg);

            //String errorMessage = null;
            Iterator throwableIter = Collections.EMPTY_LIST.iterator();
            ETLEngineContext context = taskNode.getContext();
            if (context != null) {
                throwableIter = context.getThrowableList().iterator();
            }

            if (throwableIter.hasNext()) {
                do {
                    taskManager.fireETLEngineExecEvent(ETLEngine.STATUS_COLLAB_EXCEPTION, (Throwable) throwableIter.next());
                } while (throwableIter.hasNext());
            } else {
                taskManager.fireETLEngineExecEvent(ETLEngine.STATUS_COLLAB_COMPLETED, infoMsg);
            }
            taskManager.setEngineListener(null);
        }
    }

    /**
     * Method handles Exception
     * 
     * @param aTask ETL Task
     * @param curETLTaskNode current task node
     * @param e exceptions
     * @return int
     */
    private int handleException(ETLTask aTask, ETLTaskNode curETLTaskNode, Throwable e) {
        try {
            if (task != null) {
                aTask.handleException(e instanceof ETLException ? (ETLException) e : new ETLException(e));
            }

            if (e instanceof SQLException) {
                SQLException chainedSqlEx = ((SQLException) e).getNextException();
                if (chainedSqlEx != null) {
                    Logger.printThrowable(Logger.DEBUG, LOG_CATEGORY, this, "Chained SQLException:", chainedSqlEx);
                }
            }

            if (curETLTaskNode != null) {
                // ETLException is typically wrapped around an Exception or Throwable from
                // a different package or library - the nested exception should be treated
                // as the exception to be displayed and logged.
                Throwable unwrappedException = (e instanceof ETLException) ? e.getCause() : e;
                if (unwrappedException == null) {
                    unwrappedException = e;
                }
                curETLTaskNode.getContext().putValue(
                    ETLEngineContext.KEY_ERROR_MESSAGE,
                    System.getProperty("line.separator", "\n") + unwrappedException.getMessage() + " [while executing " + curETLTaskNode.getId()
                        + "]");
                curETLTaskNode.getContext().addToThrowableList(unwrappedException);

                // Per JDK spec we should be rethrowing ThreadDeath - apps should NOT
                // swallow
                // this Error, otherwise the thread being killed will not die.
                if (e instanceof ThreadDeath) {
                    throw (ThreadDeath) e;
                }
                
                String nodeHandle = curETLTaskNode.getNextTaskList(ETLTask.EXCEPTION);
                if (nodeHandle == null) {
                    Logger.print(Logger.DEBUG, LOG_CATEGORY, this, "Missing next task for exception path!");
                    return 0;
                }
            }
        } catch (Exception uex) {
            Logger.printThrowable(Logger.ERROR, LOG_CATEGORY, null, "While executing handleException():", uex);
        }
        return -1;
    }

    private void init(ETLTaskThreadManager theTaskManager, ETLTaskNode theETLTaskNode) throws ETLException {

        taskId = theETLTaskNode.getId();
        taskManager = theTaskManager;
        taskNode = theETLTaskNode;
        String categoryName = taskNode.getTaskType();
        taskType = getTaskType(categoryName);

        String className = taskNode.getParent().getFullQualifiedImplClassName(categoryName);
        Class taskClass;
        try {
            if (className != null) {
                taskClass = Class.forName(className, true, getClass().getClassLoader());
                task = (ETLTask) taskClass.newInstance();
            } else {
                String err = messageManager.getString("ERR_CLASS_NOT_INTASK_MAP", categoryName);
                throw new ETLException(err);
            }
        } catch (InstantiationException ie) {
            ie.printStackTrace();
            String errMsg = messageManager.getString("EX_INIT_TASK", className, ie);
            Logger.print(Logger.DEBUG, LOG_CATEGORY, errMsg);
        } catch (IllegalAccessException iae) {
            iae.printStackTrace();
            String errMsg = messageManager.getString("EX_INIT_TASK", className, iae);
            Logger.print(Logger.DEBUG, LOG_CATEGORY, errMsg);
        } catch (ClassNotFoundException cnfe) {
            cnfe.printStackTrace();
            String errMsg = messageManager.getString("EX_INIT_TASK", className, cnfe);
            Logger.print(Logger.DEBUG, LOG_CATEGORY, errMsg);
        }

        if (taskType == WAIT_TASK) {
            ArrayList dependentTaskList = new ArrayList();
            String dependsStr = taskNode.getDependsOn();
            StringTokenizer taskList = new StringTokenizer(dependsStr, ",");
            while (taskList.hasMoreElements()) {
                dependentTaskList.add(taskList.nextToken());
            }
            // taskManager.addDependentTasks(getName(), dependentTaskList);
            WaitTask waitTask = (WaitTask) task;
            waitTask.setDependentTaskIds(dependsStr);
            taskManager.addWaitDependency(getName(), waitTask);
        }
    }
}
