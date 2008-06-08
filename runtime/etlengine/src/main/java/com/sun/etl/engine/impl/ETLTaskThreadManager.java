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
 * @(#)ETLTaskThreadManager.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.sun.etl.engine.ETLEngineExecEvent;
import com.sun.etl.engine.ETLEngineListener;

/**
 * Manager for task thread.
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public class ETLTaskThreadManager {

    /** * If a Task is active */
    private boolean active = true;

    /** * keep track of resume calls to start thread */
    private ETLEngineListener execListener;

    /** * name */
    private String name;

    /** Resume counter */
    private int resumeCounter = 0;

    /** Start shoold wait on this Object */
    private Object startThreadWait = new Object();

    /** usually activity name that owns this object */
    private HashMap taskDependents = new HashMap();

    /**
     * Constructor for the ETLTaskThreadManager object
     * 
     * @param name Name of the task thread manager
     */
    public ETLTaskThreadManager(String name) {
        this.name = name;
    }

    /**
     * Adds a feature to the ExecListener attribute of the ETLTaskThreadManager object
     * 
     * @param listener The feature to be added to the ExecListener attribute
     */
    public void addEngineListener(ETLEngineListener listener) {
        this.execListener = listener;
    }

    /**
     * Adds the task associated with the given task name as a dependency of the given
     * WaitTask.
     * 
     * @param taskName task name to be added as a dependency of <code>waitTask</code>.
     * @param waitTask WaitTask with which to associate <code>taskName</code> as a
     *        dependency.
     */
    public synchronized void addWaitDependency(String taskName, WaitTask waitTask) {
        taskDependents.put(taskName, waitTask);
    }

    /**
     * Remove dependents for this given task.
     * 
     * @param taskName Task name
     */
    public synchronized void clearTaskThread(String taskName) {
        taskDependents.remove(taskName);
    }

    /**
     * Fires an ETLEngineExecEvent to the associated listener, indicating the given
     * execution status and passing a generic Exception with the given message.
     * 
     * @param status execution status, one of ETLEngine.STATUS_COLLAB_COMPLETED or
     *        ETLEngine.STATUS_COLLAB_EXCEPTION
     * @param msg message to include as a generated Exception within the
     *        ETLEngineExecEvent
     * @see com.sun.etl.engine.ETLEngine#STATUS_COLLAB_COMPLETED
     * @see com.sun.etl.engine.ETLEngine#STATUS_COLLAB_EXCEPTION
     */
    public void fireETLEngineExecEvent(int status, String msg) {
        if (execListener != null) {
            execListener.executionPerformed(new ETLEngineExecEvent(name, status, new Exception(msg)));
        }
    }

    /**
     * Notify execution listener.
     * 
     * @param status Status
     * @param message Message to the listener
     */
    public void fireETLEngineExecEvent(int status, Throwable cause) {
        if (execListener != null) {
            execListener.executionPerformed(new ETLEngineExecEvent(name, status, cause));
        }
    }

    /**
     * Gets the ETLEngineExecListener attribute of the ETLTaskThreadManager object
     * 
     * @return ETLEngineExecListener value
     */
    public ETLEngineListener getEngineListener() {
        return execListener;
    }

    /**
     * Gets the active attribute of the ETLTaskThreadManager object
     * 
     * @return The active value
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Gets the dependentExists attribute of the ETLTaskThreadManager object
     * 
     * @param taskName Task name.
     * @return The dependentExists value
     */
    public synchronized boolean isDependentExists(String taskName) {
        Object dependencyTracker = taskDependents.get(taskName);
        if (dependencyTracker instanceof List) {
            List dependentList = (List) dependencyTracker;
            if (dependentList == null || dependentList.isEmpty()) {
                return false;
            }
        } else if (dependencyTracker instanceof WaitTask) {
            WaitTask wait = (WaitTask) dependencyTracker;
            return wait.hasActiveDependencies();
        }
        return true;
    }

    /**
     * Gets the taskThreadExists attribute of the ETLTaskThreadManager object
     * 
     * @param taskName Task name
     * @return The taskThreadExists value
     */
    public synchronized boolean isTaskThreadExists(String taskName) {
        Set taskSet = taskDependents.keySet();
        return taskSet.contains(taskName);
    }

    /**
     * Notifies the WaitTask associated with the given task name that the task associated
     * with the given dependent task name has failed.
     * 
     * @param taskName Task name used to locate associated WaitTask, if any.
     * @param dependentName Dependent task name to mark in WaitTask as having failed
     */
    public synchronized void markDependencyAsFailed(String taskName, String dependentName) {
        Object dependencyTracker = taskDependents.get(taskName);
        if (dependencyTracker instanceof WaitTask) {
            WaitTask wait = (WaitTask) dependencyTracker;
            wait.markAsFailure(dependentName);
        }
    }

    /**
     * Notifies the WaitTask associated with the given task name that the task associated
     * with the given dependent task name has succeeded.
     * 
     * @param taskName Task name used to locate associated WaitTask, if any.
     * @param dependentName Dependent task name to mark in WaitTask as having succeeded
     */
    public synchronized void markDependencyAsSucceeded(String taskName, String dependentName) {
        Object dependencyTracker = taskDependents.get(taskName);
        if (dependencyTracker instanceof WaitTask) {
            WaitTask wait = (WaitTask) dependencyTracker;
            wait.markAsSuccess(dependentName);
        }
    }

    /**
     * Removes the wait dependency associated with the given task name.
     * 
     * @param taskName task name whose WaitTask, if any, will be dissociated from it
     * @return true if a WaitTask was associated with <code>taskName</code> and removed;
     *         false otherwise
     */
    public synchronized boolean removeWaitDependency(String taskName) {
        return (taskDependents.remove(taskName) != null);
    }

    /**
     * Resumes the suspended START task thread. Usually called when END task is
     * encountered or during exception
     */
    public void resumeStartThread() {
        synchronized (startThreadWait) {
            if (resumeCounter < 0) {
                //-- start thread is already suspended...notify it
                startThreadWait.notify();
            } else {
                resumeCounter++;
            }
        }
    }

    /**
     * Sets the ETLEngineExecListener attribute of the ETLTaskThreadManager object
     * 
     * @param listener engine exec listener
     */
    public void setEngineListener(ETLEngineListener listener) {
        execListener = listener;
    }

    /**
     * Stop engine.
     */
    public void stopETLEngine() {
        active = false;
    }

    /**
     * Should be run from with-in START task thread. For each call to suspend, all the
     * previous calls to resume will be cleared.
     */
    public void suspendStartThread() {
        synchronized (startThreadWait) {
            if (resumeCounter > 0) {
                //-- resume was already called, so don't wait
                resumeCounter = 0;
            } else {
                //-- clear all previous calls to Resume
                resumeCounter--;
                try {
                    startThreadWait.wait();
                } catch (InterruptedException int_exp) {
                    //Ignore
                }
                resumeCounter = 0;
                //-- clear all previous calls to Resume
            }
        }
    }

}
