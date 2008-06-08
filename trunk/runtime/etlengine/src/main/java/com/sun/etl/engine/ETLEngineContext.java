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
 * @(#)ETLEngineContext.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.etl.engine.utils.MessageManager;
import com.sun.sql.framework.utils.Logger;

/**
 * Engine context.
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public class ETLEngineContext {

    public static class CollabStatistics {

        class TableData {
            private int executionId;
            private long rowsExtracted;
            private long rowsInserted;
            private long rowsRejected;
            private String tableName;
            private Timestamp timeFinished;
            private Timestamp timeStarted;

            public TableData(String name) {
                tableName = name;
            }

            public int getExecutionId() {
                return executionId;
            }

            public long getRowsExtracted() {
                return rowsExtracted;
            }

            public long getRowsInserted() {
                return rowsInserted;
            }

            public long getRowsRejected() {
                return rowsRejected;
            }

            public String getTableName() {
                return tableName;
            }

            public Timestamp getTimeFinished() {
                return timeFinished;
            }

            public Timestamp getTimeStarted() {
                return timeStarted;
            }

            public void setExecutionId(int newId) {
                executionId = newId;
            }

            public void setRowsExtracted(long newCt) {
                rowsExtracted = newCt;
            }

            public void setRowsInserted(long newCt) {
                rowsInserted = newCt;
            }

            public void setRowsRejected(long newCt) {
                rowsRejected = newCt;
            }

            public void setTimeFinished(Timestamp newTime) {
                timeFinished = newTime;
            }

            public void setTimeStarted(Timestamp newTime) {
                timeStarted = newTime;
            }
        }

        private Timestamp collabFinish;
        private Timestamp collabStart;
        private Map tableStats = new HashMap(3);

        public CollabStatistics() {
        }

        public void collabStarted() {
            if (collabStart == null) {
                collabStart = new Timestamp(System.currentTimeMillis());
            }
        }

        public void collabStopped() {
            if (collabFinish == null) {
                collabFinish = new Timestamp(System.currentTimeMillis());
            }
        }

        public Timestamp getCollabFinishTime() {
            return (collabFinish != null) ? collabFinish : new Timestamp(Long.MIN_VALUE);
        }

        public Timestamp getCollabStartTime() {
            return (collabStart != null) ? collabStart : new Timestamp(Long.MIN_VALUE);
        }

        public Collection getKnownTableNames() {
            return new HashSet(tableStats.keySet());
        }

        public long getRowsExtractedCount(String tableName) {
            TableData stats = getOrCreateTableStatistics(tableName);
            return stats.getRowsExtracted();
        }

        public long getRowsInsertedCount(String tableName) {
            TableData stats = getOrCreateTableStatistics(tableName);
            return stats.getRowsInserted();
        }

        public long getRowsRejectedCount(String tableName) {
            TableData stats = getOrCreateTableStatistics(tableName);
            return stats.getRowsRejected();
        }

        public int getTableExecutionId(String tableName) {
            TableData stats = getOrCreateTableStatistics(tableName);
            return stats.getExecutionId();
        }

        public Timestamp getTableFinishTime(String tableName) {
            TableData stats = getOrCreateTableStatistics(tableName);
            return stats.getTimeFinished();
        }

        public Timestamp getTableStartTime(String tableName) {
            TableData stats = getOrCreateTableStatistics(tableName);
            return stats.getTimeStarted();
        }

        public void setRowsExtractedCount(String tableName, long newCt) {
            TableData stats = getOrCreateTableStatistics(tableName);
            stats.setRowsExtracted(newCt);
        }

        public void setRowsInsertedCount(String tableName, long newCt) {
            TableData stats = getOrCreateTableStatistics(tableName);
            stats.setRowsInserted(newCt);
        }

        public void setRowsRejectedCount(String tableName, long newCt) {
            TableData stats = getOrCreateTableStatistics(tableName);
            stats.setRowsRejected(newCt);
        }

        public void setTableExecutionId(String tableName, int newId) {
            TableData stats = getOrCreateTableStatistics(tableName);
            stats.setExecutionId(newId);
        }

        public void setTableFinishTime(String tableName, Timestamp newTime) {
            TableData stats = getOrCreateTableStatistics(tableName);
            stats.setTimeFinished(newTime);
        }

        public void setTableStartTime(String tableName, Timestamp newTime) {
            TableData stats = getOrCreateTableStatistics(tableName);
            stats.setTimeStarted(newTime);
        }

        private TableData getOrCreateTableStatistics(String tableName) {
            TableData stats = (TableData) tableStats.get(tableName);
            if (stats == null) {
                stats = new TableData(tableName);
                tableStats.put(tableName, stats);
            }
            return stats;
        }
    }

    /* Key constant corresponding to runtime output value */
    public static final String KEY_ERROR_MESSAGE = "errorMessage";

    /* Key constant corresponding to runtime output value */
    public static final String KEY_RUNTIME_OUTPUTS = "runtimeOutputs";

    /** List of Connection instances to be committed. */
    private List connCommitList = null;

    /** Set of extractor Connection instances to be closed. */
    private Set connectionsToClose = null;

    /**
     * context object accross the tasks, put anything you want the engine to remember in
     * the life cycle of the activity
     */
    private HashMap context = null;

    /** * Runtime context for this class. */
    private final String logCategory = ETLEngineContext.class.getName();

    private CollabStatistics statistics;

    /** List of Thwoables caught during engine execution */
    private List throwableList = null;

    /**
     * Constructor for the ETLEngineContext object
     */
    public ETLEngineContext() {
        context = new HashMap();
        statistics = new CollabStatistics();

        connCommitList = new ArrayList();
        connectionsToClose = new HashSet(5);
        throwableList = new ArrayList();
    }

    /**
     * Adds given Throwable to list of Throwables kept in this context.
     * 
     * @param t
     */
    public void addToThrowableList(Throwable t) {
        throwableList.add(t);
    }

    /**
     * Clears current contents of list of Throwables kept in this context.
     */
    public void clearThrowableList() {
        throwableList.clear();
    }

    /**
     * Adds the given Connection to the collection of instances to be shutdown and closed
     * at some later time.
     * 
     * @param aConn Connection to be committed and closed at some later time
     * @see #getConnectionListToCommit
     */
    public synchronized void closeAndReleaseLater(Connection aConn) {
        connectionsToClose.add(aConn);
    }

    /**
     * Adds the given Connection to the collection of instances to be committed and closed
     * at some later time.
     * 
     * @param aConn Connection to be committed and closed at some later time
     * @see #getConnectionListToCommit
     */
    public void commitLater(Connection aConn) {
        connCommitList.add(aConn);
    }

    /**
     * Clean up the engine context.
     */
    public synchronized void flush() {
        context.clear();
    }

    /**
     * Gets the allKeys attribute of the ETLEngineContext class
     * 
     * @return The allKeys value
     */
    public String[] getAllParams() {
        Set keyset = context.keySet();
        return (String[]) keyset.toArray();
    }

    /**
     * Gets the List of Connection instances to be commited and closed, and zeroes out the
     * list held in this context.
     * 
     * @return List of Connection instances.
     */
    public List getConnectionListToCommit() {
        List theList = new ArrayList(connCommitList);
        connCommitList.clear();

        return theList;
    }

    /**
     * Gets the Collection of Connection instances to be shut down and closed, zeroing out
     * the local copy held in the context.
     * 
     * @return Collection of Connection instances to shut down and close.
     */
    public synchronized Collection getConnectionsToClose() {
        List theList = new ArrayList(connectionsToClose);
        connectionsToClose.clear();
        return theList;
    }

    public CollabStatistics getStatistics() {
        return statistics;
    }

    /**
     * Gets List of current Throwables kept in this context. Typically used to keep track
     * of exceptions that are caught and handled during engine execution.
     * 
     * @return List (possibly empty) of Throwables
     */
    public List getThrowableList() {
        return new ArrayList(throwableList);
    }

    /**
     * Gets the value attribute of the ETLEngineContext class
     * 
     * @param key key for lookup.
     * @return The value value
     */
    public Object getValue(String key) {
        return context.get(key);
    }

    /**
     * Binds key and value into the context.
     * 
     * @param key key
     * @param value value
     */
    public synchronized void putValue(String key, Object value) {
        context.put(key, value);
    }

    /**
     * Sets the context attribute of the ETLEngineContext class
     * 
     * @param theState The new context value
     */
    public synchronized void setContext(HashMap theState) {
        // after context has some value force user to call flush
        // to make sure he really want to do putAll...
        if (context.isEmpty()) {
            context.putAll(theState);
        } else {
            MessageManager messageManager = MessageManager.getManager(ETLEngineContext.class);
            String err = messageManager.getString("ERR_ENGINE_CONTEXT");
            Logger.print(Logger.ERROR, logCategory, err);
        }
    }
}
