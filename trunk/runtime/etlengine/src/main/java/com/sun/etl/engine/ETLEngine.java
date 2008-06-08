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
 * @(#)ETLEngine.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine;

import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

import com.sun.sql.framework.exception.BaseException;

/**
 * This interface contains all the methods that will be implemented by ETLEngineImpl
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public interface ETLEngine {

    /** Cleanup task constant. */
    public static final String CLEANUP = "Cleanup";

    /** Commit task constant. */
    public static final String COMMIT = "Commit";

    /** Constant for END task. */
    public static final String END = "STOP";

    /** Extractor for basic mode :Default Mode */
    public static final String EXTRACTOR = "Extractor";
    public static final String LOADER = "Loader";
    public static final String INIT = "INIT";

    /** Constant for START task. */
    public static final String START = "START";

    /** Status code indicating that collaboration processing has started. */
    public static final int STATUS_COLLAB_STARTED = 0;

    /** Status code indicating that collaboration processing has completed. */
    public static final int STATUS_COLLAB_COMPLETED = 1;
    
    /** Status code indicating Collaboration execution exception */
    public static final int STATUS_COLLAB_EXCEPTION = 2;
    
    /** Status code indicating that activity/target table processing has started. */
    public static final int STATUS_ACTIVITY_STARTED = 3;

    /** Status code indicating that activity/target table processing has completed. */
    public static final int STATUS_ACTIVITY_COMPLETED = 4;
    
    /** Status code indicating that activity/target table processing has completed with exception. */
    public static final int STATUS_ACTIVITY_EXCEPTION = 5;

    
    /** TAB constant. */
    public static final String TAB = "\t";

    /** Pipeline task constant. */
    public static final String PIPELINE = "Pipeline";
    
    /** Pipeline task constant. */
    public static final String VALIDATING = "Validating";

    /** Transformer task constant. */
    public static final String TRANSFORMER = "Transformer";

    /** Task to executed linked and related queries **/
    public static final String CORRELATED_QUERY_EXECUTOR = "CorrelatedQueryExecutor";

    /** Update Statistics task constant. */
    public static final String UPDATE_STATS = "UpdateStatistics";

    /** Constant for WAIT task. */
    public static final String WAIT = "WAIT";
    
    /** Null task constant. */
    public static final String NULL = "NULL";

    /**
     * Create end task node.
     * 
     * @return true if successful. false if failed.
     */
    public boolean createEndETLTaskNode();

    /**
     * Creates task node of the given type.
     * 
     * @param taskType String representing task node type.
     * @return new ETLTaskNode instance of given type
     */
    public ETLTaskNode createETLTaskNode(String taskType);

    /**
     * Create start task node.
     * 
     * @return true if successful. false if failed.
     */
    public boolean createStartETLTaskNode();

    /**
     * execute the tasks by invoking process() method.
     * 
     * @param execListener Which implements ETLEngineExecListener
     * @return an integer
     */
    public int exec(ETLEngineListener execListener);

    /**
     * Generate a unique id for the tasknode
     * 
     * @param prefix is used to make it more redable.
     */
    public String generateId(String prefix);

    /**
     * get connection definition list associated with this engine
     */
    public List getConnectionDefList();

    /**
     * Gets the associated context of the engine instance
     * 
     * @return The context value
     */
    public ETLEngineContext getContext();

    /**
     * get display name of the engine.
     */
    public String getDisplayName();

    /**
     * Get the end ETLTaskNode
     * 
     * @return ETLTaskNode The end ETLTaskNode value
     */
    public ETLTaskNode getEndETLTaskNode();

    /**
     * @return ETLEngineListener
     */
    public ETLEngineListener getETLEngineListener();

    /**
     * Get the taskNode of a given taskId
     * 
     * @param taskId Task Id
     * @return ETLTaskNode The taskNode value
     */
    public ETLTaskNode getETLTaskNode(String taskId);

    /**
     * Get the Full qualified impl class name of a given task type
     * 
     * @param taskType Task Type
     * @return The classRef value
     */
    public String getFullQualifiedImplClassName(String taskType);

    /**
     * Get Runtime attributes as name and value to be passed as args to other modules
     * 
     * @return Map
     */
    public Map getInputAttributes();

    /**
     * Gets read-only copy of current map of runtime input arguments.
     * 
     * @return Map of runtime input arguments
     */
    public Map getInputAttrMap();

    /**
     * Indicates whether engine is running on an app server.
     * 
     * @param flag true if engine is running on app server, false otherwise
     */
    public boolean isRunningOnAppServer();
    
    /**
     * Sets whether engine is running on an app server, as opposed to a development workstation.
     * 
     * @param newValue true if running on app server, false otherwise
     */
    public void setRunningOnAppServer(boolean newValue);

    /**
     * Getter for Runtime Output arguments
     * 
     * @return Collection of runtime output arguments
     */
    public Map getRuntimeOutputArguments();

    /**
     * Get the Start ETLTaskNode
     * 
     * @return ETLTaskNode The Start ETLTaskNode value
     */
    public ETLTaskNode getStartETLTaskNode();

    /**
     * Parse the XML element to build Engine
     * 
     * @param element XML form of engine tasks
     * @throws com.sun.sql.framework.exception.BaseException if error.
     */
    public void parseXML(Element element) throws BaseException;

    /**
     * set connection definition list associated with this engine
     */
    public void setConnectionDefList(List conDefs);

    /**
     * Sets the context of the engine
     * 
     * @param engineContext The context value
     */
    public void setContext(ETLEngineContext engineContext);

    /**
     * set display name of the engine.
     */
    public void setDisplayName(String theDisplayName);

    /**
     * Sets collection of runtime input arguments to the contents of the given Map.
     * 
     * @param newAttrs Map of Attributes representing runtime input arguments
     */
    public void setInputAttrMap(Map newAttrs);

    /**
     * Sets collection of runtime output arguments to the contents of the given Map.
     * 
     * @param newAttrs Map of Attributes representing runtime output arguments
     */
    public void setOutputAttrMap(Map newAttrs);

    /**
     * Setter for Overriding runtime values
     * 
     * @param msg ETLPersistableMessage containing input values to be set
     */
    public void setOverrideMap(ETLPersistableMessage msg);

    /**
     * Starts ETLEngine
     */
    public void start();

    /**
     * Stop the ETLEngine using taskmanager
     */
    public void stopETLEngine();

    /**
     * Gets the XML String for Engine
     * 
     * @return String XML form of engine tasks
     */
    public String toXMLString();

}
