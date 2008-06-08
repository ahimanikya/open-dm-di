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
 * @(#)ETLEngineExecEvent.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine;

/**
 * Execution event for engine
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public class ETLEngineExecEvent {

    private Throwable cause;

    /** * Error message, if any, produced by engine. */
    private String errorMessage;

    /** * Current execution status of the engine. */
    private int execStatus;

    /** * usually task id */
    private String sourceName;

    private String targetTableName;
    
    private String executionId;
    /**
     * Constructor for the ETLEngineExecEvent object
     * 
     * @param sourceName Source name
     * @param execStatus Execution status
     * @param errorMessage Error message
     */
    public ETLEngineExecEvent(String sourceName, int execStatus, String errorMessage) {
        this.sourceName = sourceName;
        this.execStatus = execStatus;
        this.errorMessage = errorMessage;
    }

    /**
     * Constructor for the ETLEngineExecEvent object
     * 
     * @param sourceName Source name
     * @param execStatus Execution status
     * @param theCause the Cause
     */
    public ETLEngineExecEvent(String sourceName, int execStatus, Throwable theCause) {
        this.sourceName = sourceName;
        this.execStatus = execStatus;
        this.errorMessage = theCause.getMessage();
        this.cause = theCause;
    }

    /**
     * Constructor for the ETLEngineExecEvent object
     * 
     * @param execStatus status
     * @param targetTableName table Name
     * @param executionId execution Id
     */
    public ETLEngineExecEvent(int execStatus, String targetTableName, String executionId) {
        this.execStatus = execStatus;
        this.targetTableName = targetTableName;
        this.executionId = executionId;
    }
    
    /**
     * @return the Cause.
     */
    public Throwable getCause() {
        return this.cause;
    }

    /**
     * Set the Cause.
     * @param cause
     */
    public void setCause(Throwable cause) {
        this.cause = cause;
    }
    
    /**
     * Gets the errorMessage attribute of the ETLEngineExecEvent object
     * 
     * @return The errorMessage value
     */
    public String getErrorMessage() {
        if (errorMessage == null) {
            return "";
        }
        return errorMessage;
    }

    /**
     * Sets the errorMessage attribute of the ETLEngineExecEvent object
     * 
     * @param errorMessage 
     */
    public void getErrorMessage(String errorMsg) {
    	this.errorMessage = errorMsg;
    }
    
    /**
     * Gets the sourceName attribute of the ETLEngineExecEvent object
     * 
     * @return The sourceName value
     */
    public String getSourceName() {
        return sourceName;
    }

    /**
     * Gets the status attribute of the ETLEngineExecEvent object
     * 
     * @return The status value
     */
    public int getStatus() {
        return execStatus;
    }

	public String getExecutionId() {
		return executionId;
	}

	public void setExecutionId(String executionId) {
		this.executionId = executionId;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

}
