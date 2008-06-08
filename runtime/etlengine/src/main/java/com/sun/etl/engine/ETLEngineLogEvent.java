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
 * @(#)ETLEngineLogEvent.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine;

import com.sun.sql.framework.utils.Logger;

/**
 * ETLEngine Status Event for engine
 * 
 * @author Ahimanikya Satapathy
 * @version :
 */
public class ETLEngineLogEvent {

    /** log level, defaults to INFO */
    private int logLevel = Logger.INFO;

    /** Log message, if any, produced by engine. */
    private String logMessage;

    /** usually task id */
    private String sourceName;

    /**
     * Constructs an instance of ETLEngineLogEvent with the given source ID and message.
     * 
     * @param sourceName Source name
     * @param logMessage Error message
     */
    public ETLEngineLogEvent(String sourceName, String logMessage) {
        this.sourceName = sourceName;
        this.logMessage = logMessage;
    }

    /**
     * Constructs an instance of ETLEngineLogEvent with the given source ID, message, and
     * level.
     * 
     * @param source ID of source that generated this log message
     * @param message message to enter in the log
     * @param level one of Logger.DEBUG, Logger.INFO, Logger.WARN, Logger.ERROR, or
     *        Logger.FATAL
     * @see com.sun.sql.framework.utils.Logger#DEBUG
     * @see com.sun.sql.framework.utils.Logger#INFO
     * @see com.sun.sql.framework.utils.Logger#WARN
     * @see com.sun.sql.framework.utils.Logger#ERROR
     * @see com.sun.sql.framework.utils.Logger#FATAL *
     */
    public ETLEngineLogEvent(String source, String message, int level) {
        this(source, message);
        logLevel = level;
    }

    /**
     * Gets the log level associated with the message in this log event.
     * 
     * @return level; one of Logger.DEBUG, Logger.INFO, Logger.WARN, Logger.ERROR, or
     *         Logger.FATAL
     * @see com.sun.sql.framework.utils.Logger#DEBUG
     * @see com.sun.sql.framework.utils.Logger#INFO
     * @see com.sun.sql.framework.utils.Logger#WARN
     * @see com.sun.sql.framework.utils.Logger#ERROR
     * @see com.sun.sql.framework.utils.Logger#FATAL
     */
    public int getLogLevel() {
        return logLevel;
    }

    /**
     * Gets the statusMessage attribute of the ETLEngineLogEvent object
     * 
     * @return The statusMessage value
     */
    public String getLogMessage() {
        if (logMessage == null) {
            return "";
        }
        return logMessage;
    }

    /**
     * Gets the sourceName attribute of the ETLEngineLogEvent object
     * 
     * @return The sourceName value
     */
    public String getSourceName() {
        return sourceName;
    }
}
