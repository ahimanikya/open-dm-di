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
 * @(#)ETLException.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.etl.engine.utils;

/**
 * Super class for all application exceptions. All SQL specific Exceptions will Extends
 * this Exception This Exception class will contain More methods and functionalities in
 * future.
 * 
 * @version :
 * @author Sudhi Seshachala
 */
public class ETLException extends Exception {

    /**
     * Should not be called in program, just for loading purpose
     */
    public ETLException() {
    }

    /**
     * Creates a new instance of BaseException
     * 
     * @param message Message for this exception
     */
    public ETLException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of BaseException
     * 
     * @param logCategory Form where this exception is thrown
     * @param message Message for this exception
     */
    public ETLException(String logCategory, String message) {
        super(logCategory + "-" + message);
    }

    /**
     * @param message message identifying this exception.
     * @param cause cause identifying this exception.
     */
    public ETLException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor for BaseException
     * 
     * @param cause of this exception.
     */
    public ETLException(Throwable cause) {
        super(cause);
    }

    public String getMessage() {
        StringBuffer buf = new StringBuffer();
        String msg = super.getMessage();
        buf.append(msg);

        Throwable t = this;
        //we are getting only the first exception which is wrapped,
        //should we get messages from all the exceptions in the chain?
        while (t.getCause() != null) {
            t = t.getCause();
            // Prevent infinite loop if cause of t references itself.
            if (t.getCause() == t) {
                break;
            }
        }

        if (t != this) {
            buf.append(" - Root Cause: ").append(t.getMessage());
        }

        return buf.toString();
    }
}
