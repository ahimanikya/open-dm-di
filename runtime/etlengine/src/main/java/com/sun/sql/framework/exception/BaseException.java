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
 * @(#)BaseException.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.exception;

/**
 * Super class for all application exceptions. All SQL specific Exceptions will Extends
 * this Exception This Exception class will contain More methods and functionalities in
 * future.
 * 
 * @version :
 * @author Sudhi Seshachala
 */
public class BaseException extends Exception {

    
    private int errorCode = -1;
    
    public static final int OPERATOR_NOT_DEFINED = 1;
    /**
     * Should not be called in program, just for loading purpose
     */
    public BaseException() {
    }

    /**
     * Creates a new instance of BaseException
     * 
     * @param message Message for this exception
     */
    public BaseException(String message) {
        super(message);
    }
    
    /**
     * Creates a new instance of BaseException
     * 
     * @param message Message for this exception
     */
    public BaseException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Creates a new instance of BaseException
     * 
     * @param logCategory Form where this exception is thrown
     * @param message Message for this exception
     */
    public BaseException(String logCategory, String message) {
        super(logCategory + "-" + message);
    }

    /**
     * @param message message identifying this exception.
     * @param cause cause identifying this exception.
     */
    public BaseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor for BaseException
     * 
     * @param cause of this exception.
     */
    public BaseException(Throwable cause) {
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

    public int getErrorCode() {
        return errorCode;
    }
}
