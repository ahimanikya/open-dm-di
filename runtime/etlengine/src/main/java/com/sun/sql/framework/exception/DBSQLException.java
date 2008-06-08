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
 * @(#)DBSQLException.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.sql.framework.exception;

import java.sql.SQLException;

/**
 * The SQLBuilder jdbc Connector uses this DBSQLException to handle error conditions.
 * 
 * @author Sudhi Seshachala
 * @version :
 */
public class DBSQLException extends BaseException {

    /** * The error code associated with the DBSQLException */
    private int errorCode = -1;

    /** * The SQLStatement associated with the exception */
    private String sqlStmt = null;

    /**
     * Construct the exception from a SQLException that was thrown.
     * 
     * @param e SQLException that caused the problem
     */
    public DBSQLException(SQLException e) {
        super(e);
        errorCode = e.getErrorCode();
    }

    /**
     * Construct the exception from a SQLException that was thrown.
     * 
     * @param e SQLException that caused the problem
     * @param stmt statement source that caused the problem
     */
    public DBSQLException(SQLException e, String stmt) {
        super(e);
        errorCode = e.getErrorCode();
        sqlStmt = stmt;
    }

    /**
     * Constructor for the DBSQLException object
     * 
     * @param message String associated with this Exception
     */
    public DBSQLException(String message) {
        super(message);
    }

    /**
     * Construct the exception from a Throwable.
     * 
     * @param e Throwable that caused the problem
     * @param msg String which indicates where the exception is thrown in code segment.
     */
    public DBSQLException(String msg, Throwable e) {
        super(msg, e);
    }

    /**
     * Construct the exception from a Throwable.
     * 
     * @param e Throwable that caused the problem
     */
    public DBSQLException(Throwable e) {
        super(e);
    }

    /**
     * Gets the errorCode attribute of the DBSQLException object
     * 
     * @return The errorCode value
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Gets the sQLStmt attribute of the DBSQLException object
     * 
     * @return The sQLStmt value
     */
    public String getSQLStmt() {
        return sqlStmt;
    }
}
