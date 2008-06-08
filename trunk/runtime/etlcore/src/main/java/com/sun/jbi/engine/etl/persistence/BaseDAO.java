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
 * @(#)BaseDAO.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.persistence;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * BaseDAO instance
 *
 * @author Sun Microsystems
 */
public interface BaseDAO {
    /**
     * gets insert statement string
     *
     * @return insert statement
     */
    String getInsertStmt();

    /**
     * gets update statement
     *
     * @return String update statement
     */
    String getUpdateStmt();

    /**
     * get delete statement
     *
     * @return String delete statement
     */
    String getDeleteStmt();

    /**
     * gets query statement
     *
     * @return String query statement
     */
    String getQueryStmt();

    /**
     * fills insert statement
     *
     * @param stmt insert statement
     *
     * @throws SQLException SQLException
     */
    void fillInsertStmt(PreparedStatement stmt) throws SQLException;

    /**
     * fills update statement
     *
     * @param stmt update statement
     *
     * @throws SQLException SQLException
     */
    void fillUpdateStmt(PreparedStatement stmt) throws SQLException;

    /**
     * fills delete statement
     *
     * @param stmt delete statement
     *
     * @throws SQLException SQLException
     */
    void fillDeleteStmt(PreparedStatement stmt) throws SQLException;

    /**
     * fills query statement
     *
     * @param stmt PreparedStatement
     *
     * @throws SQLException SQLException
     */
    void fillQueryStmt(PreparedStatement stmt) throws SQLException;

    /**
     * fills DBObject with values from Resultset
     *
     * @param rs ResultSet
     *
     * @throws SQLException SQLException
     */
    void populateDAO(ResultSet rs) throws SQLException;

    /**
     * DOCUMENT ME!
     *
     * @return DBObject DBObject
     */
    BaseDAO createNew();
}
