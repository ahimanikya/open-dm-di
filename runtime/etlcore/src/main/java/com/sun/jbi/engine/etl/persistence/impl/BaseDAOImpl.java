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
 * @(#)DBObjectImpl.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.persistence.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.sun.jbi.engine.etl.persistence.BaseDAO;


/**
 * DOCUMENT ME!
 *
 * @author Sun Microsystems
 */
public abstract class BaseDAOImpl implements BaseDAO {
    /**
     * The following 3 variables even though defined as instane variables are  essentially static
     * in nature. But for readability of the code and to be in line with java supported
     * inheritance, these are made instance variables. if these variables  need to be static then
     * the corresponding get methods will be forced to be in the  inherited classes. And hence the
     * justification of these being instance variables.
     */
    /** insert statement string */
    protected String INSERT_STMT_STR;

    /** update statement string */
    protected String UPDATE_STMT_STR;

    /** delete statement string */
    protected String DELETE_STMT_STR;

    /** query statement string */
    protected String QUERY_STMT_STR;

    /**
     * initialization
     *
     * @param insert insert statement string
     * @param update update statement string
     * @param delete delete statement string
     * @param query query statement string
     */
    protected void init(String insert, String update, String delete,
        String query) {
        INSERT_STMT_STR = insert;
        UPDATE_STMT_STR = update;
        DELETE_STMT_STR = delete;
        QUERY_STMT_STR = query;
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#getInsertStmt()
     */
    public String getInsertStmt() {
        return INSERT_STMT_STR;
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#getUpdateStmt()
     */
    public String getUpdateStmt() {
        return UPDATE_STMT_STR;
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#getDeleteStmt()
     */
    public String getDeleteStmt() {
        return DELETE_STMT_STR;
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#getQueryStmt()
     */
    public String getQueryStmt() {
        return QUERY_STMT_STR;
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#fillInsertStmt(java.sql.PreparedStatement)
     */
    public abstract void fillInsertStmt(PreparedStatement stmt)
        throws SQLException;

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#fillDeleteStmt(java.sql.PreparedStatement)
     */
    public void fillDeleteStmt(PreparedStatement stmt)
        throws SQLException {
        //      when no delete statement is needed, the sub classes need not implement it.
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#fillUpdateStmt(java.sql.PreparedStatement)
     */
    public void fillUpdateStmt(PreparedStatement stmt)
        throws SQLException {
        //      when no update  statement is needed, the sub classes need not implement it.        
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#fillQueryStmt(java.sql.PreparedStatement)
     */
    public abstract void fillQueryStmt(PreparedStatement stmt)
        throws SQLException;

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#populateDBO(java.sql.ResultSet)
     */
    public abstract void populateDAO(ResultSet rs) throws SQLException;

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#createNew()
     */
    public BaseDAO createNew() {
        BaseDAOImpl obj = (BaseDAOImpl) getNewObject();
        obj.DELETE_STMT_STR = DELETE_STMT_STR;
        obj.INSERT_STMT_STR = INSERT_STMT_STR;
        obj.UPDATE_STMT_STR = UPDATE_STMT_STR;
        obj.QUERY_STMT_STR = QUERY_STMT_STR;

        return obj;
    }

    /**
     * get new instance of DBObject
     *
     * @return DBObject DBObject
     */
    protected abstract BaseDAO getNewObject();
}
