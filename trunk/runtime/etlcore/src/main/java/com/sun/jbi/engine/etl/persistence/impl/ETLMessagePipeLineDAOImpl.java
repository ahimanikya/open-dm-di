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
 * @(#)InstanceCorrDBOImpl.java 
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
import com.sun.jbi.engine.etl.persistence.ETLMessagePipeLineDAO;
import com.sun.jbi.engine.etl.persistence.EngineState;
import javax.xml.transform.Source;
import javax.jbi.messaging.InOut;



/**
 *
 * @author Sun Microsystems
 */
public class ETLMessagePipeLineDAOImpl extends BaseDAOImpl
    implements ETLMessagePipeLineDAO {
    
    private String mesgExId;
    private String serviceName;
    private String operationName;
    private String normalizedMessageContent;
    private int status = -1;
            

    /**
     * constructor
     *
     * @param dbType database type
     * @param bpId BPID
     */
    public ETLMessagePipeLineDAOImpl(String msgExId, String serviceName, String operationName, String content, int status) {
        mesgExId = msgExId;
        this.serviceName = serviceName;
        this.operationName = operationName;
        this.normalizedMessageContent = content;
        this.status = status;
        init(BASE_INSERT_STMT_STR, BASE_UPDATE_STMT_STR, BASE_DELETE_STMT_STR,
            BASE_QUERY_STMT_STR);
    }
    
   

    /**
     * constructor
     *
     * @param dbType database type
     */
    public ETLMessagePipeLineDAOImpl(int dbType) {
        init(BASE_INSERT_STMT_STR, BASE_UPDATE_STMT_STR, BASE_DELETE_STMT_STR,
            BASE_QUERY_STMT_STR);
    }

    private ETLMessagePipeLineDAOImpl() {
    }

    
     /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.InstanceCorrelationDBO#getValue()
     */
    public String getMsgExId() {
        return this.mesgExId;
    }
    
    public String getServiceName() {
        return this.serviceName;
    }
    
    public String getOperationName() {
        return this.operationName;
    }

    public String getNormalizedMessage() {
        return this.normalizedMessageContent;
    }
    
    public int getStatus() {
        return this.status;
    }
    
    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#fillDeleteStmt(java.sql.PreparedStatement)
     */
    public void fillDeleteStmt(PreparedStatement stmt)
        throws SQLException {
        stmt.setString(1, this.mesgExId);
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#fillInsertStmt(java.sql.PreparedStatement)
     */
    public void fillInsertStmt(PreparedStatement stmt)
        throws SQLException {
        stmt.setString(1, this.mesgExId);
        stmt.setString(2, this.serviceName);
        stmt.setString(3, this.operationName);

        stmt.setString(4, this.normalizedMessageContent);
        stmt.setInt(5, this.status);
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#fillUpdateStmt(java.sql.PreparedStatement)
     */
    public void fillUpdateStmt(PreparedStatement stmt)
        throws SQLException {
        stmt.setInt(1, status);
        stmt.setString(2, this.mesgExId);
        // TODO Auto-generated method stub
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#fillQueryStmt(java.sql.PreparedStatement)
     */
    public void fillQueryStmt(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, getMsgExId());
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#populateDBO(java.sql.ResultSet)
     */
    public void populateDAO(ResultSet rs) throws SQLException {
        this.mesgExId = rs.getString(1);
        this.serviceName = rs.getString(2);
        this.operationName = rs.getString(3);
        this.normalizedMessageContent = rs.getString(4);
        this.status = rs.getInt(5);
    }

    /**
     * @see com.sun.jbi.engine.bpel.core.bpel.dbo.DBObject#getNewObject()()()
     */
    public BaseDAO getNewObject() {
        return new ETLMessagePipeLineDAOImpl();
    }
}
