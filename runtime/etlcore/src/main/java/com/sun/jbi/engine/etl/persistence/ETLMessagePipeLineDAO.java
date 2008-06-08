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
 * @(#)OutstandingMsgExDBO.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.persistence;

import com.sun.jbi.engine.etl.persistence.PersistenceDBSchemaCreation;


/**
 * DOCUMENT ME!
 *
 * @author Sun Microsystems
 */
public interface ETLMessagePipeLineDAO extends BaseDAO {
    /* oracle statements */

    /** insert statement */
    String BASE_INSERT_STMT_STR = "INSERT INTO " + //$NON-NLS-1$
    PersistenceDBSchemaCreation.MESSAGEPIPELINE + " (EXCHANGEID, SERVICENAME, OPERATIONNAME, CONTENT, STATUS) VALUES (?, ?, ?, ?, ?)"; //$NON-NLS-1$

    /** update statement */
    String BASE_UPDATE_STMT_STR = "UPDATE " + PersistenceDBSchemaCreation.MESSAGEPIPELINE + " SET STATUS = ? WHERE exchangeid=?" ; //$NON-NLS-1$

    /** delete statement */
    String BASE_DELETE_STMT_STR = "DELETE FROM " + //$NON-NLS-1$
    PersistenceDBSchemaCreation.MESSAGEPIPELINE + " WHERE EXCHANGEID = ?"; //$NON-NLS-1$

    /** query statement */
    String BASE_QUERY_STMT_STR = "SELECT EXCHANGEID, SERVICENAME, OPERATIONNAME, CONTENT, STATUS FROM " + //$NON-NLS-1$
    PersistenceDBSchemaCreation.MESSAGEPIPELINE + " WHERE STATUS in ( 0, 2 )"; //$NON-NLS-1$

    /* sybase, db2, sqlserver, pointbase statements to be added */

    
  

    /**
     * message exchange id
     *
     * @return String 
     */
    String getMsgExId();
    
    String getServiceName();
    
    String getOperationName();
    
    String getNormalizedMessage();
    
    int getStatus();
    
}
