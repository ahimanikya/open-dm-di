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
 * @(#)DBObjectFactory.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.persistence;

import java.sql.SQLException;
import java.sql.Timestamp;

import javax.sql.rowset.serial.SerialException;
import javax.xml.namespace.QName;

import com.sun.jbi.engine.etl.persistence.impl.ETLMessagePipeLineDAOImpl;
import javax.xml.transform.Source;



/**
 * DOCUMENT ME!
 *
 * @author Sun Microsystems
 */
public class DAOFactory {
    
    private static DAOFactory mSingleton;

    // TODO evaluate the advantage gained if caching of these DB object instances is done.
    private int mDBType;

    private DAOFactory(int dbType) {
        mDBType = dbType;
    }

    /**
     * get DBObject factory
     *
     * @param dbType database type
     *
     * @return DBObjectFactory DBObject factory
     */
    public static DAOFactory getDAOFactory(int dbType) {
        if (mSingleton == null) {
            mSingleton = new DAOFactory(dbType);
        }

        return mSingleton;
    }

    
     public BaseDAO createETLMessagePipelineDAO(String msgExId, String serviceName, 
             String operationName, String content, int status ) {
        return new ETLMessagePipeLineDAOImpl(msgExId, serviceName, operationName, content, status);
    }
    
    /**
     * creates engine DAOObject
     *
     * @param id ID
     * @return BaseDAO
     */
  
    public BaseDAO createETLMessagePipelineDAO() {
        return new ETLMessagePipeLineDAOImpl(mDBType);
    }
}
