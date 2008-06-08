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
 * @(#)ETLSEDBConnectionProvider.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.sun.sql.framework.exception.BaseException;
import com.sun.sql.framework.jdbc.DBConnectionFactory;
import com.sun.sql.framework.jdbc.DBConnectionParameters;
import com.sun.etl.engine.spi.DBConnectionProvider;
import com.sun.jbi.engine.etl.Localizer;
import java.sql.DatabaseMetaData;


/**
 * @author Sujit Biswas
 *
 */
public class ETLSEDBConnectionProvider implements DBConnectionProvider {

     private static transient final Logger mLogger = Logger.getLogger(ETLSEDBConnectionProvider.class.getName());
       
     /* */
     public static final String DERBY_DATABASE = "Apache Derby";

  
     public static final String ORACLE_DATABASE = "Oracle";
    

     private static transient final Localizer mLoc = Localizer.get();

    public void closeConnection(Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                mLogger.log(Level.SEVERE, mLoc.loc("INFO085: {0}", e.getMessage()), e);
            }
        }

    }

    public Connection getConnection(DBConnectionParameters conDef) throws BaseException {
        String driver = conDef.getDriverClass();
        try {
            Class.forName(driver).newInstance();
        } catch (Exception e) {
            mLogger.log(Level.SEVERE, mLoc.loc("ERRO086: Exception {0}", e.getMessage()), e);
        }

        String username = conDef.getUserName();
        String password = conDef.getPassword();
        String url = conDef.getConnectionURL();
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            mLogger.log(Level.SEVERE, mLoc.loc("ERRO086: Exception {0}", e.getMessage()), e);
            throw new BaseException(e);
        }

    }

    public Connection getConnection(Properties connProps) throws BaseException {
        String driver = connProps.getProperty(DBConnectionFactory.PROP_DRIVERCLASS);
        try {
            Class.forName(driver).newInstance();
        } catch (Exception e) {
            mLogger.log(Level.SEVERE, mLoc.loc("ERRO086: Exception {0}", e.getMessage()), e);
        }
        String username = connProps.getProperty(DBConnectionFactory.PROP_USERNAME);
        String password = connProps.getProperty(DBConnectionFactory.PROP_PASSWORD);
        String url = connProps.getProperty(DBConnectionFactory.PROP_URL);
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (SQLException e) {
            mLogger.log(Level.SEVERE, mLoc.loc("ERRO086: Exception {0}", e.getMessage()), e);
            throw new BaseException(e);
        }

    }

    public static String getDBType(Connection conn) throws SQLException {
        String dbType = DERBY_DATABASE;
        DatabaseMetaData meta = conn.getMetaData();
        String databaseProductName = meta.getDatabaseProductName();
        databaseProductName = (databaseProductName.indexOf("Oracle") == -1) ? databaseProductName : ORACLE_DATABASE;
        
        dbType = databaseProductName.equalsIgnoreCase(ORACLE_DATABASE) ? ORACLE_DATABASE:DERBY_DATABASE;

        // DO we need to check for the suppored version also ?
        if (!databaseProductName.equalsIgnoreCase(DERBY_DATABASE) && !databaseProductName.equalsIgnoreCase(ORACLE_DATABASE)) {
            String msg = mLoc.loc("ERR-0087: Database Not Supported: {0} Supported Databases : {1} and {2}",
                    databaseProductName, DERBY_DATABASE, ORACLE_DATABASE);
            throw new RuntimeException(msg);
        }
        return dbType;
    }

}
