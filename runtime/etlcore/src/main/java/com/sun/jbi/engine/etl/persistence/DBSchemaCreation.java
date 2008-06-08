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
 * @(#)DBSchemaCreation.java 
 *
 * Copyright 2004-2007 Sun Microsystems, Inc. All Rights Reserved.
 * 
 * END_HEADER - DO NOT EDIT
 */

package com.sun.jbi.engine.etl.persistence;

import com.sun.etl.engine.Localizer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.jbi.engine.etl.ETLSEDBConnectionProvider;
import com.sun.sql.framework.exception.BaseException;

/**
 * @author Sun Microsystems
 */
public abstract class  DBSchemaCreation {

    private static final Logger LOGGER = Logger.getLogger(DBSchemaCreation.class.getName());
    
 
    private static String DELIMITER = ";"; //$NON-NLS-1$   
    protected static String CONFIG_ROOT = null;

    
    private static transient final Localizer mLoc = Localizer.get();
    /**
     * Check to see if all the tables required by ETL-SE for persistence are present. 
     * If not present, create the tables
     * @param prop database properties
     */
    public void checkAndCreateTables(Connection conn) throws SQLException {
        //Check to see if all the tables required by ETL-SE for persistence are present
        //If not present, create the tables
            String dbType = ETLSEDBConnectionProvider.getDBType(conn);
            if (!checkTablesIntegrity(conn)) {
                createTables(conn, dbType);
            }
    }

    /**
     * Creates the tables required for persistence
     * @param prop database properties
     */
    public void createTables(Connection conn, String dbType) {
        StringTokenizer tokens = populateCreateScript(dbType, getCreateScriptName());
        executeScript(conn, tokens);
    }
    
     public static void setInstallRoot(String installRoot) {
        CONFIG_ROOT = installRoot;
    }

    
    /**
     * Drops the tables created for persistence
     * @param prop database properties
     */
    public void dropTables(Connection conn, String dbType) {
        StringTokenizer tokens = populateCreateScript(dbType, getDropScriptName());
        executeScript(conn, tokens);
    }
    
    public void truncateTables (Connection conn, String dbType) {
        StringTokenizer tokens = populateCreateScript(dbType, getTruncateScriptName());
        executeScript(conn, tokens);
    }

    /**
     * Check to see if all the tables required by BPEL-SE for persistence are present
     *
     * @param prop database properties
     * @return boolean true: if valid schema exists in database false: otherwise
     */
    public boolean checkTablesIntegrity(Connection dbConn) {

        boolean allTablesPresent = false;
        String dbUserName = null;
        ResultSet resultSet = null;


        // DEVNOTE: Do not try again if there is a SQLException, since this is a startup method. If the method
        // fails due to DBUnavailability (or anything else for that matter) the BPEL SE will not start.
        try {
            dbUserName = dbConn.getMetaData().getUserName();

            //Check for all tables present
            int numTablesFound = 0;

            String[] objectsToSearchFor = {"TABLE"};
            resultSet = dbConn.getMetaData().getTables(null, "%", "%", objectsToSearchFor); //$NON-NLS-1$

            while (resultSet.next() && (numTablesFound != getTabels().length)) {

                String rsTableName = resultSet.getString("TABLE_NAME");

                //if the table is not in a schema same as the user name, go to next
                String rsTableSchemaName = resultSet.getString("TABLE_SCHEM");
                if (!(dbUserName.equalsIgnoreCase(rsTableSchemaName))) {
                    continue;
                }

                for (int i = 0; i < getTabels().length; i++) {
                    String tableItem = getTabels()[i];
                    if (rsTableName.equalsIgnoreCase(tableItem)) { //$NON-NLS-1$
                        //Found a table, increment the counter

                        numTablesFound++;
                        break;
                    }
                }
            }


            if (numTablesFound == getTabels().length) { // found all tables

                allTablesPresent = true;
                return allTablesPresent;
            }
            return false;

        } catch (Exception e) {
            throw new RuntimeException(mLoc.loc("INFO201: Exception occured while verifying that all the " +
                    "tables required for persistence are present in the schema {0}", dbUserName), e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException resCloseExcp) {
                    LOGGER.log(Level.WARNING,
                            mLoc.loc("INFO202: Exception occured while closing a JDBC Resultset"), resCloseExcp);
                }
            }
            //if (dbConn != null) {
            //    try {
            //        dbConn.close(); // this wrapper takes care of setting the initial value of setAutoCommit

            //    } catch (SQLException ex) {
            //        LOGGER.log(Level.WARNING, mLoc.t("BPCOR-6062: Exception while closing a JDBC connection"), ex);
            //    }
            //}
        }
    }
    
    private StringTokenizer populateCreateScript(String dbType, String scriptName) {
        String scriptsPath = CONFIG_ROOT + "/" + "scripts" + "/"; //$NON-NLS-1$
        scriptsPath = scriptsPath +  scriptName;
               
        // we want : scriptsPath=scriptsPath+"derby/create_bpel_schema.sql";
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        InputStream is = null;
        
        try {
            LOGGER.log(Level.INFO, "scriptsPath = " + scriptsPath);
            is = new java.io.FileInputStream(scriptsPath);
            
            int c;
            
            while ((c = is.read()) != -1) {
                os.write(c);
            }
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
            LOGGER.log(Level.SEVERE, mLoc.loc("INFO203: failed_to_load_the_schema_scripts"), e);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
                
                if (os != null) {
                    os.close();
                }
            } catch (IOException ex) {
                LOGGER.log(Level.WARNING,
                		mLoc.loc("INFO204: Error closing the streams"), ex); //$NON-NLS-1$
                throw new RuntimeException(ex);
            }
        }
        
        String script = new String(os.toByteArray());
        StringTokenizer tokens = new StringTokenizer(script, DELIMITER);
        
        return tokens;
    }

    private static void executeScript(
    		Connection conn, StringTokenizer tokens) {
       
        Statement stmt = null;
        
        // DEVNOTE: Do not try again if there is a SQLException, since this is a startup method. If the method
        // fails due to DBUnavailability (or anything else for that matter) the BPEL SE will not start.
        try {
            conn.setAutoCommit(false);
            stmt = conn.createStatement();

            String element = null;
            for (int i = 0, size = tokens.countTokens(); i < size; i++) {
                // last token may just be empty.
                element = tokens.nextToken();
                if( !"".equalsIgnoreCase(element.trim())) {
                    stmt.execute(element);
                }
            }

            conn.commit();

        } catch (Exception ex) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    LOGGER.log(Level.WARNING, 
                    		mLoc.loc("INFO205: Exception occured while rollback called on a JDBC connection"), e);
                }
            }
            
            throw new RuntimeException(mLoc.loc("INFO206: Exception occured while trying to create database " + 
            		"tables required by for persistence"), ex);
        } finally {
        	if (stmt != null) {
        		try {
        			stmt.close();
        		} catch (SQLException e) {
        			LOGGER.log(Level.WARNING, mLoc.loc("INFO207: Exception while closing a JDBC statement"), e);                        
        		}
        	}
         }
    }
    
    protected abstract String [] getTabels ();
    
    protected abstract String getCreateScriptName ();
    
    protected abstract String getDropScriptName ();
    
    protected abstract String getTruncateScriptName ();
}
