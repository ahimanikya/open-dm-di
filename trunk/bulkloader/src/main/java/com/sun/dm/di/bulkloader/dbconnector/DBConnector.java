/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright 2003-2007 Sun Microsystems, Inc. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the Common 
 * Development and Distribution License ("CDDL")(the "License"). You 
 * may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at
 * https://open-dm-mi.dev.java.net/cddl.html
 * or open-dm-mi/bootstrap/legal/license.txt. See the License for the 
 * specific language governing permissions and limitations under the  
 * License.  
 *
 * When distributing the Covered Code, include this CDDL Header Notice 
 * in each file and include the License file at
 * open-dm-mi/bootstrap/legal/license.txt.
 * If applicable, add the following below this CDDL Header, with the 
 * fields enclosed by brackets [] replaced by your own identifying 
 * information: "Portions Copyrighted [year] [name of copyright owner]"
 */
package com.sun.dm.di.bulkloader.dbconnector;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public abstract class DBConnector implements DBConnection {

    Connection connection = null;
    String connuristr = null;
    ETLDefGenerator etldef = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(DBConnector.class.getName());
    private static Localizer sLoc = Localizer.get();

    public DBConnector() {
    }

    public Connection ConnectToDB(String dburi, String dbname) {
        String connuri = BLConstants.URI_AXION_PRIFIX + dbname + BLConstants.PS + dburi;
        connuristr = connuri;
        try {
            this.connection = DriverManager.getConnection(connuri);
            if (this.connection != null) {
                sLog.info(sLoc.x("LDR140: Connection established with File DB : {0}", connuri));
            }
        } catch (SQLException ex) {
            sLog.severe(sLoc.x("LDR141: Cannot connect to Database [ {0} ]. Reason : {1}", connuri, ex.getMessage()));
            System.exit(0);
        }
        return this.connection;
    }

    public Connection ConnectToDB(String host, int port, String id, String login, String pw, String dbtype) {
        String connuri = null;
        if (dbtype.equals("ORACLE")) {
            connuri = BLConstants.URI_ORACLE_PRIFIX + "thin" + BLConstants.PS + "@" + host + BLConstants.PS + port + BLConstants.PS + id;
        } else if (dbtype.equals("DERBY")) {
            connuri = BLConstants.URI_DERBY_PRIFIX + "//" + host + BLConstants.PS + port + "/" + id;
        }
        connuristr = connuri;

        try {
            this.connection = DriverManager.getConnection(connuri, login, pw);
            if (this.connection != null) {
                sLog.info(sLoc.x("LDR142: Connection established with [ Host: {0}, Port: {1}], Uri: {2}", host, getStringPort(port), connuri));
            }
        } catch (SQLException ex) {
            sLog.severe(sLoc.x("LDR143: Cannot connect to host [ {0} ]. Reason : {1}", host, ex.getMessage()));
            System.exit(0);
        }

        return this.connection;
    }
    
    //Somehow, port no passed here has a comma, generate a port without it ?? wierd but works
    private String getStringPort(int dbport){
        //Port no has a comma
        String portno = Integer.toString(dbport);
        int commaindex = portno.indexOf(",");
        if (commaindex != -1){
            portno = portno.substring(0, commaindex) + portno.substring(commaindex + 1, portno.length());
        }
        return portno;
    }

    public void addDBModelToETLDef(String db, String schema, String catalog, int dbtype, String targetTableQName, String login, String pw) {
        // Add this connection to ETLDefinition Generator
        if (checkIfTableExistsInDB(schema, catalog, targetTableQName)) {
            etldef.addDBModel(this.connection, db, targetTableQName, dbtype, login, pw);
        } else {
            sLog.infoNoloc("System will exit. Pls correct the problem and rerun the script");
            System.exit(0);
        }
    }

    public void addDBModelToETLDef(String db, String tableName, int dbtype) {
        // Add this connection to ETLDefinition Generator
        etldef.addDBModel(this.connection, db, tableName, dbtype, "sa", "sa");
    }

    public void addDBModelToDEF(ETLDefGenerator etldefgen, Connection conn, String schema, String catalog, int dbtype, String login, String pw, String targetTableQName) {
    }

    protected boolean checkIfTableExistsInDB(String schema, String catalog, String targetTableQName) {
        sLog.fine("Check If Target Table is Available in Target Schema ...");
        ArrayList tablenamelist = new ArrayList();
        if (this.connection != null) {

            //Check the Database Type here
            String dbproductname = null;
            try {
                dbproductname = this.connection.getMetaData().getDatabaseProductName();
            } catch (SQLException ex) {
                sLog.severe(sLoc.x("LDR144:Error while connecting to DB : {0}", ex.getMessage()));
            }

            try {
                DatabaseMetaData dbmd = this.connection.getMetaData();
                ResultSet rset = null;
                if (dbproductname.equalsIgnoreCase(BLConstants.ORACLE_PRODUCT_NAME)) {
                    String[] names = {"TABLE"};
                    rset = dbmd.getTables(catalog, schema, "%" + targetTableQName, names);
                } else if (dbproductname.equalsIgnoreCase(BLConstants.DERBY_PRODUCT_NAME)) {
                    String[] names = {"TABLE"};
                    rset = dbmd.getTables(catalog, schema, "%" + targetTableQName, names);
                } else if (dbproductname.equalsIgnoreCase(BLConstants.AXION_PRODUCT_NAME)) {
                    String[] names = {"TABLE", "DELIMITED TEXT TABLE"};
                    rset = dbmd.getTables(catalog, schema, "%" + targetTableQName, names);
                }

                while (rset.next()) {
                    tablenamelist.add(rset.getString(3));
                }

            } catch (SQLException ex) {
                sLog.severe(sLoc.x("LDR145: Error while retriving DB metadata : {0}", ex.getMessage()));
            }

            if (tablenamelist.size() == 1) {
                sLog.info(sLoc.x("LDR146: Table [ {0} ] found in the Target DB", targetTableQName));
                return true;
            } else if (tablenamelist.size() == 0) {
                sLog.severe(sLoc.x("LDR147: Table [  {0}  ] missing in Target Database. Create Target Table and proceed.", targetTableQName));
                return false;
            } else if (tablenamelist.size() > 1) {
                sLog.severe(sLoc.x("LDR148: More Than One Table with Name [ {0} ] available in Target. Count : {1}", targetTableQName, tablenamelist.size()));
                return false;
            }
        } else {
            sLog.warnNoloc("Connection to Target DB is null");
        }
        return false;
    }

    protected String getTargetTableQualifiedName(String tname) {
        if (tname.indexOf(".") != -1) {
            return tname.substring(0, tname.lastIndexOf(".")).toUpperCase();
        }
        return tname;
    }
}
