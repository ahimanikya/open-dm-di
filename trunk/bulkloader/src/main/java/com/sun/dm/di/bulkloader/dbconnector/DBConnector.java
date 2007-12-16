/*
 * DBConnector.java
 *
 * Created on Nov 12, 2007, 12:41:13 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.dbconnector;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 *
 * @author Manish
 */
public abstract class DBConnector implements DBConnection {

    Connection connection = null;
    ETLDefGenerator etldef = null;

    public DBConnector() {
    }

    public Connection ConnectToDB(String dburi, String dbname) {
        String connuri = BLConstants.URI_AXION_PRIFIX + dbname + BLConstants.PS + dburi;
        try {
            this.connection = DriverManager.getConnection(connuri);
            if (this.connection != null) {
                System.out.println("Connection established with File DB : " + connuri);
            }
        } catch (SQLException ex) {
            System.out.println("Cannot connect to Database [" + connuri + "]. Reason : " + ex.getMessage());
            System.exit(0);
        }
        return this.connection;
    }

    public Connection ConnectToDB(String host, int port, String sid, String login, String pw) {
        String connuri = BLConstants.URI_ORACLE_PRIFIX + "thin" + BLConstants.PS + "@" + host + BLConstants.PS + port + BLConstants.PS + sid;
        try {
            this.connection = DriverManager.getConnection(connuri, login, pw);
            if (this.connection != null) {
                System.out.println("Connection established with [ " + host + ":" + port + " ]");
            }
        } catch (SQLException ex) {
            System.out.println("Cannot connect to host [" + host + "] : " + ex.getMessage());
            System.exit(0);
        }

        return this.connection;
    }

    public void addDBModelToETLDef(String schema, String catalog, int dbtype, String targetTableQName, String login, String pw) {
        // Add this connection to ETLDefinition Generator
        if (checkIfTableExistsInDB(schema, catalog, targetTableQName)) {
            etldef.addDBModel(this.connection, targetTableQName, dbtype, login, pw);
        }
    }

    public void addDBModelToETLDef(String tableName, int dbtype) {
        // Add this connection to ETLDefinition Generator
        etldef.addDBModel(this.connection, tableName, dbtype, "sa", "sa");
    }

    public void addDBModelToDEF(ETLDefGenerator etldefgen, Connection conn, String schema, String catalog, int dbtype, String login, String pw, String targetTableQName) {
    }

    protected boolean checkIfTableExistsInDB(String schema, String catalog, String targetTableQName) {
        //System.out.println("Check If Target Table is Available in Target Schema");
        ArrayList tablenamelist = new ArrayList();
        if (this.connection != null) {

            //Check the Database Type here
            String dbproductname = null;
            try {
                dbproductname = this.connection.getMetaData().getDatabaseProductName();
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }

            try {
                DatabaseMetaData dbmd = this.connection.getMetaData();
                ResultSet rset = null;
                if (dbproductname.equalsIgnoreCase(BLConstants.ORACLE_PRODUCT_NAME)) {
                    String[] names = {"TABLE"};
                    rset = dbmd.getTables(schema, catalog, "%" + targetTableQName, names);
                } else if (dbproductname.equalsIgnoreCase(BLConstants.AXION_PRODUCT_NAME)) {
                    String[] names = {"TABLE", "DELIMITED TEXT TABLE"};
                    rset = dbmd.getTables(schema, catalog, "%" + targetTableQName, names);
                }

                while (rset.next()) {
                    tablenamelist.add(rset.getString(3));
                }

            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }

            if (tablenamelist.size() == 1) {
                System.out.println("Table [ " + targetTableQName + " ] found in the Target DB");
                return true;
            } else if (tablenamelist.size() == 0) {
                System.out.println("Table [ " + targetTableQName + " ] missing in Target Database. Create Target Table and proceed.");
                return false;
            } else if (tablenamelist.size() > 1) {
                System.out.println("More Than One Table with Name [ " + targetTableQName + " ] available in Target. Count " + tablenamelist.size());
                return false;
            }
        } else {
            System.out.println("Connection to Target DB is null");
        }
        return false;
    }

    protected String getTargetTableQualifiedName(String tname) {
        if (tname.indexOf(".") != -1) {
            return tname.substring(0, tname.lastIndexOf("."));
        }
        return tname;
    }
}
