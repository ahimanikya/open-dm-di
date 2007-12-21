/*
 * AxionDBConnector.java
 *
 * Created on Nov 12, 2007, 12:40:32 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.dbconnector;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Manish
 */
public class AxionDBConnector extends DBConnector {

    String targetTableQName = null;

    public AxionDBConnector() {
        try {
            System.out.println("Initializing Axion DB Connector ...");
            Class.forName(BLConstants.DB_AXION_DRIVER);
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage());
            System.exit(0);
        }
    }

    public AxionDBConnector(ETLDefGenerator etldefgen, String dbDir, String dbName, String tablename, int type) {
        this();
        etldef = etldefgen;
        targetTableQName = getTargetTableQualifiedName(tablename);
        ConnectToDB(dbDir, dbName);
        addDBModelToETLDef(null, null, type, targetTableQName, "sa", "sa");
    }

    public Connection getDataBaseConnection() {
        return connection;
    }

    public ArrayList getTableMetaDataObjectList() {
        ArrayList<TableMetaDataObject> mdlist = new ArrayList();
        try {
            DatabaseMetaData dbmd = connection.getMetaData();
            //String targetTableName = (String) getAllUserTablesList(null, null).get(0);
            ResultSet rset = dbmd.getColumns(null, null, targetTableQName, null);

            while (rset.next()) {
                TableMetaDataObject tableMD = new TableMetaDataObject();
                tableMD.setColumnName(rset.getString(4));
                tableMD.setColumnLength(Integer.parseInt(rset.getString(5)));
                tableMD.setColumnDataType(rset.getString(6));
                mdlist.add(tableMD);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return mdlist;
    }
}
