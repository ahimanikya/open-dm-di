/*
 * To change this template, choose Tools | Templates
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

/**
 *
 * @author Manish
 */
public class OracleDBConnector extends DBConnector {

    String targetTableQName = null;

    public OracleDBConnector() {
        try {
            System.out.println("Initializing Oracle DB Connector ...");
            Class.forName(BLConstants.DB_ORACLE_DRIVER);
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage());
            System.exit(0);
        }
    }

    public OracleDBConnector(ETLDefGenerator etldefgen, String host, int port, String sid, String schema, String catalog, String login, String pw, String tablename, int type) {
        this();
        etldef = etldefgen;
        targetTableQName = getTargetTableQualifiedName(tablename);
        //System.out.println("Target Table Qualified Name is : " + targetTableQName);
        ConnectToDB(host, port, sid, login, pw);
        addDBModelToETLDef(schema, catalog, type, targetTableQName, login, pw);
    }
    
    @Override
    public void addDBModelToDEF(ETLDefGenerator etldefgen, Connection conn, String schema, String catalog, int dbtype, String login, String pw, String tablename) {
        etldef = etldefgen;
        targetTableQName = getTargetTableQualifiedName(tablename);
        
        // Add this connection to ETLDefinition Generator
        if (checkIfTableExistsInDB(schema, catalog, targetTableQName)) {
            etldef.addDBModel(conn, targetTableQName, dbtype, login, pw);
        }
    }
    
    

    public Connection getDataBaseConnection() {
        return connection;
    }

    public ArrayList getTableMetaDataObjectList() {
        ArrayList<TableMetaDataObject> mdlist = new ArrayList();
        try {
            DatabaseMetaData dbmd = connection.getMetaData();
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
