/*
 * FlatFileDBConnector.java
 *
 * Created on Nov 12, 2007, 9:57:13 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.dbconnector;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author Manish
 */
public class FlatFileDBConnector extends DBConnector {

    String sourceTableQName = null;

    public FlatFileDBConnector() {
        try {
            System.out.println("Initializing FlatFile DB Connector");
            Class.forName(BLConstants.DB_AXION_DRIVER);
        } catch (ClassNotFoundException ex) {
            System.out.println(ex.getMessage());
            System.exit(0);
        }
    }

    public FlatFileDBConnector(ETLDefGenerator etldefgen, String fileloc, String filename, String fld_delimiter, String rec_delimiter, DBConnection target_inf, int type) {
        this();
        etldef = etldefgen;
        etldef.setSourceFileDBName(filename); //Set the source file db name
        sourceTableQName = getSourceTableQualifiedName(filename);
        ConnectToDB(fileloc, sourceTableQName);
        createExternalFlatFileTable(fileloc, filename, fld_delimiter, rec_delimiter, target_inf);
        addDBModelToETLDef(sourceTableQName, type);
    }

    public Connection getDataBaseConnection() {
        return connection;
    }

    public ArrayList getTableMetaDataObjectList() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private boolean createExternalFlatFileTable(String fileloc, String filename, String fld_delimiter, String rec_delimiter, DBConnection target_inf) {
        boolean status = false;
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
            sb.append(filename.substring(0, filename.indexOf(".")) + " (");
            ArrayList mdlist = target_inf.getTableMetaDataObjectList();
            for (int i = 0; i < mdlist.size(); i++) {
                TableMetaDataObject mdobj = (TableMetaDataObject) mdlist.get(i);
                sb.append(mdobj.getColumnName() + " " + mdobj.getColumnDataType() + "(" + mdobj.getColumnLength() + ")");
                if (i + 1 < mdlist.size()) {
                    sb.append(", ");
                }
            }
            sb.append(") ORGANIZATION(LOADTYPE=\'DELIMITED\' filename=\'" + filename + "\'");
            if (rec_delimiter != null) {
                sb.append(" RECORDDELIMITER=\'" + rec_delimiter + "\'");
            }
            sb.append(" FIELDDELIMITER=\'" + fld_delimiter + "\')");

            Statement stmt = connection.createStatement();
            status = stmt.execute(sb.toString());
        } catch (SQLException ex) {
            System.out.println(ex.getMessage());
        }
        return status;
    }

    protected String getSourceTableQualifiedName(String sname) {
        if (sname.indexOf(".") != -1) {
            return sname.substring(0, sname.lastIndexOf("."));
        }
        return sname;
    }
    /*
    private void selectdata(String filename){
    try{
    Statement stmt = conn.createStatement();
    ResultSet rset = stmt.executeQuery("select * from " + filename.substring(0, filename.indexOf(".")));
    while(rset.next()) {
    System.out.print(rset.getString(1));
    System.out.print("\t");
    System.out.print(rset.getString(2));
    System.out.print("\t");
    System.out.print(rset.getString(3));
    System.out.print("\t");
    System.out.print("\n");
    }
    System.out.print("\n-----------------\n");
    }catch (SQLException ex) {
    System.out.println(ex);
    }
    }
    private void selectMetaData(String filename){
    try{
    DatabaseMetaData dbmd = conn.getMetaData();
    ResultSet rset = dbmd.getColumns(conn.getCatalog(), null, filename.substring(0, filename.indexOf(".")), null);
    while(rset.next()) {
    System.out.print(rset.getString(4));
    System.out.print("\t");
    System.out.print(rset.getString(5));
    System.out.print("\t");
    System.out.print(rset.getString(6));
    System.out.print("\t");
    System.out.print("\n");
    }
    }catch (SQLException ex) {
    System.out.println(ex);
    }
    }
     */
}
