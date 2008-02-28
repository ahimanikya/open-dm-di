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
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class FlatFileDBConnector extends DBConnector {

    String sourceTableQName = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(FlatFileDBConnector.class.getName());
    private static Localizer sLoc = Localizer.get();

    public FlatFileDBConnector() {
        try {
            sLog.info(sLoc.x("LDR150: Initializing FlatFile DB Connector ..."));
            Class.forName(BLConstants.DB_AXION_DRIVER);
        } catch (ClassNotFoundException ex) {
            sLog.severe(sLoc.x("LDR151 : Axion Driver Class Not Found : {0}", ex.getMessage()));
            System.exit(0);
        }
    }

    public FlatFileDBConnector(ETLDefGenerator etldefgen, String fileloc, String filename, String fld_delimiter, String rec_delimiter, String schema, String catalog, DBConnection target_inf, int type) {
        this();
        etldef = etldefgen;
        etldef.setSourceFileDBName(filename); //Set the source file db name
        sourceTableQName = getSourceTableQualifiedName(filename);
        ConnectToDB(fileloc, sourceTableQName);
        createExternalFlatFileTable(fileloc, filename, fld_delimiter, rec_delimiter, schema, catalog, target_inf);
        addDBModelToETLDef(sourceTableQName, type);
    }

    public Connection getDataBaseConnection() {
        return connection;
    }

    public ArrayList getTableMetaDataObjectList(String schema, String catalog) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private boolean createExternalFlatFileTable(String fileloc, String filename, String fld_delimiter, String rec_delimiter, String schema, String catalog, DBConnection target_inf) {
        boolean status = false;
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE EXTERNAL TABLE IF NOT EXISTS ");
            sb.append(filename.substring(0, filename.indexOf(".")) + " (");
            ArrayList mdlist = target_inf.getTableMetaDataObjectList(schema, catalog);
            for (int i = 0; i < mdlist.size(); i++) {
                TableMetaDataObject mdobj = (TableMetaDataObject) mdlist.get(i);
                String colname = mdobj.getColumnName();
                String coltype = mdobj.getColumnDataType();
                int collen = mdobj.getColumnLength();

                //Handling special cases
                //1. Column with type and name TIMESTAMP
                if (colname.equalsIgnoreCase("TIMESTAMP")) {
                    colname = "IGNORED_TIMESTAMP";
                    coltype = "VARCHAR2";
                    collen = 32;
                    sLog.warn(sLoc.x("LDR154 : Excluding Column name TIMESTAMP from the BULK LOADER. Name is a reserved Keyword."));
                }
                //2. BLOB type columns
                if (coltype.equalsIgnoreCase("BLOB")) {
                    colname = "IGNORED_" + colname;
                    coltype = "VARCHAR2";
                    collen = 32;
                    sLog.warn(sLoc.x("LDR155 : Excluding Column Type BLOB from the BULK LOADER. Unsupported Data Type for External Source Tables."));
                }

                if (coltype.equals("VARCHAR2")) {
                    sb.append(colname + " " + coltype + "(" + collen + ")");
                } else {
                    sb.append(colname + " " + coltype);
                }
                if (i + 1 < mdlist.size()) {
                    sb.append(", ");
                }
            }
            sb.append(") ORGANIZATION(LOADTYPE=\'DELIMITED\' filename=\'" + filename + "\'");
            if (rec_delimiter != null) {
                sb.append(" RECORDDELIMITER=\'" + rec_delimiter + "\'");
            }
            sb.append(" FIELDDELIMITER=\'" + fld_delimiter + "\')");

            sLog.fine("LDR152: Creating external table :" + sb.toString());

            Statement stmt = connection.createStatement();
            status = stmt.execute(sb.toString());
        } catch (SQLException ex) {
            sLog.severe(sLoc.x("LDR153 : Error Executing SQL : {0}", ex.getMessage()));
        }
        return status;
    }

    protected String getSourceTableQualifiedName(String sname) {
        if (sname.indexOf(".") != -1) {
            return sname.substring(0, sname.lastIndexOf("."));
        }
        return sname;
    }

    public String getDBConnectionURI() {
        return super.connuristr;
    }
}
