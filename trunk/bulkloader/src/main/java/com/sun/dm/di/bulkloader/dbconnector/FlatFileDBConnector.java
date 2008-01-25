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

            sLog.info(sLoc.x("LDR152: Creating external table : {0}", sb.toString()));
            
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
}
