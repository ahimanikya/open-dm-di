/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.sun.dm.di.bulkloader.dbconnector;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import com.sun.dm.di.bulkloader.util.BLConstants;
import com.sun.dm.di.bulkloader.util.Localizer;
import com.sun.dm.di.bulkloader.util.LogUtil;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class OracleDBConnector extends DBConnector {

    String targetTableQName = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(OracleDBConnector.class.getName());
    private static Localizer sLoc = Localizer.get();    

    public OracleDBConnector() {
        try {
            sLog.info(sLoc.x("LDR160: Initializing Oracle DB Connector ..."));
            Class.forName(BLConstants.DB_ORACLE_DRIVER);
        } catch (ClassNotFoundException ex) {
            sLog.severe(sLoc.x("LDR161 : Oracle Driver Class Not Found : {0}", ex.getMessage()));
            System.exit(0);
        }
    }

    public OracleDBConnector(ETLDefGenerator etldefgen, String host, int port, String sid, String schema, String catalog, String login, String pw, String tablename, int type) {
        this();
        etldef = etldefgen;
        targetTableQName = getTargetTableQualifiedName(tablename);
        sLog.fine("Oracle Target Table Qualified Name is : " + targetTableQName);
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

    public ArrayList getTableMetaDataObjectList(String schema, String catalog) {
        ArrayList<TableMetaDataObject> mdlist = new ArrayList();
        try {
            DatabaseMetaData dbmd = connection.getMetaData();
            ResultSet rset = dbmd.getColumns(catalog, schema, targetTableQName, "%");

            while (rset.next()) {
                TableMetaDataObject tableMD = new TableMetaDataObject();
                tableMD.setColumnName(rset.getString(4)); //Set Col Name
                tableMD.setColumnDataType(rset.getString(6)); //Set Col DataType
                tableMD.setColumnLength(Integer.parseInt(rset.getString(7))); //Set Col Length
                mdlist.add(tableMD);
            }
        } catch (SQLException ex) {
            sLog.severe(sLoc.x("LDR162 : Error Retrieving Oracle DB Metadata : {0}", ex.getMessage()));
        }
        return mdlist;
    }
    
}
