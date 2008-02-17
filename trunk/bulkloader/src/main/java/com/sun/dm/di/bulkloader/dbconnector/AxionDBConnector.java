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
public class AxionDBConnector extends DBConnector {

    String targetTableQName = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(AxionDBConnector.class.getName());
    private static Localizer sLoc = Localizer.get();

    public AxionDBConnector() {
        try {
            sLog.info(sLoc.x("LDR101: Initializing Axion DB Connector ..."));
            Class.forName(BLConstants.DB_AXION_DRIVER);
        } catch (ClassNotFoundException ex) {
            sLog.severe(sLoc.x("LDR102: Unable to Locate Axion Driver : {0}", ex.getMessage()));
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
            ex.printStackTrace();
        }
        return mdlist;
    }

    public String getDBConnectionURI() {
        return super.connuristr;
    }
}
