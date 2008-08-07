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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import net.java.hulp.i18n.Logger;

/**
 *
 * @author Manish
 */
public class SQLServerDBConnector extends DBConnector {
    
    String targetTableQName = null;
    //logger
    private static Logger sLog = LogUtil.getLogger(OracleDBConnector.class.getName());
    private static Localizer sLoc = Localizer.get();    

    public SQLServerDBConnector() {
        try {
            sLog.fine("LDR170: Initializing SQL Server DB Connector ...");
            Class.forName(BLConstants.DB_SQLSERVER_DRIVER);
        } catch (ClassNotFoundException ex) {
            sLog.severe(sLoc.x("LDR171 : SQL Server Driver Class Not Found : {0}", ex.getMessage()));
            System.exit(0);
        }
    }

    public SQLServerDBConnector(ETLDefGenerator etldefgen, String host, int port, String sid, String schema, String catalog, String login, String pw, String tablename, int type) {
        this();
        etldef = etldefgen;
        targetTableQName = getTargetTableQualifiedName(tablename);
        sLog.fine("SQL Server Target Table Qualified Name is : " + targetTableQName);
        ConnectToDB(host, port, sid, login, pw, "SQLSERVER");
        addDBModelToETLDef("SQLSERVER", schema, catalog, type, targetTableQName, login, pw);
    }
    
    @Override
    public void addDBModelToDEF(ETLDefGenerator etldefgen, Connection conn, String schema, String catalog, int dbtype, String login, String pw, String tablename) {
        etldef = etldefgen;
        targetTableQName = getTargetTableQualifiedName(tablename);
        // Add this connection to ETLDefinition Generator
        if (checkIfTableExistsInDB(schema, catalog, targetTableQName)) {
            etldef.addDBModel(conn, "SQLSERVER", schema, catalog, targetTableQName, dbtype, login, pw);
        }
    }
    
    public Connection getDataBaseConnection() {
        return connection;
    }

    public ArrayList getTableMetaDataObjectList(String schema, String catalog) {
        ArrayList<TableMetaDataObject> mdlist = new ArrayList();
        
            //SomeTimes Catalog is null but does not show that.
            if (catalog != null){
                if (catalog.length() == 0){
                    catalog = null;
                }
            }
            
            //Sometimes Schema is null but does not show that
            if (schema != null){
                if (schema.length() == 0){
                    schema = null;
                }
            }        
        
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
            sLog.severe(sLoc.x("LDR172 : Error Retrieving SQL Server DB Metadata : {0}", ex.getMessage()));
        }
        return mdlist;
    }

    public String getDBConnectionURI() {
        return super.connuristr;
    }    

}
