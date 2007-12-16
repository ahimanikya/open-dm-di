/*
 * CommonCalls.java
 * 
 * Created on Nov 12, 2007, 9:58:32 AM
 * 
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package com.sun.dm.di.bulkloader.dbconnector;

import com.sun.dm.di.bulkloader.modelgen.ETLDefGenerator;
import java.sql.Connection;
import java.util.ArrayList;

/**
 *
 * @author Manish
 */
public interface DBConnection {
    
    public abstract Connection getDataBaseConnection();
    public abstract ArrayList getTableMetaDataObjectList();
    //public abstract void addDBModelToETLDef(String schema, String catalog, int dbtype, String targetTableQName);
    public abstract void addDBModelToDEF(ETLDefGenerator etldefgen, Connection conn, String schema, String catalog, int dbtype, String login, String pw, String targetTableQName);

}
